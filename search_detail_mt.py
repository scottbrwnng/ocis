import requests
import json
import duckdb as ddb
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import random
import sys

import logging
hs = logging.StreamHandler()
hf = logging.FileHandler('logs.log')
logging.basicConfig(
    level=logging.INFO, handlers=[hs, hf], 
    format='%(asctime)s [%(levelname)s] %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger(__name__)

warnings.filterwarnings('ignore')

PARTITION = int(sys.argv[1])

counter_lock = Lock()


class Searcher:

    def __init__(self) -> None:
        self.session = self.create_session()
        self.proxy_list = load_proxies()
        self.proxy = random.choice(self.proxy_list)
        self.pay = None


    def create_session(self):

        def get_cookie() -> str:
            while True:
                try:
                    url = "https://eapps.courts.state.va.us/ocis-rest/api/public/termsAndCondAccepted"
                    res = requests.get(url, verify=False)
                    cookie = res.cookies['OES_TC_JSESSIONID']
                    break
                except Exception as e:
                    log.error(f'{self.pay} {type(e).__name__}: {e}')
            return cookie

        cookie = get_cookie()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
            'Cookie': f'OES_TC_JSESSIONID={cookie}'
        }
        s = requests.Session()
        s.headers=headers
        return s    


    def extract(self, pay:dict) -> dict|None: # NOTE: the combination of fips4 and casenumber, and division type should return 1 result
        self.pay = pay
        retries = 3
        while retries > 0:                    # - case details required fields in payload are
            try:                       # qualifiedFips, courtLevel, divisionType, caseNumber
                log.info(f'{self.pay} requesting.....')
                res = self.session.post(
                    'https://eapps.courts.state.va.us/ocis-rest/api/public/getCaseDetails',
                    json = pay,
                    verify=False,
                    timeout=1,
                    proxies = {'http': self.proxy} #, 'https': self.proxy}
                )
                res = res.json()['context']['entity']['payload']
                return res
            except Exception as e:
                log.error(f'{self.pay} {type(e).__name__}: {e}')
                self.proxy = random.choice(self.proxy_list)
                self.session = self.create_session()
                # ra = 1 / random.randint(50, 100)
                # time.sleep(ra)
                retries-=1
    

    def transform(self, res:dict) -> tuple[str,dict]|None:
        try:
            case = res['caseTrackingID']
            fips = res['caseCourt']['fipsCode'] + res['caseCourt']['courtCategoryCode']['value']
            div = res['caseCategory']['caseCategoryCode']
            file_name = f'{fips}_{case}_{div}.json'
            return file_name, res
        except Exception as e:
            log.error(f'{self.pay} {type(e).__name__}: {e}')


    def load(self, f_nm:str, res:dict) -> None:
        global global_counter
        try:
            with open(f'./dtl/{f_nm}', 'x', encoding='utf-8') as f:
                json.dump(res, f, indent=4)
                with counter_lock: 
                    global_counter -= 1
                log.info(f'{self.pay} written successfully {total_size - global_counter} requests executed {global_counter} remaining.')
        except FileExistsError:
            with counter_lock: 
                global_counter -= 1
            log.error(f'{self.pay} already written, skipping write {total_size - global_counter} requests executed {global_counter} remaining.')



def query_payload() -> list[dict]:
    with ddb.connect('ocis') as conn:
        sql = '''
            SELECT 
                qualifiedFips,
                courtLevel,
                divisionType,
                caseNumber
            FROM V_TODO
            WHERE PART = ?
        '''
        res = conn.execute(sql, [PARTITION]).fetchall()
        return [{'qualifiedFips': r[0], 'courtLevel': r[1], 'divisionType': r[2], 'caseNumber': r[3]} for r in res]

    

def chunk(lst: list, n: int) -> list[list]:
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def load_proxies() -> list:
    with open('proxies.txt', 'r') as f:
        proxies = f.read().split('\n')
    return proxies


def run(payload_group:list[dict]):
    search = Searcher()
    for pay in payload_group:
        res = search.extract(pay)
        if not res:
            continue
        file_name, res = search.transform(res)
        if file_name and res:
            search.load(file_name, res)



if __name__ == '__main__':
    threads = 8
    payload = query_payload()
    payload_groups = chunk(payload, threads)

    global_counter = len(payload)
    total_size = len(payload)
    
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(run, group) for group in payload_groups]
        for future in as_completed(futures):
            future.result()
