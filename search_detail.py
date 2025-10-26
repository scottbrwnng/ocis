import requests
import json
import duckdb as ddb
import warnings
import random
import sys
import logging
import time

hs = logging.StreamHandler()
hf = logging.FileHandler('logs.log')
logging.basicConfig(
    level=logging.INFO, handlers=[hs, hf], 
    format='%(asctime)s [%(levelname)s] %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

warnings.filterwarnings('ignore')

PARTITION = int(sys.argv[1])


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


    def extract(self, pay:dict) -> dict | None: # NOTE: the combination of fips4 and casenumber, and division type should return 1 result
        self.pay = pay
        retries = 3
        while retries > 0:                    # - case details required fields in payload are
            try:                       # qualifiedFips, courtLevel, divisionType, caseNumber
                log.info(f'{self.pay} requesting.....')
                res = self.session.post(
                    'https://eapps.courts.state.va.us/ocis-rest/api/public/getCaseDetails',
                    json = pay,
                    verify=False,
                    timeout=2,
                    proxies = {'http': self.proxy} #, 'https': self.proxy}
                )
                res = res.json()['context']['entity']['payload']
                return res
            except Exception as e:
                log.error(f'{self.pay} Exception: {e}... {res.content}')
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
        global counter
        try:
            with open(f'./dtl/{f_nm}', 'x', encoding='utf-8') as f:
                json.dump(res, f, indent=4)
            counter += 1
            log.info(f'{self.pay} written successfully. {counter} requests executed {total_size - counter} remaining.')
        except FileExistsError:
            counter += 1
            log.error(f'{self.pay} already written, skipping write. {counter} requests executed {total_size - counter} remaining.')


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

    

def load_proxies() -> list:
    with open('proxies.txt', 'r') as f:
        proxies = f.read().split('\n')
    return proxies




if __name__ == '__main__':
    payload = query_payload()
    counter = 0
    total_size = len(payload)
    search = Searcher()
    for pay in payload:
        res = search.extract(pay)
        if not res:
            continue
        file_name, res = search.transform(res)
        if file_name and res:
            search.load(file_name, res)
