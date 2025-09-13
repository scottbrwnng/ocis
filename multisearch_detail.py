import requests
import json
import duckdb as ddb
from datetime import date, timedelta
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import sys

warnings.filterwarnings('ignore')

PARTITION = int(sys.argv[1])

class Searcher:
    def __init__(self, cookie:str) -> None:
        self.cookie = cookie
        self.session = self.create_session()

    def create_session(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
            'Cookie': f'OES_TC_JSESSIONID={self.cookie}'
        }
        s = requests.Session()
        s.headers=headers
        return s

    def search(self, x:dict) -> dict: # NOTE: the combination of fips4 and casenumber, and division type should return 1 result
        while True:                    # - case details required fields in payload are
            try:                       # qualifiedFips, courtLevel, divisionType, caseNumber
                res = self.session.post(
                    'https://eapps.courts.state.va.us/ocis-rest/api/public/getCaseDetails',
                    json = x,
                    verify=False,
                    timeout = 5
                ).json()
                print('successful response. Time: ', round(time.time(), 3))
                time.sleep(random.uniform(.75, 2))
                break
            except:
                print(f'request failed for cookie: {self.cookie}')
                time.sleep(random.uniform(1, 2))
        return res
    
    def write_json(self, res:dict):
        pay = res['context']['entity']['payload']
        case = pay['caseTrackingID']
        fips = pay['caseCourt']['fipsCode'] + pay['caseCourt']['courtCategoryCode']['value']
        div = pay['caseCategory']['caseCategoryCode']
        try:
            with open(f'./dtl/{fips}_{case}_{div}.json', 'x', encoding='utf-8') as f:
                json.dump(pay, f, indent=4)
        except FileExistsError:
            print('File already exists, skipping write.')
            

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

def get_cookie():
    while True:
        try:
            url = "https://eapps.courts.state.va.us/ocis-rest/api/public/termsAndCondAccepted"
            res = requests.get(url, verify=False)
            cookie = res.cookies['OES_TC_JSESSIONID']
            break
        except:
            print('get cookie failed')
    return cookie

def chunk_list(lst: list, n: int) -> list[list]:
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

def run(payload_group:list[dict], cookie:str):
    search = Searcher(cookie)
    for x in payload_group:
        res = search.search(x)
        search.write_json(res)


if __name__ == '__main__':
    threads = 8
    payload = query_payload()
    cookie = get_cookie()
    payload_groups = chunk_list(payload, threads)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(run, group, cookie) for group in payload_groups]
        for future in futures:
            future.result()