import requests
import json
from datetime import date, timedelta, datetime
import warnings
import random
import gzip
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

hs = logging.StreamHandler()
hf = logging.FileHandler('logs.log')
logging.basicConfig(
    level=logging.INFO, handlers=[hs, hf], 
    format='xxx -- %(asctime)s [%(levelname)s] %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger(__name__)

warnings.filterwarnings('ignore')






class Searcher:
    def __init__(self) -> None:
        self.pay = None
        self.session = self.create_session()
        self.proxy_list = load_proxies()
        self.proxy = random.choice(self.proxy_list)
        self.idx = None
        self.output = []


    def create_session(self):

        def get_cookie() -> str:
            while True:
                try:
                    url = "https://eapps.courts.state.va.us/ocis-rest/api/public/termsAndCondAccepted"
                    res = requests.get(url, verify=False)
                    cookie = res.cookies['OES_TC_JSESSIONID']
                    break
                except Exception as e:
                    print(f'{self.pay} {type(e).__name__}: {e}')
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

    def extract(self, _date:date) -> dict:
        self._date = _date
        self.pay = {"courtLevels":[],"divisions":[],"selectedCourts":[],"searchString":[self._date.strftime('%m/%d/%Y')],"searchBy":"HD"}
        if self.idx: # may not need if statement
            self.pay.update({"endingIndex":self.idx})
        retries = 0 
        while retries < 5:
            try:
                res = self.session.post(
                    'https://eapps.courts.state.va.us/ocis-rest/api/public/search',
                    json = self.pay,
                    verify=False,
                    proxies = {'http': self.proxy},
                    timeout=5
                )
                res = res.json()
                return res
            except requests.JSONDecodeError:
                log.error(res.content)
            except KeyError as e: # TODO
                pass
                # if e == "name='OES_TC_JSESSIONID', domain=None, path=None":
                    # self.session = self.create_session()
            except Exception as e:
                log.error(f'{self.pay} - retries: {retries} - {type(e).__name__}: {e}')
                time.sleep(1 / random.randint(5, 10))
                if retries > 0:
                    time.sleep(1 / random.randint(5, 10))
                if retries > 1:
                    self.proxy = random.choice(self.proxy_list)
                if retries > 2:
                    self.session = self.create_session()
                retries+=1

    def last_result_index(self, res:dict=None) -> None:
        if res.get('context').get('entity').get('payload').get('lastResponseIndex'):
            self.idx = res.get('context').get('entity').get('payload').get('lastResponseIndex')
        else:
            self.idx = None
        log.info(f'{self.pay} updated self.idx: {self.idx}')
            
    def increase_date(self) -> None: 
        self._date += timedelta(days=1)
    
    def append_res(self, res:dict) -> None:
        self.output.append(res)

    def load(self):
        f_nm = f'./case_hearings/{self._date}.json.gz'
        try:
            with gzip.open(f_nm, 'wt') as f:
                json.dump(self.output, f)
            log.info(f'{self.pay} successfully written {f_nm}')
        except Exception as e:
            log.error(f'{self.pay} {type(e).__name__}: {e}')


def load_proxies() -> list:
    with open('proxies.txt', 'r') as f:
        proxies = f.read().split('\n')
    return proxies


def date_range(start_date:date, end_date:date) -> list:
    # TODO: this needs to be query_payload and pulling from clean datastore
    diff = (end_date-start_date).days
    old = {datetime.strptime(x[:10], '%Y-%m-%d').date() for x in os.listdir('./case_hearings') if '.json' in x}
    d = []
    for x in range(diff):
        new = start_date + timedelta(days=x)
        if new not in old:
            d.append(start_date + timedelta(days=x))
    return d


def run(single_date:date):
    log.info(f'Running {single_date}')
    search = Searcher()
    while True:
        res = search.extract(single_date)
        if not res:
            continue
        search.append_res(res)
        search.last_result_index(res)
        if not search.idx:
            search.load()
            break  # no more results for this date
        
    log.info(f'Finished {single_date}')



if __name__ == '__main__':
    start_date = date(year = 2010, month = 1, day = 1)
    end_date = date.today()
    dates = date_range(start_date, end_date)
    for d in dates:
        run(d)