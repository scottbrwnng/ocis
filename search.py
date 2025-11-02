import requests
import json
from datetime import date, timedelta, datetime
import warnings
import random
import gzip
import time
import os
import boto3

warnings.filterwarnings(action='ignore')

BUCKET_NAME = os.getenv("OUTPUT_BUCKET")  # Set this in Lambda environment variables
s3 = boto3.client('s3')


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
                print(res.content)
            except KeyError as e: # TODO
                pass
                # if e == "name='OES_TC_JSESSIONID', domain=None, path=None":
                    # self.session = self.create_session()
            except Exception as e:
                print(f'{self.pay} - retries: {retries} - {type(e).__name__}: {e}')
                time.sleep(1 / random.randint(5, 10))
                if retries > 0:
                    time.sleep(1 / random.randint(5, 10))
                if retries > 1:
                    self.proxy = random.choice(self.proxy_list)
                if retries > 2:
                    self.session = self.create_session()
                retries+=1

    def last_result_index(self, res:dict=None) -> None:
        # TODO: need to need to account for the following error:	
            # File "/app/search.py", line 85, in last_result_index
            # if res.get('context').get('entity').get('payload').get('lastResponseIndex'):
            # AttributeError: 'NoneType' object has no attribute 'get'
        if res.get('context').get('entity').get('payload').get('lastResponseIndex'):
            self.idx = res.get('context').get('entity').get('payload').get('lastResponseIndex')
        else:
            self.idx = None
        print(f'{self.pay} updated self.idx: {self.idx}')
            
    def increase_date(self) -> None: 
        self._date += timedelta(days=1)
    
    def append_res(self, res:dict) -> None:
        self.output.append(res)

    def load(self):
        f_nm = f'Raw/{self._date}.json.gz'
        try:
            s3.put_object(
                Bucket=BUCKET_NAME, 
                Key=f_nm, 
                Body=gzip.compress(json.dumps(self.output).encode('utf-8'))
            )
            print(f'{self.pay} successfully written {f_nm}')
        except Exception as e:
            print(f'{self.pay} {type(e).__name__}: {e}')


def load_proxies() -> list:
    with open('proxies.txt', 'r') as f:
        proxies = f.read().split('\n')
    return proxies


def date_range(start_date:date, end_date:date) -> list:
    # TODO: this needs to be query_payload and pulling from clean datastore
    diff = (end_date-start_date).days
    # old = {datetime.strptime(x[:10], '%Y-%m-%d').date() for x in os.listdir(BUCKET_NAME) if '.json' in x}
    old = []
    d = []
    for x in range(diff):
        new = start_date + timedelta(days=x)
        if new not in old:
            d.append(start_date + timedelta(days=x))
    return d


def run(search_date:date):
    print(f'Running {search_date}')
    search = Searcher()
    while True:
        res = search.extract(search_date)
        if not res:
            continue
        search.append_res(res)
        search.last_result_index(res)
        if not search.idx:
            search.load()
            del search # clear memory
            break  # no more results for this date
    print(f'Finished {search_date}')


if __name__ == '__main__':
    
    start_date_str = os.getenv("START_DATE")
    if start_date_str:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    else:
        start_date = date(year=1993, month=3, day=1)

    end_date = date.today()
    dates = date_range(start_date, end_date)

    for d in dates:
        run(d)




