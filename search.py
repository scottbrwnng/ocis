import requests
import json
from datetime import date, timedelta, datetime
import warnings
import uuid
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

def create_session():
    url = "https://eapps.courts.state.va.us/ocis-rest/api/public/termsAndCondAccepted"
    res = requests.get(url, verify=False)
    cook = res.cookies['OES_TC_JSESSIONID']

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        'Cookie': f'OES_TC_JSESSIONID={cook}'
    }
    s = requests.Session()
    s.headers=headers
    return s
    
def date_range(start_date:date, end_date:date) -> list:
    diff = (end_date-start_date).days
    old = {datetime.strptime(x[:10], '%Y-%m-%d').date() for x in os.listdir('./dumps') if '.json' in x}
    d = []
    for x in range(diff):
        new = start_date + timedelta(days=x)
        if new not in old:
            d.append(start_date + timedelta(days=x))
    return d


class Searcher:
    def __init__(self, start_date:date) -> None:
        self.session = create_session()
        self._date = start_date
        self.idx = None
        self.out = []

    def extract(self) -> dict:
        _date = self._date.strftime('%m/%d/%Y')
        payload = {"courtLevels":[],"divisions":["Adult Criminal/Traffic"],"selectedCourts":[],"searchString":[_date],"searchBy":"HD"}
        if self.idx: # may not need if statement
            payload.update({"endingIndex":self.idx})
        res = self.session.post(
            'https://eapps.courts.state.va.us/ocis-rest/api/public/search',
            json = payload,
            verify=False,
            timeout=10
        ).json()
        return res
        
    def last_result_index(self, res:dict=None) -> None:
        if res.get('context').get('entitiy').get('payload').get('lastResponseIndex'):
            self.idx = res.get('context').get('entitiy').get('payload').get('lastResponseIndex')
        else:
            self.idx = None
            
    def increase_date(self) -> None: 
        self._date += timedelta(days=1)

    def write_json(self, res:dict):
        with open(f'./dumps/{self._date}_{self.idx}.json', 'w', encoding='utf-8') as f:
            json.dump(res, f, indent=4)


def run(single_date:date):
    print('running', single_date)
    search = Searcher(single_date)
    while True:
        res = search.extract()
        search.write_json(res)
        search.last_result_index(res)
        if not search.idx:
            break  # no more results for this date
    print('Finished', single_date)

def main():
    start_date = date(year = 1980, month = 1, day = 1)
    end_date = date.today()
    dates = date_range(start_date, end_date)[10:]
    # threads are fine for network I/O and avoid pickling problems
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(run, d): d for d in dates}
        for future in futures:
            future.result()


if __name__ == '__main__':
    main()