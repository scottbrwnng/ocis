I am running a two part webscraper that will pull case information from a court website.


Orchestrate a webscraper and data pipeline that will parse the response from the first scraper into a table so that it can be used in the second scraper that will get the court case details. I want to make a raw bucket/table for the case details that will be append only for the case details returned from the second scraper. Its ok to have multiple records for the same case but with different amounts of hearing dates. the clean table should be a create or refresh materialized view or table that queries the instance of a case details response if the response contains the most recent scheduleDate in the hearing date fields.

I want to do http requests only for new cases and existing cases that have a new hearing date that I have not already ingested. This means a particular case details may have already been ingested, but a new hearing for a future date was captured during the first script, search.py.  


This is how i want to set up everything:

1. First scraper (search.py) runs for every hearing date between START_DATE and today and writes json response to here: s3://ocis-response-search/Raw/
2. A sql query or pyspark script that will append/write to a clean table stored here: s3://ocis-response-search/Clean/
3. A sql query or pyspark script that will query qualifiedfips, casenumber, divisionType, and hearingDate from s3://ocis-response-search/Clean/ and return the rows ONLY if a combination of qualifiedfips, casenumber, and divisionType has a hearingDate that comes after the most recent hearingDate in the s3://ocis-response-getcasedetails/Clean/ table. 
4. Using the results from the above query, run the second script search_detail.py to scrape to get case details. Write the response jsons to s3://ocis-response-getcasedetails/Raw/
5. A sql query or pyspark script that will append/write to a clean table stored here: s3://ocis-response-getcasedetails/Clean/


Rules:
 - a case can be uniquely identified by the combination of the fields qualifiedfips, casenumber, and divisionType. A case can have one or many hearingDates.


Notes:

search.py and search_detail.py are going to be uploaded to ECR images that run using ECS tasks.

I need to decide what services to use the run the queries and how to orchestrate the system. 
Both scrapers only need to run once a day or week, it does not have to be streamed.
Should I be running the pyspark scripts/queries from inside the ECR images, or materialize a view/table outside of the scraper using glue or athena?


Right now I have a script for step 2 but provide any suggestions you have. I am currently running the script manually in Athena but am not sure if that is the best option for orchestration:
df = (
    spark.table("ocis_db.raw")
        .withColumn("t", explode("array"))
        .withColumn("sr", explode("t.context.entity.payload.searchResults"))
        .select("sr.*")
)
df.write.parquet('s3://ocis-response-search/Clean/', partitionBy='hearingdate', mode='overwrite')


Here is an example of the output:
+-------------+----------+------------+----------+-------------------+--------------------+-----------+-------------+-----------+--------------------+--------+--------------------+-----------+-------------+
|qualifiedfips|courtlevel|divisiontype|casenumber|formattedcasenumber|                name|offensedate|chargeamended|codesection|          chargedesc|casetype|         hearingdate|hearingtype|hearingresult|
+-------------+----------+------------+----------+-------------------+--------------------+-----------+-------------+-----------+--------------------+--------+--------------------+-----------+-------------+
|         730C|         C|           R|7900045000|      CR79000450-00|HARRIS, MANDEL VA...| 10/31/1979|        false|    18.2-91|  STATUTORY BURGLARY|       F| 01/02/1980, 9:00 AM|       TRYL|         TRYD|






See code and response schemas for both scrapers below:




search.py:

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

Here is an example of the output from this script:

[{"context": {"committingOutputStream": {"bufferSize": 0, "directWrite": true, "isCommitted": false, "isClosed": false}, "entity": {"status": "SUCCESS", "payload": {"noOfRecords": 3, "searchResults": [{"qualifiedFips": "730C", "courtLevel": "C", "divisionType": "R", "caseNumber": "7900045000", "formattedCaseNumber": "CR79000450-00", "name": "HARRIS, MANDEL VAUGHAN", "offenseDate": "10/31/1979", "chargeAmended": false, "codeSection": "18.2-91", "chargeDesc": "STATUTORY BURGLARY", "caseType": "F", "hearingDate": "01/02/1980, 9:00 AM", "hearingType": "TRYL", "hearingResult": "TRYD"}, {"qualifiedFips": "121C", "courtLevel": "C", "divisionType": "R", "caseNumber": "9401256300", "formattedCaseNumber": "CR94012563-00", "name": "LESTER, RICKIE LEON", "chargeAmended": false, "chargeDesc": "DIST MARIJUANA", "caseType": "F", "hearingDate": "01/02/1980, 9:00 AM", "hearingType": "GJ", "hearingResult": "TB"}, {"qualifiedFips": "121C", "courtLevel": "C", "divisionType": "R", "caseNumber": "9401256301", "formattedCaseNumber": "CR94012563-01", "name": "LESTER, RICKIE LEON", "chargeAmended": false, "chargeDesc": "DIST MARIJUANA", "caseType": "F", "hearingDate": "01/02/1980, 9:00 AM", "hearingType": "GJ", "hearingResult": "TB"}]}}, "entityType": {"type": "scv.oes.ocis.rest.dto.ApiResponse", "rawType": "scv.oes.ocis.rest.dto.ApiResponse"}, "entityStream": {"bufferSize": 0, "directWrite": true, "isCommitted": false, "isClosed": false}}, "status": "OK", "closed": false, "buffered": false}]


This is the code for search_detail.py. Right now it is configued to run locally. I still need to set it up so it can be used in AWS:

import requests
import json
import duckdb as ddb
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import random
import sys
import time
import logging

hs = logging.StreamHandler()
hf = logging.FileHandler('logs.log')
logging.basicConfig(
    level=logging.INFO, handlers=[hs, hf], 
    format='xxx -- %(asctime)s [%(levelname)s] [%(threadName)s] %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

warnings.filterwarnings('ignore')

# PARTITION = int(sys.argv[1])

counter_lock = Lock()


class Searcher:

    def __init__(self) -> None:
        self.pay = None
        self.session = self.create_session()
        self.proxy_list = load_proxies()
        self.proxy = random.choice(self.proxy_list)
        

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
        retries = 0
        while retries < 4:             # - case details required fields in payload are
            try:                       # qualifiedFips, courtLevel, divisionType, caseNumber
                log.info(f'{self.pay} requesting, retries: {retries}')
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
                time.sleep(1 / random.randint(5, 10))
                if retries > 0:
                    time.sleep(1 / random.randint(5, 10))
                if retries > 1:
                    pass
                if retries > 2:
                    self.proxy = random.choice(self.proxy_list)
                    self.session = self.create_session()
                retries+=1
    

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
            with open(f'./case_details/{f_nm}', 'x', encoding='utf-8') as f:
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
            --WHERE PART = ?
        '''
        res = conn.execute(sql).fetchall()
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


Here is an example of the output/responses from search_detail.py:

{
        "locality": {
            "localityName": "COMMONWEALTH OF VA",
            "localityCode": "C"
        },
        "caseTrackingID": "1902707600",
        "caseCategory": {
            "caseCategoryCode": "C",
            "criminalCaseIndicator": "Y"
        },
        "caseActiveIndicator": {
            "value": "Y"
        },
        "caseCharge": {
            "chargeFilingDate": "04/18/2019",
            "offenseDate": "04/17/2019",
            "summonsNumber": "0431936448",
            "arrestDate": "04/17/2019",
            "amendedCharge": {},
            "originalCharge": {
                "chargeDescriptionText": "POSSESSION OF MARIJUANA",
                "caseTypeCode": "M",
                "codeSection": "18.2-250.1"
            },
            "offenseTrackingNumber": "087GC1902707600"
        },
        "caseCourt": {
            "courtCategoryCode": {
                "value": "G"
            },
            "fipsCode": "087"
        },
        "caseOtherInfo": {},
        "disposition": {
            "dispositionInfo": {
                "dispositionDate": "08/06/2019",
                "dispositionText": "G"
            },
            "probationInfo": {
                "duration": {
                    "years": 0,
                    "months": 0,
                    "days": 0
                }
            }
        },
        "dmvInformation": {
            "driverLicense": {
                "licenseLoss": {
                    "days": 0,
                    "months": 6,
                    "years": 0,
                    "licenseSurrenderCode": "0"
                },
                "licenseRestrictions": {
                    "startDate": "01/09/2023",
                    "restrictions": "AEG"
                }
            }
        },
        "sentencingInformation": {
            "sentence": {
                "months": 12,
                "days": 0,
                "hours": 0
            },
            "sentenceSuspended": {
                "months": 12,
                "days": 0,
                "hours": 0
            }
        },
        "financialInformation": {
            "fines": {
                "amount": {
                    "decimal": 100
                },
                "dueDate": {
                    "date": 1565064000000
                }
            },
            "restitution": {
                "paidIndicator": "N"
            },
            "costs": {
                "amount": {
                    "decimal": 286
                }
            }
        },
        "appealCase": {},
        "caseParticipant": [
            {
                "sequenceNumber": {
                    "identificationID": "1"
                },
                "participantCode": "DEF",
                "contactInformation": {
                    "primaryAddress": {
                        "locationCityName": "HIGHLAND SPRINGS",
                        "locationState": "VA",
                        "locationCountry": "US",
                        "locationPostalCode": "23075"
                    },
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                },
                "attorneyDetails": [
                    {
                        "id": "087GC1902707600_P1_A1",
                        "judicialOfficialCategoryText": "A",
                        "attorneyName": {
                            "personGivenName": "STEVE",
                            "personSurName": "BRYANT",
                            "fullName": "BRYANT, STEVE"
                        },
                        "sequenceNumber": {
                            "identificationID": "1"
                        }
                    }
                ],
                "personalDetails": {
                    "race": "B",
                    "gender": "M",
                    "maskedBirthDate": "02/16"
                },
                "participantStatus": "O"
            },
            {
                "sequenceNumber": {
                    "identificationID": "1"
                },
                "participantCode": "CMP",
                "contactInformation": {
                    "personName": {
                        "personGivenName": "A",
                        "personMiddleName": "G",
                        "personSurName": "BARNES",
                        "fullName": "BARNES, A G"
                    }
                }
            }
        ],
        "caseHearing": [
            {
                "courtActivityScheduleDay": {
                    "scheduleDate": "04/29/2019",
                    "scheduleDayStartTime": {
                        "time": "09:00:00"
                    }
                },
                "courtRoom": "3",
                "hearingType": "AR",
                "hearingResult": "C",
                "sequenceNumber": {
                    "identificationID": "1"
                }
            },
            {
                "courtActivityScheduleDay": {
                    "scheduleDate": "08/06/2019",
                    "scheduleDayStartTime": {
                        "time": "11:00:00"
                    }
                },
                "courtRoom": "3",
                "hearingType": "AJ",
                "hearingResult": "F",
                "sequenceNumber": {
                    "identificationID": "2"
                }
            }
        ],
        "serviceInfo": [
            {
                "sequenceNumber": {
                    "identificationID": "1"
                },
                "noticeType": "326C",
                "dateIssued": "04/30/2019",
                "serviceContact": {
                    "personName": {
                        "personSurName": "OFFICER A  G  BARNES #1434",
                        "fullName": "OFFICER A. G. BARNES #1434"
                    }
                }
            },
            {
                "sequenceNumber": {
                    "identificationID": "2"
                },
                "noticeType": "225",
                "serviceContact": {
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                }
            },
            {
                "sequenceNumber": {
                    "identificationID": "3"
                },
                "noticeType": "225",
                "serviceContact": {
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                }
            },
            {
                "sequenceNumber": {
                    "identificationID": "4"
                },
                "noticeType": "225",
                "serviceContact": {
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                }
            },
            {
                "sequenceNumber": {
                    "identificationID": "5"
                },
                "noticeType": "225",
                "serviceContact": {
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                }
            },
            {
                "sequenceNumber": {
                    "identificationID": "6"
                },
                "noticeType": "225",
                "serviceContact": {
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                }
            },
            {
                "sequenceNumber": {
                    "identificationID": "7"
                },
                "noticeType": "225",
                "serviceContact": {
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                }
            },
            {
                "sequenceNumber": {
                    "identificationID": "8"
                },
                "noticeType": "225",
                "serviceContact": {
                    "personName": {
                        "personGivenName": "DEONTA",
                        "personMiddleName": "PEREZ",
                        "personSurName": "BRANDON",
                        "fullName": "BRANDON, DEONTA PEREZ"
                    }
                }
            }
        ]
    }