import os
import json
import gzip
import duckdb as ddb
import shutil



def table_size() -> int:
    with ddb.connect('ocis') as conn:
        size = conn.execute('SELECT COUNT(*) FROM CASE_DTL_STGN').fetchone()[0]
        print(size)
    return size


def compress():
    all_files = os.listdir('./case_details/')
    size = table_size() + 1
    out = []
    for i, x in enumerate(all_files):
        i += size
        with open(f'./case_details/{x}', 'rb') as f: 
            try:
                j = json.loads(f.read())
                out.append(j)
            except Exception as e:
                print(x, '.....', e)
        if i % 10_000 == 0: 
            path = os.path.join(f'./case_details_gz/dtl_{i}.json.gz')
            with gzip.open(path, 'wt') as f:
                json.dump(out, f)
            print('creating', path)
            out = []
    path = os.path.join(f'./case_details_gz/dtl_{i}.json.gz')
    with gzip.open(path, 'wt') as f:
        json.dump(out, f)
    print('creating', path)


def load_table():
    sql = '''
        SET preserve_insertion_order = false;
        CREATE OR REPLACE TABLE CASE_DTL_STGN AS
        SELECT
            *,
            caseCourt.fipsCode || caseCourt.courtCategoryCode.value as qualifiedFips,
            caseCourt.courtCategoryCode.value as courtLevel,
            caseCategory.caseCategoryCode as divisionType,
            caseTrackingID as caseNumber
        FROM READ_JSON('./case_details_gz/dtl*.json.gz');
    '''
    print('loading table...')
    with ddb.connect('ocis') as conn:
        conn.execute(sql)
    print('table created')


def reset_dir():
    if os.path.exists('./case_details'):
        shutil.rmtree('./case_details')
        print('./case_details deleted')
    if not os.path.exists('./case_details'):
        os.mkdir('./case_details')
        print('./case_details created')
    



if __name__ == '__main__':
    compress()
    load_table()
    reset_dir()