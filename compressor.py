import os
import json
import gzip
import duckdb as ddb

# 1. scrape
# 2. compress
# 3. load CASE_DTL_STGN
# 4. delete dtl folder
# 5. create dtl folder
# 6. repeat

def table_size() -> int:
    with ddb.connect('ocis') as conn:
        size = conn.execute('SELECT COUNT(*) FROM CASE_DTL_STGN').fetchone()[0]
        print(size)
    return size


def comprx():
    all_files = os.listdir('./dtl/')
    size = table_size() + 1
    out = []
    for i, x in enumerate(all_files):
        i += size
        with open(f'./dtl/{x}', 'rb') as f: 
            try:
                j = json.loads(f.read())
                out.append(j)
            except Exception as e:
                print(x, '.....', e)
        if i % 10_000 == 0: 
            path = os.path.join(f'./dtl_gz/dtl_{i}.json.gz')
            with gzip.open(path, 'wt') as f:
                json.dump(out, f)
            print('creating', path)
            out = []
    path = os.path.join(f'./dtl_gz/dtl_{i}.json.gz')
    with gzip.open(path, 'wt') as f:
        json.dump(out, f)
    print('creating', path)

if __name__ == '__main__':
    comprx()