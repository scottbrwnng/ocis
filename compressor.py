import os
import json
import gzip


def comprx():
    all_files = os.listdir('./dtl/')
    out = []
    for i, x in enumerate(all_files, start=1):  # start at 1
        with open(f'./dtl/{x}', 'rb') as f: 
            try:
                j = json.loads(f.read())
                out.append(j)
            except Exception as e:
                print(x, '.....', e)
        if i % 10_000 == 0:
            p = os.path.join(f'./dtl_gz/dtl_{i}.json.gz')
            with gzip.open(p, 'wt') as f:
                json.dump(out, f)
            print('creating', p)
            out = []
    p = os.path.join(f'./dtl_gz/dtl_{i}.json.gz')
    with gzip.open(p, 'wt') as f:
        json.dump(out, f)
    print('creating', p)

if __name__ == '__main__':
    comprx()
