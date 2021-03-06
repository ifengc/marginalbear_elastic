import time
import pathlib
from datetime import datetime, timedelta
import jsonlines
from concurrent.futures import ProcessPoolExecutor, as_completed
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import parallel_bulk
from tqdm import tqdm

from marginalbear_elastic.mapping import Post
from marginalbear_elastic.preprocessing import json_to_doc


def parallel_insert(client, executors, filename):
    start = time.time()
    reader = jsonlines.open(filename, 'r')
    futures = [executors.submit(json_to_doc, obj) for obj in reader]
    kwargs = {
        'total': len(futures),
        'unit': 'parsed',
        'unit_scale': True,
        'leave': True
    }
    for f in tqdm(as_completed(futures), **kwargs):
        pass
    results = [f.result() for f in futures]
    print("Json preprocessing done in {:.5f}s".format(start - time.time()))

    t = time.time()
    slicing = 5000
    for i in range(slicing, len(results) + slicing, slicing):
        docs = results[i - slicing:i]
        for r in parallel_bulk(client, Post.bulk_dicts(docs), thread_count=4, chunk_size=400):
            pass
        print('{} insertion done in {:.5f}s'.format(i, time.time() - t))
        t = time.time()
    print('Total elapsed time: {:.5f}s'.format(time.time() - start))


client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
executors = ProcessPoolExecutor(6)

found_jl = pathlib.Path('output/').glob('*.jl')
date = datetime.now() - timedelta(days=1)
date_str = date.strftime('%Y-%m-%d')
filenames = [str(i) for i in found_jl if date_str in i.name]
for filename in filenames:
    parallel_insert(client, executors, filename)
