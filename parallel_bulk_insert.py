import sys
import time
import jsonlines
from concurrent.futures import ProcessPoolExecutor, as_completed
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import parallel_bulk
from tqdm import tqdm

from app.mapping import Post
from preprocessing import json_to_doc


def parallel_insert(reader):
    futures = [e.submit(json_to_doc, obj) for obj in reader]
    kwargs = {
        'total': len(futures),
        'unit': 'parsed',
        'unit_scale': True,
        'leave': True
    }
    for f in tqdm(as_completed(futures), **kwargs):
        pass
    results = [f.result() for f in futures]
    print("Parsing done.")

    t = time.time()
    slicing = 5000
    for i in range(slicing, len(results) + slicing, slicing):
        docs = results[i - slicing:i]
        for r in parallel_bulk(client, Post.bulk_dicts(docs), thread_count=4, chunk_size=400):
            pass
        print(i, time.time() - t)
        t = time.time()


client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
e = ProcessPoolExecutor(6)

start = time.time()
input_name = sys.argv[1].strip()
parallel_insert(jsonlines.open(input_name, 'r'))
print('Total time:', time.time() - start)
