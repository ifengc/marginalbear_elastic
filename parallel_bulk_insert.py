import sys
import time
import regex
import jsonlines
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import parallel_bulk
from tqdm import tqdm

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from app.mapping import Post
from utils import concat_tokens


def parse(obj):
    match = regex.search(r'https://www\.ptt\.cc/bbs/(\w+)/(.+)\.html', obj['url'])
    if match:
        url_id = ''.join([match.group(1), match.group(2).replace('.', '')])
    doc = Post(meta={'id': url_id})

    doc.board = match.group(1)
    doc.author = obj['author'].split(" ", 1)[0]
    doc.url = obj['url']
    try:
        doc.date = datetime.strptime(obj['date'], '%a %b %d %H:%M:%S %Y')
    except Exception as err:
        print(err)

    match = regex.search(r'\[(.*)\]\s*(.+)', obj['title'])
    if match:
        doc.topic, doc.title_origin = match.group(1, 2)
    else:
        doc.topic, doc.title_origin = "", obj['title']

    doc.title_unigram = concat_tokens(unigram.cut(doc.title_origin), False)
    doc.title_ccjieba, doc.title_pos = concat_tokens(ccjieba.cut(doc.title_origin), True)
    doc.title_quality = 1.0

    push_dict = {}
    for push in obj['push']:
        comment_author, comment_origin = push.split(':', 1)
        comment_origin = comment_origin.strip()
        if comment_author in push_dict:
            push_dict[comment_author] += comment_origin
        else:
            push_dict[comment_author] = comment_origin
    for comment_author, comment_origin in push_dict.items():
        comment_unigram = concat_tokens(unigram.cut(comment_origin), False)
        comment_ccjieba, comment_pos = concat_tokens(ccjieba.cut(comment_origin), True)
        comment_audio_url = ''
        comment_quality = 1.0
        doc.add_comment(comment_author,
                        comment_origin,
                        comment_unigram,
                        comment_ccjieba,
                        comment_pos,
                        comment_audio_url,
                        comment_quality)
    return doc


def parallel_insert(reader):
    futures = [e.submit(parse, obj) for obj in reader]
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
ccjieba = CCEmojiJieba()
unigram = UniGram()
e = ProcessPoolExecutor(6)

start = time.time()
input_name = sys.argv[1].strip()
parallel_insert(jsonlines.open(input_name, 'r'))
print('Total time:', time.time() - start)
