import time
import fileinput
import regex
import json
from datetime import datetime
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import parallel_bulk

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from app.mapping import Post
from utils import concat_tokens


client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
ccjieba = CCEmojiJieba()
unigram = UniGram()


docs = []
start = time.time()

for i, line in enumerate(fileinput.input()):
    p = json.loads(line)

    match = regex.search(r'https://www\.ptt\.cc/bbs/(\w+)/(.+)\.html', p['url'])
    if match:
        url_id = ''.join([match.group(1), match.group(2).replace('.', '')])
    doc = Post(meta={'id': url_id})

    doc.board = match.group(1)
    doc.author = p['author'].split(" ", 1)[0]
    doc.url = p['url']
    doc.date = datetime.strptime(p['date'], '%a %b %d %H:%M:%S %Y')

    match = regex.search(r'\[(.*)\]\s*(.+)', p['title'])
    if match:
        doc.topic, doc.title_origin = match.group(1, 2)
    else:
        doc.topic, doc.title_origin = "", p['title']

    doc.title_unigram = concat_tokens(unigram.cut(doc.title_origin), False)
    doc.title_ccjieba, doc.title_pos = concat_tokens(ccjieba.cut(doc.title_origin), True)

    for push in p['push']:
        comment_author, content_origin = push.split(': ', 1)
        content_unigram = concat_tokens(unigram.cut(content_origin), False)
        content_ccjieba, content_pos = concat_tokens(ccjieba.cut(content_origin), True)
        doc.add_comment(comment_author,
                        content_origin,
                        content_unigram,
                        content_ccjieba,
                        content_pos)
    docs.append(doc)
    if i % 2000 == 0 and i != 0:
        for r in parallel_bulk(client, Post.bulk_dicts(docs), thread_count=4, chunk_size=200):
            pass
        print(i, time.time() - start)
        start = time.time()
        docs = []

for r in parallel_bulk(client, Post.bulk_dicts(docs), thread_count=4, chunk_size=200):
    pass
print(i, time.time() - start)
