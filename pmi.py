import pickle
import time
import itertools as it
from collections import Counter
from elasticsearch_dsl.connections import connections
from query import post_query_all


def docfreq(title, comment):
    return [title_term + ':' + comment_term for title_term, comment_term in it.product(title, comment)]


def cnter(hits, pairs, pairs_cnter):
    for doc in hits:
        if doc['title_pos'] != 'url':
            title = doc['title_unigram']
            for comment in doc['comments']:
                if comment['comment_pos'] != 'url':
                    pairs += docfreq(title.split(' '), comment['comment_unigram'].split(' '))
    pairs_cnter += Counter(pairs)
    return pairs_cnter


hits = []
pairs = []
pairs_cnter = Counter()
start = time.time()
t1 = time.time()

client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
s = post_query_all(client, index='post')

for i, hit in enumerate(s.scan()):
    hits.append(hit)
    if i % 5000 == 0:
        pairs_cnter = cnter(hits, pairs, pairs_cnter)
        print(i, time.time() - t1)
        print(pairs_cnter.most_common(10))

        hits = []
        pairs = []
        t1 = time.time()

pairs_cnter = cnter(hits, pairs, pairs_cnter)
print(i, time.time() - start)
print("Dump to pickle")
with open('pmi.pickle', 'wb') as f:
    pickle.dump(pairs_cnter, f, pickle.HIGHEST_PROTOCOL)
