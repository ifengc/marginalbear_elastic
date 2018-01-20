import time
import pickle
from elasticsearch_dsl.connections import connections
from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram

from query import post_search
from utils import concat_tokens
from ranking import avg_unigram_pmi


client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
ccjieba = CCEmojiJieba()
unigram = UniGram()

t = time.time()
print('Loading pickle')
with open('pmi.pickle', 'rb') as f:
    pairs_cnt = dict(pickle.load(f))
print('Pickle loaded in', time.time() - t)

while True:
    input_sentence = input("Query: ")
    query = unigram.cut(input_sentence.strip())
    results = post_search(client, index='post', tokenizer='unigram', query=concat_tokens(query, pos=False), top=100)
    score = avg_unigram_pmi(query, results, pairs_cnt)
    print(score[:20])
