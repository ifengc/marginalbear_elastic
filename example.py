import time
import pickle
from elasticsearch_dsl.connections import connections

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from query import post_search
from utils import concat_tokens
from ranking import avg_unigram_pmi, avg_ccjieba_pmi


client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
ccjieba = CCEmojiJieba()
unigram = UniGram()


def query_ccjieba(input_sentence, pairs_cnt):
    query = ccjieba.cut(input_sentence.strip())
    results = post_search(client, index='post', tokenizer='ccjieba', query=concat_tokens(query, pos=False), top=100)
    sorted_ans = avg_ccjieba_pmi([str(i['word']) for i in query], results, pairs_cnt)
    return sorted_ans


def query_unigram(input_sentence, pairs_cnt):
    query = unigram.cut(input_sentence.strip())
    results = post_search(client, index='post', tokenizer='unigram', query=concat_tokens(query, pos=False), top=100)
    sorted_ans = avg_unigram_pmi(query, results, pairs_cnt)
    return sorted_ans


def main(tokenizer):
    t = time.time()
    print('Loading ' + tokenizer + ' pmi pickle')
    with open('pmi_' + tokenizer + '.pickle', 'rb') as f:
        pairs_cnt = dict(pickle.load(f))
    print('Pickle loaded in {:.5f}s'.format(time.time() - t))

    while True:
        input_sentence = input("Query: ")
        if tokenizer == 'ccjieba':
            sorted_ans = query_ccjieba(input_sentence, pairs_cnt)
        elif tokenizer == 'unigram':
            sorted_ans = query_unigram(input_sentence, pairs_cnt)
        print(sorted_ans[:20])


if __name__ == '__main__':
    main('unigram')
