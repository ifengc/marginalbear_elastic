import sys
import time
import pickle
from elasticsearch_dsl.connections import connections

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from query import post_search, post_multifield_query, comment_query
from app.mapping import Post
from utils import concat_tokens
from ranking import avg_pmi


client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
ccjieba = CCEmojiJieba()
unigram = UniGram()


def query_ccjieba(input_sentence, pairs_cnt, total_pairs_cnt):
    query = ccjieba.cut(input_sentence.strip())
    results = post_search(client, index='post', tokenizer='ccjieba', query=concat_tokens(query, pos=False), top=100)
    tokenized_query = [str(i['word']) for i in query]
    sorted_ans = avg_pmi(tokenized_query, results, pairs_cnt, total_pairs_cnt, tokenizer='ccjieba')
    return sorted_ans


def query_unigram(input_sentence, pairs_cnt, total_pairs_cnt):
    query = unigram.cut(input_sentence.strip())
    results = post_search(client, index='post', tokenizer='unigram', query=concat_tokens(query, pos=False), top=100)
    sorted_ans = avg_pmi(query, results, pairs_cnt, total_pairs_cnt, tokenizer='unigram')
    return sorted_ans


def query_multifield(input_sentence, pairs_cnt, total_pairs_cnt):
    query_ccjieba = ccjieba.cut(input_sentence.strip())
    query_unigram = unigram.cut(input_sentence.strip())
    results = post_multifield_query(client,
                                    index='post',
                                    query_ccjieba=concat_tokens(query_ccjieba, pos=False),
                                    query_unigram=concat_tokens(query_unigram, pos=False),
                                    top=100)
    sorted_ans = avg_pmi(query_unigram, results, pairs_cnt, total_pairs_cnt, tokenizer='unigram')
    return sorted_ans


def update_audio_url(client, query_field, query_str, top, url):
    hits = comment_query(client, query_field, query_str, top)
    cnt = 0
    for hit in hits:
        for comment in hit.comments:
            if comment.comment_origin == query_str:
                comment.comment_audio_url = url
                cnt += 1
        doc = Post(meta={'id': hit.meta.id})
        doc.update(comments=hit.comments)
    print("{} comments updated".format(cnt))


def main(tokenizer):
    t = time.time()
    print('Loading ' + tokenizer + ' pmi pickle')
    with open('pmi_' + tokenizer + '.pickle', 'rb') as f:
        pairs_cnt = dict(pickle.load(f))
    total_pairs_cnt = sum(pairs_cnt.values())
    print('Pickle loaded in {:.5f}s'.format(time.time() - t))

    while True:
        input_sentence = input("Query: ")
        if tokenizer == 'ccjieba':
            sorted_ans = query_ccjieba(input_sentence, pairs_cnt, total_pairs_cnt)
        elif tokenizer == 'unigram':
            sorted_ans = query_unigram(input_sentence, pairs_cnt, total_pairs_cnt)
        elif tokenizer == 'multifield':
            sorted_ans = query_multifield(input_sentence, pairs_cnt, total_pairs_cnt)
        print(sorted_ans[:20])


if __name__ == '__main__':
    # tokenizer = sys.argv[1].strip()
    # main(tokenizer)
    with open('audio_url.csv', 'r') as f:
        for line in f:
            keyword, url = line.strip().strip(',')
            update_audio_url(client, "comments.comment_origin", keyword, 100, url)
            # update_audio_url(client, "comments.comment_origin", keyword, 100, "")
