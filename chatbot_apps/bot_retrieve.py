from elasticsearch_dsl.connections import connections

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from marginalbear_elastic.query import post_multifield_query
from marginalbear_elastic.utils import concat_tokens
from marginalbear_elastic.ranking import avg_pmi


client = connections.create_connection()
ccjieba = CCEmojiJieba()
unigram = UniGram()


def retrieve(query_str, pairs_cnt, total_pairs_cnt, top_title, top_response, random):
    query_ccjieba = ccjieba.cut(query_str.strip())
    query_unigram = unigram.cut(query_str.strip())
    results = post_multifield_query(client,
                                    index='post',
                                    query_ccjieba=concat_tokens(query_ccjieba, pos=False),
                                    query_unigram=concat_tokens(query_unigram, pos=False),
                                    top=top_title)
    ans = avg_pmi(query_unigram, results, pairs_cnt, total_pairs_cnt, tokenizer='unigram')
    reply_str = '\n'.join(['<{:.3f}> <title:{}> comment: {}'.format(score, title, comment) for score, comment, title in ans[:top_response]])
    return reply_str
