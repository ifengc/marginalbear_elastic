import os
from elasticsearch_dsl.connections import connections

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from marginalbear_elastic.mapping import Post
from marginalbear_elastic.query import comment_query


client = connections.create_connection(hosts=['elastic:changeme@localhost'], timeout=20)
ccjieba = CCEmojiJieba()
unigram = UniGram()

package_dir = os.path.dirname(os.path.realpath(__name__))


def update_comment_audio_url(client, query_field, query_str, top, url):
    hits = comment_query(client, query_field, query_str, top)
    cnt = 0
    for hit in hits:
        for comment in hit.comments:
            if comment.comment_origin == query_str:
                comment.comment_audio_url = url
                cnt += 1
        doc = Post(meta={'id': hit.meta.id})
        doc.update(comments=hit.comments)
    print("{} comments updated for comment: {}".format(cnt, query_str))


if __name__ == '__main__':

    with open(package_dir + '/data/audio_url.csv', 'r') as f:
        for line in f:
            keyword, url = line.strip().split(',')
            update_comment_audio_url(client, "comments.comment_origin", keyword, None, url)
