from datetime import datetime

import regex

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from mapping import Post
from utils import concat_tokens


ccjieba = CCEmojiJieba()
unigram = UniGram()


def json_to_doc(obj):
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
        try:
            comment_author, comment_origin = push.split(':', 1)
            comment_origin = comment_origin.strip()
            if comment_author in push_dict:
                push_dict[comment_author] += comment_origin
            else:
                push_dict[comment_author] = comment_origin
        except Exception as err:
            print(err)
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
