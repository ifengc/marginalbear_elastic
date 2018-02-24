from __future__ import unicode_literals

import os
import sys
import time
import pickle
import regex
import random
import urllib
from configparser import ConfigParser
from argparse import ArgumentParser

from flask import Flask, request
from elasticsearch_dsl.connections import connections
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    FollowEvent, ImageSendMessage, JoinEvent, LeaveEvent,
    MessageEvent, SourceGroup, SourceRoom, SourceUser,
    TextMessage, TextSendMessage, UnfollowEvent, AudioSendMessage
)

from marginalbear_elastic.query import post_multifield_query
from marginalbear_elastic.ranking import avg_pmi
from marginalbear_elastic.utils import concat_tokens
from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram


app = Flask(__name__)

package_dir = os.path.dirname(os.path.realpath(__name__))
config = ConfigParser()
config.read(package_dir + '/chatbot_apps/config.ini')
channel_secret = config.get('line', 'line_channel_secret')
channel_access_token = config.get('line', 'line_channel_access_token')

if channel_secret is None:
    print('Specify LINE_CHANNEL_SECRET as environment variable.')
    sys.exit(1)
if channel_access_token is None:
    print('Specify LINE_CHANNEL_ACCESS_TOKEN as environment variable.')
    sys.exit(1)

line_bot_api = LineBotApi(channel_access_token)
parser = WebhookParser(channel_secret)

top_title = 100
top_response = 15


@app.route("/linebot", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # parse webhook body
    try:
        events = parser.parse(body, signature)
    except InvalidSignatureError:
        return '', 404
    except LineBotApiError:
        return '', 400

    for event in events:
        if not isinstance(event, MessageEvent):
            continue

        if isinstance(event.message, TextMessage):
            question_string = event.message.text
            try:
                query_ccjieba = ccjieba.cut(question_string.strip())
                query_unigram = unigram.cut(question_string.strip())
                results = post_multifield_query(client,
                                                index='post',
                                                query_ccjieba=concat_tokens(query_ccjieba, pos=False),
                                                query_unigram=concat_tokens(query_unigram, pos=False),
                                                top=top_title)
                ans = avg_pmi(query_unigram, results, pairs_cnt, total_pairs_cnt, tokenizer='unigram')
                reply_text = random.choice([comment for score, comment, title in ans[:top_response]])
                print("question_str: {}  reply: {}".format(question_string, reply_text))
                msg_obj = gen_msg_obj(reply_text)
            except Exception as err:
                print(err)

        elif isinstance(event, FollowEvent) or isinstance(event, JoinEvent):
            reply_text = follow_join_reply(event)
            msg_obj = gen_msg_obj(reply_text)

        elif isinstance(event, UnfollowEvent) or isinstance(event, LeaveEvent):
            reply_text = unfollow_leave_reply(event)
            msg_obj = gen_msg_obj(reply_text)

        try:
            if msg_obj is None:
                msg_obj = gen_msg_obj("ㄤㄤㄤ")
            line_bot_api.reply_message(event.reply_token, msg_obj)
        except Exception as err:
            app.logger.error('okbot.chat_app.line_webhook, message: {}'.format(err))

    return 'OK'


def _user_id(source):
    if isinstance(source, SourceUser):
        utype = 'user'
        uid = source.user_id
    elif isinstance(source, SourceGroup):
        utype = 'group'
        uid = source.group_id
    elif isinstance(source, SourceRoom):
        utype = 'room'
        uid = source.room_id
    return utype, uid


def gen_msg_obj(reply_text, audio_duration=5000):
    if 'imgur' in reply_text:
        match_web = regex.search(r'(http|https):\/\/imgur\.com\/[a-z0-9A-Z]{7}', reply_text)
        match_jpg = regex.search(r'(http|https):\/\/(i|m)\.imgur\.com\/[a-z0-9A-Z]{7}\.jpg', reply_text)
        if match_web:
            match = match_web.group()
            imgur_url = regex.sub('http:', 'https:', match)
            print(imgur_url)
            return ImageSendMessage(original_content_url=imgur_url,
                                    preview_image_url=imgur_url)
        elif match_jpg:
            match = match_jpg.group()
            imgur_url = regex.sub('http:', 'https:', match)
            print(imgur_url)
            return ImageSendMessage(original_content_url=imgur_url,
                                    preview_image_url=imgur_url)
        else:
            return TextSendMessage(text=reply_text)
    elif 'm4a' in reply_text:
        reply_text = reply_text.replace('.m4a', '')
        m4a_url = 'https://marginalbear.ml/m4a/' + urllib.parse.quote(reply_text) + '.m4a'
        return AudioSendMessage(original_content_url=m4a_url, duration=audio_duration)
    else:
        return TextSendMessage(text=reply_text)


def follow_join_reply(event):
    query = '<FollowEvent or JoinEvent>'
    utype, uid = _user_id(event.source)
    reply_text = ""
    app.logger.info('reply message: utype: {}, uid: {}, query: {}, reply: {}'.format(utype, uid, query, reply_text))
    return reply_text


def unfollow_leave_reply(event):
    query = '<UnfollowEvent or LeaveEvent>'
    utype, uid = _user_id(event.source)
    reply_text = ""
    app.logger.info('leave or unfollow: utype: {}, uid: {}, query: {}'.format(utype, uid, query))
    return reply_text


if __name__ == "__main__":
    arg_parser = ArgumentParser(
        usage='Usage: python ' + __file__ + ' [--port <port>] [--help]'
    )
    arg_parser.add_argument('-p', '--port', default=8000, help='port')
    arg_parser.add_argument('-d', '--debug', default=False, help='debug')
    options = arg_parser.parse_args()

    client = connections.create_connection()
    ccjieba = CCEmojiJieba()
    unigram = UniGram()
    t = time.time()
    print('Loading unigram pmi pickle')
    with open(package_dir + '/data/pmi_pickle/pmi_unigram.pickle', 'rb') as f:
        pairs_cnt = dict(pickle.load(f))
    total_pairs_cnt = sum(pairs_cnt.values())
    print('Pickle loaded in {:.5f}s'.format(time.time() - t))

    app.run(debug=options.debug, port=int(options.port))
