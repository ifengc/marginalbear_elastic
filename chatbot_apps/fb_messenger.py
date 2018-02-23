import os
import time
import pickle
import random
import requests
from configparser import ConfigParser
from argparse import ArgumentParser

from flask import Flask, request
from elasticsearch_dsl.connections import connections

from marginalbear_elastic.query import post_multifield_query
from marginalbear_elastic.ranking import avg_pmi
from marginalbear_elastic.utils import concat_tokens
from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram

package_dir = os.path.dirname(os.path.realpath(__name__))
config = ConfigParser()
config.read(package_dir + '/chatbot_apps/config.ini')
PAGE_TOKEN = config.get('facebook', 'page_access_key')
VERTIFY_TOKEN = config.get('facebook', 'vertify_token')

top_title = 100
top_response = 15

app = Flask(__name__)


@app.route('/fbbot/', methods=['GET'])
def verify():
    if request.args.get("hub.mode") == "subscribe" and request.args.get("hub.challenge"):
        if not request.args.get("hub.verify_token") == VERTIFY_TOKEN:
            return "Verification token mismatch", 403
        return request.args["hub.challenge"], 200


@app.route('/fbbot/', methods=['POST'])
def webhook():
    data = request.get_json()
    if data["object"] == "page":
        for entry in data["entry"]:
            for messaging_event in entry["messaging"]:
                if messaging_event.get("message"):
                    sender_id = messaging_event["sender"]["id"]
                    # recipient_id = messaging_event["recipient"]["id"]
                    message_text = messaging_event["message"]["text"]
                    reply_text = retrieve(message_text)
                    payload = {'recipient': {'id': sender_id}, 'message': {'text': reply_text}}
                    requests.post('https://graph.facebook.com/v2.9/me/messages/?access_token=' + PAGE_TOKEN, json=payload)

    return "ok", 200


def retrieve(query_str):
    try:
        query_ccjieba = ccjieba.cut(query_str.strip())
        query_unigram = unigram.cut(query_str.strip())
        results = post_multifield_query(client,
                                        index='post',
                                        query_ccjieba=concat_tokens(query_ccjieba, pos=False),
                                        query_unigram=concat_tokens(query_unigram, pos=False),
                                        top=top_title)
        ans = avg_pmi(query_unigram, results, pairs_cnt, total_pairs_cnt, tokenizer='unigram')
        reply_text = random.choice([comment for score, comment, title in ans[:top_response]])
        print("question_str: {}  reply: {}".format(query_str, reply_text))
    except Exception as err:
        print(err)
        reply_text = "ㄤㄤㄤ"

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

    app.run(host='localhost', debug=options.debug, port=int(options.port))
