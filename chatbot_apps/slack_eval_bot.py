import os
import time
import pickle
from configparser import ConfigParser

from slackbot import bot
from slackbot.bot import Bot
from slackbot.bot import listen_to
from elasticsearch_dsl.connections import connections

from okcom_tokenizer.tokenizers import CCEmojiJieba, UniGram
from marginalbear_elastic.query import post_multifield_query
from marginalbear_elastic.utils import concat_tokens
from marginalbear_elastic.ranking import avg_pmi


top_title = 100
top_response = 15

package_dir = os.path.dirname(os.path.realpath(__name__))
config = ConfigParser()
config.read(package_dir + '/chatbot_apps/config.ini')
bot.settings.API_TOKEN = config.get('slack', 'slack_token')
SLACK_CHANNEL = config.get('slack', 'slack_channel')


@listen_to(r'(.*)')
def receive_question(message, question_string):
    if message._body['channel'] == SLACK_CHANNEL:
        try:
            query_ccjieba = ccjieba.cut(question_string.strip())
            query_unigram = unigram.cut(question_string.strip())
            results = post_multifield_query(client,
                                            index='post',
                                            query_ccjieba=concat_tokens(query_ccjieba, pos=False),
                                            query_unigram=concat_tokens(query_unigram, pos=False),
                                            top=top_title)
            ans = avg_pmi(query_unigram, results, pairs_cnt, total_pairs_cnt, tokenizer='unigram')
            ans_string = '\n'.join(['<{:.3f}> <title:{}> comment: {}'.format(score, title, comment) for score, comment, title in ans[:top_response]])
            message.send(ans_string)
        except Exception as err:
            print(err)


def main():
    bot = Bot()
    bot.run()


if __name__ == '__main__':
    client = connections.create_connection()
    ccjieba = CCEmojiJieba()
    unigram = UniGram()
    t = time.time()
    print('Loading unigram pmi pickle')
    with open(package_dir + '/data/pmi_pickle/pmi_unigram.pickle', 'rb') as f:
        pairs_cnt = dict(pickle.load(f))
    total_pairs_cnt = sum(pairs_cnt.values())
    print('Pickle loaded in {:.5f}s'.format(time.time() - t))
    main()
