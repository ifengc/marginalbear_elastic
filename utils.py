import os
import logging
from configparser import ConfigParser
from datetime import datetime
from logging import StreamHandler, FileHandler


from okcom_tokenizer.preprocessing import Word


def concat_tokens(tokens, pos):
    if pos and type(tokens[0]) is Word:
        words = ' '.join([w.word for w in tokens])
        poses = ' '.join([w.pos for w in tokens])
        return words, poses
    elif not pos and type(tokens[0]) is Word:
        return ' '.join([str(w.word) for w in tokens])
    else:
        return ' '.join(tokens)


def marginal_logger():
    package_dir = os.path.dirname(os.path.realpath(__package__))
    config = ConfigParser()
    config.read(package_dir + '/conf/local.ini')
    log_dir = config.get('main', 'log_dir')

    date = datetime.strftime(datetime.now(), '%Y%m%d')
    filename = log_dir + '' + date + '.log'

    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S')

    file_hdlr = FileHandler(filename)
    file_hdlr.setFormatter(formatter)
    logger.addHandler(file_hdlr)

    console_hdlr = StreamHandler()
    console_hdlr.setFormatter(formatter)
    logger.addHandler(console_hdlr)

    logger.setLevel(logging.INFO)

    return logger
