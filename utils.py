from okcom_tokenizer.preprocessing import Word


def concat_tokens(tokens, pos):
    if pos and type(tokens[0]) is Word:
        words = ' '.join([w.word for w in tokens])
        poses = ' '.join([w.pos for w in tokens])
        return words, poses
    elif not pos and type(tokens[0]) is Word:
        return ' '.join([w.word for w in tokens])
    else:
        return ' '.join(tokens)
