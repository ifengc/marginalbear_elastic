import math
import itertools as it


def avg_unigram_pmi(query, results, pairs_cnt):
    output = {}
    total_pairs_cnt = sum(pairs_cnt.values())
    for result in results:
        title_terms = result['term_vectors']['title_unigram']['terms']
        title_sum_ttf = result['term_vectors']['title_unigram']['field_statistics']['sum_ttf']
        comment_terms = result['term_vectors']['comments.comment_unigram']['terms']
        comment_sum_ttf = result['term_vectors']['comments.comment_unigram']['field_statistics']['sum_ttf']

        pmi_dict = {}
        for title_term, comment_term in it.product(title_terms.keys(), comment_terms.keys()):
            key = title_term + ':' + comment_term
            if key in pairs_cnt:
                p_x = title_terms[title_term]['ttf'] / title_sum_ttf
                p_y = comment_terms[comment_term]['ttf'] / comment_sum_ttf
                p_xy = pairs_cnt[key] / total_pairs_cnt
                pmi_dict[key] = math.log((p_x * p_y)) / math.log(p_xy) - 1

        for comment in result['comments']:
            if comment['comment_pos'] == 'url':
                output[result['score']] = (comment['comment_unigram'], result['title_origin'])
            else:
                comment_unigram = comment['comment_unigram'].split(" ")
                comment_score = 0
                cnter = 0
                for query_term, comment_term in it.product(query, comment_unigram):
                    key = query_term + ':' + comment_term
                    if key in pmi_dict:
                        comment_score += pmi_dict[key]
                    cnter += 1
                comment_score /= cnter
                output[comment_score * result['score']] = (comment['comment_origin'], result['title_origin'])
    return sorted(output.items(), reverse=True)


def avg_ccjieba_pmi(query, results, pairs_cnt):
    output = {}
    total_pairs_cnt = sum(pairs_cnt.values())
    for result in results:
        title_terms = result['term_vectors']['title_ccjieba']['terms']
        title_sum_ttf = result['term_vectors']['title_ccjieba']['field_statistics']['sum_ttf']
        comment_terms = result['term_vectors']['comments.comment_ccjieba']['terms']
        comment_sum_ttf = result['term_vectors']['comments.comment_ccjieba']['field_statistics']['sum_ttf']

        pmi_dict = {}
        for title_term, comment_term in it.product(title_terms.keys(), comment_terms.keys()):
            key = title_term + ':' + comment_term
            if key in pairs_cnt:
                p_x = title_terms[title_term]['ttf'] / title_sum_ttf
                p_y = comment_terms[comment_term]['ttf'] / comment_sum_ttf
                p_xy = pairs_cnt[key] / total_pairs_cnt
                pmi_dict[key] = math.log((p_x * p_y)) / math.log(p_xy) - 1

        for comment in result['comments']:
            if comment['comment_pos'] == 'url':
                output[result['score']] = (comment['comment_ccjieba'], result['title_origin'])
            else:
                comment_ccjieba = comment['comment_ccjieba'].split(" ")
                comment_score = 0
                cnter = 0
                for query_term, comment_term in it.product(query, comment_ccjieba):
                    key = query_term + ':' + comment_term
                    if key in pmi_dict:
                        comment_score += pmi_dict[key]
                    cnter += 1
                comment_score /= cnter
                output[comment_score * result['score']] = (comment['comment_origin'], result['title_origin'])
    return sorted(output.items(), reverse=True)
