import math
import itertools as it


def avg_unigram_pmi(query, results, pairs_cnt):
    ans = {}
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

        comment_scores = []
        tmp_ans = {}
        for comment in result['comments']:
            comment_unigram = comment['comment_unigram'].split(" ")
            comment_score = 0
            cnter = 0
            for query_term, comment_term in it.product(query, comment_unigram):
                key = query_term + ':' + comment_term
                if key in pmi_dict:
                    comment_score += pmi_dict[key]
                cnter += 1
            comment_score /= cnter
            comment_scores.append(comment_score)
            tmp_ans[(comment['comment_pos'], comment['comment_origin'], result['title_origin'])] = comment_score * result['score']

        max_comment_score = max(comment_scores)
        for (pos, comment, title), score in tmp_ans.items():
            if pos == 'url':
                ans[(comment, title)] = max_comment_score * result['score']
            else:
                ans[(comment, title)] = score

    return sorted([(v, k) for k, v in ans.items()], reverse=True)


def avg_ccjieba_pmi(query, results, pairs_cnt):
    ans = {}
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

        comment_scores = []
        tmp_ans = {}
        for comment in result['comments']:
            comment_ccjieba = comment['comment_ccjieba'].split(" ")
            comment_score = 0
            cnter = 0
            for query_term, comment_term in it.product(query, comment_ccjieba):
                key = query_term + ':' + comment_term
                if key in pmi_dict:
                    comment_score += pmi_dict[key]
                cnter += 1
            comment_score /= cnter
            comment_scores.append(comment_score)
            tmp_ans[(comment['comment_pos'], comment['comment_origin'], result['title_origin'])] = comment_score * result['score']

        max_comment_score = max(comment_scores)
        for (pos, comment, title), score in tmp_ans.items():
            if pos == 'url':
                ans[(comment, title)] = max_comment_score * result['score']
            else:
                ans[(comment, title)] = score

    return sorted([(v, k) for k, v in ans.items()], reverse=True)
