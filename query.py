from elasticsearch_dsl import Search


def post_search(client, field, query):
    if field == 'title_ccjieba':
        s = Search(using=client).query("match", title_ccjieba=query)
        hits = _search_scan(s)
        print('{} docs hit'.format(len(hits)))
        return _combine_termvecs(client, hits, field)
    elif field == 'title_unigram':
        s = Search(using=client).query("match", title_unigram=query)
        hits = _search_scan(s)
        print('{} docs hit'.format(len(hits)))
        return _combine_termvecs(client, hits, field)
    else:
        return None


def _combine_termvecs(client, hits, field):
    if len(list(hits)) == 0:
        return None
    else:
        ids = []
        mtermvecs = []
        for hit in hits:
            ids.append(hit.meta.id)

        for i in range(100, len(ids) + 100, 100):
            slice_ids = ids[i - 100:i]
            m = client.mtermvectors(index="post",
                                    doc_type="doc",
                                    term_statistics=True,
                                    offsets=False,
                                    payloads=False,
                                    positions=False,
                                    fields=[field],
                                    ids=slice_ids)['docs']
            mtermvecs += m

        result = []
        for hit, termvecs in zip(hits, mtermvecs):
            hit = hit.to_dict()
            hit['term_vectors'] = termvecs
            result.append(hit)

        return result


def _search_scan(s):
    hits = []
    for hit in s.scan():
        hits.append(hit)
    return hits
