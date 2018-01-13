from elasticsearch_dsl import Search


def post_search(client, tokenizer, query, top=100):
    if tokenizer == 'ccjieba':
        s = Search(using=client).query("match", title_ccjieba=query).params(preserve_order=True)
        hits = _search_scan(s, top)
        print('{} docs retrieved'.format(len(hits)))
        return _combine_termvecs(client, hits, tokenizer)
    elif tokenizer == 'unigram':
        s = Search(using=client).query("match", title_unigram=query).params(preserve_order=True)
        hits = _search_scan(s, top)
        print('{} docs retrieved'.format(len(hits)))
        return _combine_termvecs(client, hits, tokenizer)
    else:
        return None


def _combine_termvecs(client, hits, tokenizer):
    if len(list(hits)) == 0:
        return None
    else:
        fields = ['title_' + tokenizer, 'comments.content_' + tokenizer]
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
                                    fields=fields,
                                    ids=slice_ids)['docs']
            mtermvecs += m

        result = []
        for hit, termvecs in zip(hits, mtermvecs):
            hit = hit.to_dict()
            hit['term_vectors'] = termvecs['term_vectors']
            result.append(hit)

        return result


def _search_scan(s, top):
    hits = []
    for i, hit in enumerate(s.scan()):
        if i == top:
            break
        hits.append(hit)
    return hits
