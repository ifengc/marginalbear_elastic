from elasticsearch_dsl import Search


def post_search(client, field, query):
    if field == 'title_ccjieba':
        hits = Search(using=client).query("match", title_ccjieba=query)
        return _combine_termvecs(client, hits, field)
    elif field == 'title_unigram':
        hits = Search(using=client).query("match", title_unigram=query)
        return _combine_termvecs(client, hits, field)
    else:
        return None


def _combine_termvecs(client, hits, field):
    if len(list(hits)) == 0:
        return None
    else:
        ids = []
        for hit in hits:
            ids.append(hit.meta.id)

        mtermvecs = client.mtermvectors(index="post",
                                        doc_type="doc",
                                        term_statistics=True,
                                        fields=[field],
                                        ids=ids)['docs']

        result = []
        for hit, termvecs in zip(hits, mtermvecs):
            hit = hit.to_dict()
            hit['term_vectors'] = termvecs
            result.append(hit)

        return result
