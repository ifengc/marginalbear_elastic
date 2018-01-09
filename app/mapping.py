from elasticsearch_dsl import DocType, Text, Date, Keyword, Nested
from elasticsearch_dsl import analyzer


class Post(DocType):

    date = Date()
    url = Keyword()
    author = Keyword()
    topic = Keyword()

    title_origin = Keyword()
    title_unigram = Text(analyzer=analyzer('whitespace'))
    title_ccjieba = Text(analyzer=analyzer('whitespace'))
    title_pos = Text(analyzer=analyzer('whitespace'))
    comments = Nested(properties={'comment_author': Keyword(),
                                  'content_origin': Keyword(),
                                  'content_unigram': Text(analyzer=analyzer('whitespace')),
                                  'content_ccjieba': Text(analyzer=analyzer('whitespace')),
                                  'content_pos': Text(analyzer=analyzer('whitespace'))})

    class Meta:
        index = 'post'

    def save(self, *args, **kwargs):
        return super(Post, self).save(*args, **kwargs)

    def add_comment(self, author, content_origin, content_unigram, content_ccjieba, content_pos):
        self.comments.append({'comment_author': author,
                              'content_origin': content_origin,
                              'content_unigram': content_unigram,
                              'content_ccjieba': content_ccjieba,
                              'content_pos': content_pos})

    def bulk_dicts(docs):
        dicts = (d.to_dict(include_meta=True) for d in docs)
        return dicts