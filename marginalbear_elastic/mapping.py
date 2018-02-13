from elasticsearch_dsl import DocType, Text, Date, Keyword, Nested, HalfFloat
from elasticsearch_dsl import analyzer


class Post(DocType):

    date = Date()
    url = Keyword()
    author = Keyword()
    topic = Keyword()
    board = Keyword()

    title_origin = Keyword()
    title_unigram = Text(analyzer=analyzer('whitespace'))
    title_ccjieba = Text(analyzer=analyzer('whitespace'))
    title_pos = Text(analyzer=analyzer('whitespace'))
    title_quality = HalfFloat()
    comments = Nested(properties={'comment_author': Keyword(),
                                  'comment_origin': Keyword(),
                                  'comment_unigram': Text(analyzer=analyzer('whitespace')),
                                  'comment_ccjieba': Text(analyzer=analyzer('whitespace')),
                                  'comment_pos': Text(analyzer=analyzer('whitespace')),
                                  'comment_audio_url': Keyword(),
                                  'comment_quality': HalfFloat()})

    class Meta:
        index = 'post'

    def save(self, *args, **kwargs):
        return super(Post, self).save(*args, **kwargs)

    def add_comment(self,
                    comment_author,
                    comment_origin,
                    comment_unigram,
                    comment_ccjieba,
                    comment_pos,
                    comment_audio_url,
                    comment_quality):

        self.comments.append({'comment_author': comment_author,
                              'comment_origin': comment_origin,
                              'comment_unigram': comment_unigram,
                              'comment_ccjieba': comment_ccjieba,
                              'comment_pos': comment_pos,
                              'comment_audio_url': comment_audio_url,
                              'comment_quality': comment_quality})

    def bulk_dicts(docs):
        dicts = (d.to_dict(include_meta=True) for d in docs)
        return dicts
