from elasticsearch_dsl import Document, InnerDoc, Integer, Text, Field, Object, Keyword, \
    FacetedSearch, TermsFacet, connections, Q, SF, tokenizer, analyzer, token_filter
from elasticsearch_dsl.query import FunctionScore
import time

path_tokenizer = tokenizer('path_tokenizer', type='path_hierarchy', delimiter='\\')
path_analyzer = analyzer('path_analyzer', tokenizer=path_tokenizer)

word_delimiter = token_filter('word_delimiter', type='word_delimiter', catenate_all=True, preserve_original=True)
text_analyzer = analyzer('text_analyzer', filter=[word_delimiter, "lowercase"], tokenizer="standard")


class Binary(Field):
    name = 'binary'


class PositionTLBR(InnerDoc):
    top = Integer()
    left = Integer()
    bottom = Integer()
    right = Integer()


class PositionXYWH(InnerDoc):
    x = float()
    y = float()
    w = float()
    h = float()


class Face(Document):
    file_name = Text(
        analyzer=text_analyzer,
        fields={
            'raw': Keyword(),
            'path': Text(analyzer=path_analyzer)
        })
    features = Binary()

    position = Object(PositionTLBR)
    person = Text(
        fields={'raw': Keyword()}
    )

    class Index:
        name = 'faces'


class DetectedObject(Document):
    file_name = Text(
        analyzer=text_analyzer,
        fields={
            'raw': Keyword(),
            'path': Text(analyzer=path_analyzer)
        })

    position = Object(PositionXYWH)
    type = Keyword()

    class Index:
        name = 'detected_objects'


class Photo(Document):
    file_name = Text(
        analyzer=text_analyzer,
        fielddata=True,
        fields={
            'raw': Keyword(),
            'path': Text(analyzer=path_analyzer)
        }
    )

    persons = Text(
        fields={'raw': Keyword()}
    )

    person_count = Integer()

    thumbnails = Keyword()

    objects = Keyword()

    class Index:
        name = 'photos'

    def __init__(self, file_name=None, persons=None, meta=None):
        super(Photo, self).__init__(meta)
        self.file_name = file_name
        self.persons = persons
        if persons:
            self.person_count = len(persons)


class Cluster(Document):
    faces = Keyword()
    face_count = Integer()
    person = Keyword()

    class Index:
        name = 'clusters'

    def __init__(self, faces=None, person=None, meta=None):
        super(Cluster, self).__init__(meta)
        if faces:
            self.faces = faces
            self.face_count = len(faces)
            if person:
                self.person = person

    def update_faces_index(self):
        q = {
            "script": {
                "inline": "ctx._source.person=params.person",
                "lang": "painless",
                "params": {
                    "person": self.person
                }
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "_id": self.faces
                            }
                        },
                        {
                            "bool": {
                                "must_not": [
                                    {
                                        "exists": {
                                            "field": "person"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        es = connections.get_connection()
        es.update_by_query(body=q, doc_type='doc', index='faces', conflicts='proceed')


class PhotoSearch(FacetedSearch):
    index = 'photos'
    doc_types = [Photo, ]
    fields = ['persons', 'file_name']

    facets = {
        'persons': TermsFacet(field='persons.raw', size=100),
        'person_count': TermsFacet(field='person_count', size=20),
        'tags': TermsFacet(field="file_name", size=50, exclude=[
          "agr's",
          "place",
          "d",
          "мои",
          "рисунки",
          "фотографии",
          "jpg",
          "raw",
          "и",
          "с",
          "c",
          "у",
          "для",
          "по",
          "из",
          "на",
          "в"
        ])
    }

    def query(self, search, query):
        if query:
            return search.query("simple_query_string", fields=self.fields, query=query, default_operator='and')
        else:
            search.query = FunctionScore(query=Q(), functions=[SF('random_score', seed=int(time.time()))])
            return search

    def highlight(self, search):
        return search
