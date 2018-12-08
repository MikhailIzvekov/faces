from elasticsearch_dsl import DocType, InnerDoc, Integer, Text, Field, Object, Keyword, \
    FacetedSearch, TermsFacet, connections, Q, SF
from elasticsearch_dsl.query import FunctionScore
import time

class Binary(Field):
    name = 'binary'


class Position(InnerDoc):
    top = Integer()
    left = Integer()
    bottom = Integer()
    right = Integer()


class Face(DocType):
    file_name = Text(
        fields={'raw': Keyword()})
    features = Binary()

    position = Object(Position)
    person = Text(
        fields={'raw': Keyword()}
    )

    class Index:
        name = 'faces'


class Photo(DocType):
    file_name = Text(
        fielddata=True,
        fields={'raw': Keyword()}
    )

    persons = Text(
        fields={'raw': Keyword()}
    )

    person_count = Integer()

    class Index:
        name = 'photos'

    def __init__(self, file_name=None, persons=None, meta=None):
        super(Photo, self).__init__(meta)
        self.file_name = file_name
        self.persons = persons
        if persons:
            self.person_count = len(persons)


class Cluster(DocType):
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
