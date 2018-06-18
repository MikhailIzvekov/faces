from elasticsearch_dsl import DocType, Integer, Text, Field, Object, Keyword, \
    analyzer, tokenizer, FacetedSearch, TermsFacet

path_tokenizer = tokenizer('path_tokenizer', type='path_hierarchy', delimiter='\\')
path_analyzer = analyzer('path_analyzer', tokenizer=path_tokenizer)


class Binary(Field):
    name = 'binary'


class Position(DocType):
    top = Integer()
    left = Integer()
    bottom = Integer()
    right = Integer()


class Face(DocType):
    file_name = Text(analyzer=path_analyzer,
                     fields={'raw': Keyword()})
    features = Binary()

    position = Object(Position)
    person = Text(
        fields={'raw': Keyword()}
    )

    class Meta:
        index = 'faces'


class Photo(DocType):
    file_name = Text(fields={'raw': Keyword()})

    persons = Text(
        fields={'raw': Keyword()}
    )

    person_count = Integer()

    class Meta:
        index = 'photos'


class PhotoSearch(FacetedSearch):
    index = 'photos'
    doc_types = [Photo, ]
    fields = ['persons', 'file_name']

    facets = {
        'persons': TermsFacet(field='persons.raw', size=200),
        'person_count': TermsFacet(field='person_count', size=200)
    }

    def query(self, search, query):
        if query:
            return search.query("simple_query_string", fields=self.fields, query=query, default_operator='and')
        return search

    def highlight(self, search):
        return search
