# -*- coding: utf-8 -*-
import os
from elasticsearch_dsl.query import FunctionScore
from flask import Flask, render_template, request, Response, redirect
from elastic import PhotoSearch, Cluster, Face
from elasticsearch_dsl import connections, Search, A, Q, SF
from argparse import ArgumentParser
import json

app = Flask(__name__, static_url_path='')


@app.errorhandler(500)
def internal_server_error(e):
    return str(e), 500


@app.route('/clusters', methods=['GET', ])
def clusters():
    """
    Отображает AJAX-версию страницы с кластеризацией. Предназначено для замены
    display_clusters() после тестирования.
    """
    person = request.values.get('filter')
    print(person)
    Face._index.refresh()

    total = Face.search().count()
    named = Face.search().filter("exists", field="person").count()
    status = "{:.1%} ({} out of {}) faces are named. Clusters count: {}".format(
        named / total, named, total, Cluster.search().count())

    a = A("terms", field="person.raw", size=10000)
    ps = Search()
    ps.aggs.bucket("persons", a)
    psr = ps.execute()

    persons = [b.key for b in psr.aggs['persons']]

    if person:
        s = Cluster.search().filter("prefix", person=person).sort("-face_count")
        results = s[0:10000].execute()
    else:
        s = Cluster.search().exclude("exists", field="person")
        s.query = FunctionScore(query=s.query,
                                functions=[SF('random_score', weight=100),
                                           SF('field_value_factor',
                                              field="face_count", weight=1)],
                                score_mode="avg", boost_mode="replace")
        results = s[0:50].execute()

    return render_template('clusters.html', clusters=results, persons=persons,
                           status=status)


@app.route('/cluster_api', methods=["POST", ])
def clusters_api():
    """
    Обрабатывает AJAX-запрос от страницы. Возвращает JSON-ответ с результатом
    выполнения запроса.
    """
    action = request.values.get('action')
    if action in ('save', 'ignore'):
        cluster_id = request.values.get('cluster')
        cluster = Cluster.get(id=cluster_id)
        if action == 'save':
            cluster.person = request.values.get('person', None)
            if cluster.person == '':
                cluster.person = None
        else:
            cluster.person = "ignored, cluster_id: " + cluster.meta.id
        cluster.save(refresh=True)
        cluster.update_faces_index()
        result = { 'result': 'ok' }
    else:
        result = {'result': 'error'}

    return Response(response=json.dumps(result), status=200,
                    mimetype='application/json')


@app.route('/', methods=["GET", "POST"])
def display_main():
    return redirect("/index.html", code=301)


@app.route('/_search', methods=["POST", ])
def search_api():
    q = request.values.get('q')
    page = int(request.values.get('page', 1))
    pc = {"persons": request.values.getlist("person[]"),
          "tags": request.values.getlist("tag[]"),
          "person_count": [int(x) for x in request.values.getlist("person_count[]")]}
    s = PhotoSearch(query=q, filters=pc)
    results = s[(page - 1)*50:page*50].execute()
    result = {
        "count": results.hits.total,
        "hits": [{ 'path': r"\thumbnails" + os.path.splitdrive(photo.file_name)[1], 'persons': photo.person_count } for photo in results],
        "facets": {
            "person": list(results.facets.persons),
            "tag": list(results.facets.tags),
            "person_count": list(results.facets.person_count)
        }
    }
    return Response(response=json.dumps(result), status=200, mimetype='application/json')


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-es", "--elastic", dest="elastic",
                        help="Elasticsearch address, default is localhost", metavar="ADDRESS", default='localhost')
    args = parser.parse_args()

    connections.create_connection(hosts=[args.elastic])
    app.run(debug=False, host='0.0.0.0')
