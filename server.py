import os
from flask import Flask, render_template, request
from elastic import PhotoSearch
from elasticsearch_dsl import connections
from argparse import ArgumentParser

app = Flask(__name__, static_url_path='')


@app.errorhandler(500)
def internal_server_error(e):
    return str(e), 500


@app.route('/', methods=["GET", "POST"])
def display_clusters():
    q = request.values.get('q')
    pc = {"persons": request.values.getlist("person"),
          "person_count": [int(x) for x in request.values.getlist("count")]}

    s = PhotoSearch(query=q, filters=pc)

    print(s._s.to_dict())
    results = s[0:100].execute()
    images = []
    for photo in results:
        images.append(os.path.splitdrive(photo.file_name)[1])
    persons = results.facets.persons
    counts = results.facets.person_count

    return render_template('main.html', images=images, q=q,
                           persons=persons, counts=counts, total_count=results.hits.total)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-es", "--elastic", dest="elastic",
                        help="Elasticsearch address, default is localhost", metavar="ADDRESS", default='localhost')
    args = parser.parse_args()

    connections.create_connection(hosts=[args.elastic])
    app.run(debug=False)
