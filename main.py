import os
import base64
import numpy as np
from clustering import cluster
from jinja2 import Template
from elastic import Face, Photo
from elasticsearch import Elasticsearch, exceptions
from elasticsearch_dsl import connections
from collections import defaultdict
from argparse import ArgumentParser


def approximate_rank_order_clustering(vectors):
    clusters = cluster(vectors, n_neighbors=200, thresh=[0.65])
    return clusters


def generate_images_index():
    connections.create_connection(hosts=[args.elastic])
    Photo.init()
    results = Face.search().filter("exists", field="person").scan()

    groups = defaultdict(list)

    for face in results:
        groups[face.file_name].append(face.person)

    for group in groups.items():
        photo = Photo()
        photo.file_name = group[0]
        photo.persons = group[1]
        photo.person_count = len(group[1])
        photo.save()


def update_cluster(cluster, ids, name):
    lids = []
    for index in cluster:
        lids.append(ids[index])

    q = {
        "script": {
            "inline": "ctx._source.person=params.person",
            "lang": "painless",
            "params": {
                "person": name
            }
        },
        "query": {
            "terms": {
                "_id": lids
            }
        }
    }
    es = Elasticsearch()
    es.update_by_query(body=q, doc_type='doc', index='faces', conflicts='proceed')


def do_clusters(features, ids, names):
    clusters = approximate_rank_order_clustering(features)[0]['clusters']
    for cluster in clusters:
        for index in cluster:
            if names[index]:
                update_cluster(cluster, ids, names[index])


def load_faces_data():
    connections.create_connection(hosts=['localhost'])
    results = Face.search().scan()
    ids = []
    features = []
    names = []
    for face in results:
        ids.append(face._id)
        features.append(np.frombuffer(base64.b64decode(face.features.encode()), dtype=np.float64))
        names.append(face.person)
    senc = np.array(features)
    return senc, ids, names


def render_clusters_to_html(features, ids):
    template_filename = r".\templates\clusters.html"
    output_filename = r"result.html"

    clst = approximate_rank_order_clustering(features)

    with open(template_filename, 'r') as template_file:
        template = Template(template_file.read())

    clusters = [x for x in clst[0]['clusters'] if len(x) > 3]
    clusters.sort(key=len, reverse=True)
    html = template.render(clusters=clusters, labels=ids)

    with open(output_filename, 'w') as file:
        file.write(html)
    os.startfile(output_filename, 'open')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-es", "--elastic", dest="elastic",
                        help="Elasticsearch address, default is localhost", metavar="ADDRESS", default='localhost')
    args = parser.parse_args()

    # features, ids, names = load_faces_data()
    # render_clusters_to_html(features, ids)
    # do_clusters(features, ids, names)
    # generate_images_index()
