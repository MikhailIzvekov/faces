import os
import base64
from typing import List, Any

import numpy as np
from elasticsearch.helpers import bulk
from clustering.clustering import cluster
from jinja2 import Template
from elastic import Face, Photo, Cluster
from elasticsearch import Elasticsearch, exceptions
from elasticsearch_dsl import connections
from collections import defaultdict
from argparse import ArgumentParser


def approximate_rank_order_clustering(faces):
    vectors = np.array([np.frombuffer(base64.b64decode(o.features.encode()), dtype=np.float64) for o in faces])
    clusters = cluster(vectors, n_neighbors=200, thresh=[0.65])
    result = []
    for ids in clusters[0]['clusters']:
        cluster_faces = [faces[i] for i in ids]
        person = next((x.person for x in cluster_faces if x.person), None)
        cluster_faces_ids = [f.meta.id for f in cluster_faces]
        result.append(Cluster(cluster_faces_ids, person))
    return result


def generate_clusters_index(clusters):
    Cluster._index.delete(ignore=404)
    Cluster.init()
    bulk(Elasticsearch(), (c.to_dict(True) for c in clusters))


def generate_images_index():
    Photo._index.delete(ignore=404)
    Photo.init()
    results = Face.search().filter("exists", field="person").scan()

    groups = defaultdict(list)

    for face in results:
        groups[face.file_name].append(face.person)

    bulk(Elasticsearch(), (Photo(group[0], group[1]).to_dict(True) for group in groups.items()))


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


def render_clusters_to_html(faces):
    template_filename = r".\templates\clusters_local.html"
    output_filename = r"result.html"

    clst = approximate_rank_order_clustering(faces)

    with open(template_filename, 'r') as template_file:
        template = Template(template_file.read())

    clusters = [x for x in clst if len(x) > 3]
    clusters.sort(key=len, reverse=True)
    html = template.render(clusters=clusters)

    with open(output_filename, 'w') as file:
        file.write(html)
    os.startfile(output_filename, 'open')


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-es", "--elastic", dest="elastic",
                        help="Elasticsearch address, default is localhost", metavar="ADDRESS", default='localhost')
    args = parser.parse_args()
    connections.create_connection(hosts=[args.elastic])

    # faces = [face for face in Face.search().scan()]
    # clst = approximate_rank_order_clustering(faces)
    # generate_clusters_index(clst)

    # clusters_with_person = Cluster.search().filter("exists", field="person").execute()
    # for cls in clusters_with_person:
    #     cls.update_faces_index()


    # render_clusters_to_html(faces[0:300])

    # do_clusters(features, ids, names)
    # generate_images_index()
