import base64
import numpy as np
from elasticsearch.helpers import bulk
from clustering.clustering import cluster
from elastic import Face, Photo, Cluster
from elasticsearch import Elasticsearch
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
    bulk(Elasticsearch(args.elastic), (c.to_dict(True) for c in clusters))
    Cluster._index.refresh()


def generate_images_index():
    Photo._index.delete(ignore=404)
    Photo.init()
    results = Face.search().filter("exists", field="person").scan()

    groups = defaultdict(list)

    for face in results:
        groups[face.file_name].append(face.person)

    bulk(Elasticsearch(args.elastic), (Photo(group[0], group[1]).to_dict(True) for group in groups.items()))


def print_faces_stats():
    Face._index.refresh()
    total = Face.search().count()
    named = Face.search().filter("exists", field="person").count()
    print("{:.1%} ({} out of {}) faces are named.".format(named / total, named, total))


def shuffle():
    print_faces_stats()
    faces = [face for face in Face.search().scan()]
    clst = approximate_rank_order_clustering(faces)
    generate_clusters_index(clst)

    clusters_with_person = Cluster.search().filter("exists", field="person").scan()
    for cls in clusters_with_person:
        cls.update_faces_index()
    print_faces_stats()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-es", "--elastic", dest="elastic",
                        help="Elasticsearch address, default is localhost", metavar="ADDRESS", default='localhost')
    args = parser.parse_args()
    connections.create_connection(hosts=[args.elastic])

    shuffle()

    # faces = [face for face in Face.search().scan()]
    # clst = approximate_rank_order_clustering(faces)
    # generate_clusters_index(clst)


    # generate_images_index()
