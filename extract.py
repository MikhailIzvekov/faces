import sys
from argparse import ArgumentParser

import pika
from PIL import Image
from elasticsearch_dsl import connections
from pika import PlainCredentials

from elastic import Face, Photo, DetectedObject
from extract_faces import extract_faces
from extract_objects import extract_objects
from make_thumbnails import make_thumbnail

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-es", "--elastic", dest="elastic",
                        help="Elasticsearch address, default is localhost", metavar="ADDRESS", default='localhost')
    parser.add_argument("-host", "--host", dest="host",
                        help="RabbitMQ host, default is localhost", metavar="HOST", default='localhost')
    parser.add_argument("-f", "--folder", dest="folder",
                        help="Folder for extracted faces, no extraction if unspecified", metavar="FOLDER")
    parser.add_argument("-t", "--thumbs", dest="thumbs",
                        help="Folder for thumbnails, no thumbnails if unspecified", metavar="THUMBS")
    parser.add_argument("-u", "--user", dest="user",
                        help="RabbitMQ user name", metavar="USER", default='guest')
    parser.add_argument("-p", "--password", dest="password",
                        help="RabbitMQ user password", metavar="PASSWORD", default='guest')

    args = parser.parse_args()

    connections.create_connection(hosts=[args.elastic])
    # Face.init()
    # DetectedObject.init()

    print(' [*] Waiting for messages. To exit press CTRL+BREAK')

    credentials = PlainCredentials(args.user, args.password)
    params = pika.ConnectionParameters(host=args.host, credentials=credentials,
                                       heartbeat=600, blocked_connection_timeout=300)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='img_queue', durable=True)

    def callback(ch, method, properties, body):
        file_name = body.decode()
        try:
            s = Photo.search().filter('term', file_name__raw=file_name)[0:1]
            # TODO сделать проверку на несколько результатов.
            photo = next(iter(s.execute()), None) or Photo(file_name=file_name)

            image = Image.open(file_name)
            # photo.person_count = extract_faces(file_name, image, args.folder, properties.headers['named'] == 'True')

            photo.objects = extract_objects(file_name)

            if args.thumbs:
                photo.thumbnails = make_thumbnail(file_name, image, args.thumbs)

            photo.save()
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            print('{0}: {1}'.format(file_name, sys.exc_info()))
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue='img_queue')
    channel.start_consuming()
