import os
from argparse import ArgumentParser
from os.path import join
import pika
import sys
import base64
import face_recognition
from pika import PlainCredentials

from elastic import Face
from elasticsearch_dsl import connections
from PIL import Image


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-es", "--elastic", dest="elastic",
                        help="Elasticsearch address, default is localhost", metavar="ADDRESS", default='localhost')
    parser.add_argument("-host", "--host", dest="host",
                        help="RabbitMQ host, default is localhost", metavar="HOST", default='localhost')
    parser.add_argument("-f", "--folder", dest="folder",
                        help="Folder for extracted faces, no extraction if unspecified", metavar="FOLDER")
    parser.add_argument("-u", "--user", dest="user",
                        help="RabbitMQ user name", metavar="USER", default='guest')
    parser.add_argument("-p", "--password", dest="password",
                        help="RabbitMQ user password", metavar="PASSWORD", default='guest')

    args = parser.parse_args()

    connections.create_connection(hosts=[args.elastic])
    Face.init()

    print(' [*] Waiting for messages. To exit press CTRL+BREAK')

    credentials = PlainCredentials(args.user, args.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.host, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='img_queue', durable=True)

    def callback(ch, method, properties, body):
        try:
            file_name = body.decode()
            image = face_recognition.load_image_file(file_name)
            locations = face_recognition.face_locations(image)
            encodings = face_recognition.face_encodings(image, locations)
            for location, features in zip(locations, encodings):
                face = Face()
                face.file_name = file_name
                face.features = base64.b64encode(features).decode()
                face.position.top = location[0]
                face.position.right = location[1]
                face.position.bottom = location[2]
                face.position.left = location[3]
                if properties.headers['named'] == 'True':
                    face.person = os.path.splitext(os.path.basename(file_name))[0]
                face.save()

                if args.folder:
                    face_image = image[location[0]:location[2], location[3]:location[1]]
                    pil_image = Image.fromarray(face_image)
                    pil_image.save(join(args.folder, face.meta.id + '.jpg'))

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            print('{0}: {1}'.format(file_name, sys.exc_info()))
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue='img_queue')
    channel.start_consuming()
