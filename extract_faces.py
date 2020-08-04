import os
from argparse import ArgumentParser
from os.path import join, splitdrive
import pathlib
import numpy as np
import pika
import sys
import base64
import face_recognition
from pika import PlainCredentials
from elastic import Face
from elasticsearch_dsl import connections
from PIL import Image


def extract_faces(file_name, image, output_folder, named):
    image_np = np.array(image)
    locations = face_recognition.face_locations(image_np)
    encodings = face_recognition.face_encodings(image_np, locations)
    face_count = 0
    for location, features in zip(locations, encodings):
        face = Face()
        face.file_name = file_name
        face.features = base64.b64encode(features).decode()
        face.position.top = location[0]
        face.position.right = location[1]
        face.position.bottom = location[2]
        face.position.left = location[3]
        if named:
            face.person = os.path.splitext(os.path.basename(file_name))[0]
        face.save()
        face_count += 1

        if output_folder:
            face_image = image_np[location[0]:location[2], location[3]:location[1]]
            pil_image = Image.fromarray(face_image)
            pil_image.save(join(output_folder, face.meta.id + '.jpg'), "JPEG", quality=90, optimize=True,
                           progressive=True)
    return face_count


def make_thumbnail(file_name, image, output_folder):
    target_width = 805
    width, height = image.size
    target_height = int(height * (1.0 * target_width / width))
    thumb = image.resize((target_width, target_height), Image.ANTIALIAS)

    thumbnail_name = join(output_folder, splitdrive(file_name)[1].strip("\\"))
    pathlib.Path(os.path.dirname(thumbnail_name)).mkdir(parents=True, exist_ok=True)
    thumb.save(thumbnail_name, "JPEG", quality=90, optimize=True, progressive=True)


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
    Face.init()

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
            image = Image.open(file_name)
            faces_extracted = extract_faces(file_name, image, args.folder, properties.headers['named'] == 'True')
            if faces_extracted > 0 and args.thumbs:
                make_thumbnail(file_name, image, args.thumbs)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            print('{0}: {1}'.format(file_name, sys.exc_info()))
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue='img_queue')
    channel.start_consuming()
