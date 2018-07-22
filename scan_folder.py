import os
from os.path import join
from argparse import ArgumentParser
import pika
from pika import PlainCredentials


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-f", "--folder", dest="folder",
                        help="folder to add in scan queue", metavar="FOLDER")
    parser.add_argument("-host", "--host", dest="host",
                        help="RabbitMQ host, default is localhost", metavar="HOST", default='localhost')

    parser.add_argument("-n", "--named", dest="named",
                        help="Indicate that images will be used to name clusters", metavar="NAMED", default=False)

    parser.add_argument("-u", "--user", dest="user",
                        help="RabbitMQ user name", metavar="USER", default='guest')
    parser.add_argument("-p", "--password", dest="password",
                        help="RabbitMQ user password", metavar="PASSWORD", default='guest')

    args = parser.parse_args()
    if args.folder:

        credentials = PlainCredentials(args.user, args.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.host, credentials=credentials))
        channel = connection.channel()

        channel.queue_declare(queue='img_queue', durable=True)

        for dirpath, dirs, files in os.walk(args.folder):
            for f in files:
                if f.lower().endswith(('.jpg', '.jpeg')):
                    channel.basic_publish(exchange='',
                                          routing_key='img_queue',
                                          body=join(dirpath, f),
                                          properties=pika.BasicProperties(
                                              delivery_mode=2,  # make message persistent
                                              headers={'named': args.named}
                                          ))
        connection.close()
