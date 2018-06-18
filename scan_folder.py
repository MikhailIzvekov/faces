import os
from os.path import join
from argparse import ArgumentParser
import pika


def queue_file(filename, host='localhost'):
    """
    Add file to queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()

    channel.queue_declare(queue='img_queue', durable=True)

    channel.basic_publish(exchange='',
                          routing_key='img_queue',
                          body=filename,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                              headers={'named': args.named}
                          ))
    connection.close()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-f", "--folder", dest="folder",
                        help="folder to add in scan queue", metavar="FOLDER")
    parser.add_argument("-host", "--host", dest="host",
                        help="RabbitMQ host, default is localhost", metavar="HOST", default='localhost')

    parser.add_argument("-n", "--named", dest="named",
                        help="Indicate that images will be used to name clusters", metavar="NAMED", default=False)

    args = parser.parse_args()
    if args.folder:
        for dirpath, dirs, files in os.walk(args.folder):
            for f in files:
                if f.lower().endswith(('.jpg', '.jpeg')):
                    queue_file(join(dirpath, f), args.host)
