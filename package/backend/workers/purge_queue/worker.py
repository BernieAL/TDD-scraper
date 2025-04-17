import pika,os,sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection


connection = create_rabbitmq_connection()
channel = connection.channel()

# Purge the queue
channel.queue_purge(queue='scrape_queue')

print("Queue cleared.")

# Close the connection
connection.close()

