import pika
import os,sys
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
import json

# load_dotenv(find_dotenv())

# RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')


#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection

def SCRAPE_publish_to_queue(msg):
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()

        channel.queue_declare(queue='scrape_queue',durable=True)

        print(msg)
        # publish product to quueue
        channel.basic_publish(exchange='', 
                                routing_key='scrape_queue', 
                                body=json.dumps(msg),
                                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
        print(chalk.green(f"Successfully sent task to queue: {msg} \n -------------"))


    except Exception as e:
        print(chalk.red(f"Error publishing message to queue: {e}"))

    finally:
        # Close the connection to RabbitMQ
        try:
            connection.close()
        except Exception as e:
            print(chalk.red(f"Error closing RabbitMQ connection: {e}"))

if __name__ == "__main__":
    urls_to_scrape = ['https://example.com', 'https://anotherexample.com']
    

    SCRAPE_publish_to_queue()