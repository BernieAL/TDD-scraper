import pika
import os,sys
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
import json


from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection


def COMPARE_publish_to_queue(msg):

    try:
      
        connection = create_rabbitmq_connection()
        channel = connection.channel()

        channel.queue_declare(queue='compare_queue',durable=True)

        # publish product to quueue
        channel.basic_publish(exchange='', 
                                routing_key='compare_queue', 
                                body=json.dumps(msg),
                                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
        print(chalk.green(f"Successfully sent task to COMPARE QUEUE: {msg} \n -------------"))

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
    

    COMPARE_publish_to_queue()