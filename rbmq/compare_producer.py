import pika
import os,sys
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
import json


def publish_to_compare_queue(msg):

    try:
        # Set up the connection parameters (use correct RabbitMQ host and credentials)
        connection_params = pika.ConnectionParameters(
            host='localhost',  # Ensure this is the correct host, change if necessary
            port=5672,          # Default RabbitMQ port
            credentials=pika.PlainCredentials('guest', 'guest')
        )


        # Establish the connection using the connection_params object directly
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        channel.queue_declare(queue='compare_queue',durable=True)

        print(msg)
        # publish product to quueue
        channel.basic_publish(exchange='', 
                                routing_key='compare_queue', 
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
    

    publish_to_compare_queue()