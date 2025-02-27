import pika
import os,sys
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
import json

# load_dotenv(find_dotenv())

# RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')

def add_to_queue(url):
    # Set up the connection parameters (use correct RabbitMQ host and credentials)
    connection_params = pika.ConnectionParameters(
        host='localhost',  # Ensure this is the correct host, change if necessary
        port=5672,          # Default RabbitMQ port
        credentials=pika.PlainCredentials('guest', 'guest')
    )

    # Establish the connection using the connection_params object directly
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    
    channel.queue_declare(queue='scrape_tasks',durable=True)

    # Send the URL to the queue
    channel.basic_publish(exchange='', 
                          routing_key='scrape_tasks', 
                          body=url,
                           properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
    print(f"Sent task: {url}")
    connection.close()

if __name__ == "__main__":
    urls_to_scrape = ['https://example.com', 'https://anotherexample.com']
    for url in urls_to_scrape:
       add_to_queue(url)