import pika
import os

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST','localhost')

def get_rabbitmq_connection_params():
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=5672,
        credentials=pika.PlainCredentials('guest','guest'),
        heartbeat=600
    )

def create_rabbitmq_connection():
    #creates new rbmq connnection using std params
    return pika.BlockingConnection(get_rabbitmq_connection_params())