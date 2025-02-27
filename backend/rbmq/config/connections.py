import pika
import os

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')

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




if __name__ == "__main__":
    create_rabbitmq_connection()