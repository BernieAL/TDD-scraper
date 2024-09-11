import time
import pika
import os,sys,json
from simple_chalk import chalk

# from dotenv import load_dotenv,find_dotenv
# load_dotenv(find_dotenv())

from bot_selenium_scraper import perform_url_scrape

#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)



def main():

    # Set up the connection parameters (use correct RabbitMQ host and credentials)
    connection_params = pika.ConnectionParameters(
        host='rbmq',  # Ensure this is the correct host, change if necessary
        port=5672,          # Default RabbitMQ port
        credentials=pika.PlainCredentials('guest', 'guest')
    )

    # Establish the connection using the connection_params object directly
    connection = pika.BlockingConnection(connection_params)
    
    channel = connection.channel()
    channel.queue_declare(queue='scrape_tasks',durable=True) 
    
    def callback(ch,method,properties,body):

        url = body.decode('utf-8')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        print(f"Message received: {url}")

        res = perform_url_scrape(url)
        if res == True:
            print("scrape successful")
        if res == False:
            print("scrape unsuccessful")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='scrape_tasks',on_message_callback=callback)


      #listen indefinitley for messages
    print(chalk.blue('[*] waiting for messages. To exit press CTRL+C'))
    channel.start_consuming()

if __name__ == "__main__":
    main()