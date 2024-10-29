
import sys,csv,json,os
import pika
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree  # For removing directories



parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from rbmq.scrape_producer import publish_to_scrape_queue

"""

recieves brand,category,output_dir,query_hash,True from main_app_driver

spins up each scraper for each website. either sequentially or parallel

writes output from each scrape to each shared volume

write message to log for each scrape process if completed or failed

write message to queue when all scrapes for query done - ready for comparison now

"""


def main():

    recd_products = []  # Reset received products for each callback
    no_change_sources = []  # Track sources with no price changes



    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            print(f"Message received: {msg}")
            publish_to_scrape_queue({'type':'SCRAPE_COMPLETE'})
        except Exception as e:
            pass




    # RabbitMQ setup
    try:
        connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        channel.queue_declare(queue='scrape_queue', durable=True)

        # Start consuming messages
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='scrape_queue', on_message_callback=callback)
        

        print(chalk.blue('[*] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()

    except Exception as e:
        print(chalk.red(f"Error during RabbitMQ setup or message consumption: {e}"))
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()
