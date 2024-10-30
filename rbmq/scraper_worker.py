
import sys,csv,json,os
import pika
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree  # For removing directories



parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from rbmq.scrape_producer import publish_to_scrape_queue
# Import the ScraperUtils class from utils
from selenium_scraper_container.utils.ScraperUtils import ScraperUtils

# Import the scraper classes to construct instances of 
from selenium_scraper_container.scrapers.italist_scraper import ItalistScraper


"""

recieves brand,category,output_dir,query_hash,True from main_app_driver

spins up each scraper for each website. either sequentially or parallel

writes output from each scrape to each shared volume

write message to log for each scrape process if completed or failed

write message to queue when all scrapes for query done - ready for comparison now

returns names of created file paths in subdirs where scraped data written to in shared volume

"""

def run_italist_scraper(brand,category,output_dir,query_hash):
    italist_scraper = ItalistScraper(brand,category,output_dir,query_hash,True) #if True, use local site copy
    scraped_file = italist_scraper.run()
    print(chalk.green(f"(scraper_worker{scraped_file}"))



def main():

    recd_products = []  # Reset received products for each callback
    no_change_sources = []  # Track sources with no price changes



    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            print(chalk.yellow(f"Message received: {msg}"))
            
            query_hash = msg['query_hash']
            brand = msg['brand']
            category = msg['category']
            output_dir = msg['output_dir']

            
            run_italist_scraper(brand,category,output_dir,query_hash)

            


            publish_to_scrape_queue({'type':'SCRAPE_COMPLETE','query_hash':query_hash})

        except Exception as e:
            print(chalk.red(f"Error processing message: {e}"))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

       
        


    # RabbitMQ setup
    try:
        connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        channel.queue_declare(queue='scrape_queue', durable=True)

        # Start consuming messages
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='scrape_queue', on_message_callback=callback)
        
        print(chalk.green("Clearing queue"))
        channel.queue_purge(queue='scrape_queue')

        print(chalk.blue('[*] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()

    except Exception as e:
        print(chalk.red(f"Error during RabbitMQ setup or message consumption: {e}"))
    finally:
        if connection and connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
