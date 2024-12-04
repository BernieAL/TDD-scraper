import sys,csv,json,os
import pika
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree  # For removing directories

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from rbmq.scrape_producer import SCRAPE_publish_to_queue
from selenium_scraper_container.utils.ScraperUtils import ScraperUtils
from selenium_scraper_container.scrapers.italist_scraper import ItalistScraper
from rbmq.process_producer import PROCESS_publish_to_queue
from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection


def run_italist_scraper(brand,category,output_dir,query_hash,local):
    print(chalk.blue(f"Starting Italist scraper with params:"))
    print(chalk.blue(f"Brand: {brand}"))
    print(chalk.blue(f"Category: {category}"))
    print(chalk.blue(f"Output Dir: {output_dir}"))
    print(chalk.blue(f"Query Hash: {query_hash}"))

   
    italist_scraper = ItalistScraper(brand,category,output_dir,query_hash,local)
    scraped_file = italist_scraper.run()
    print(chalk.green(f"Scraper completed. Output file: {scraped_file}"))
    return scraped_file

def main():
    def callback(ch, method, properties, body):
        #declaring early in case scraper fails
        scraped_file = None
        try:
            print(chalk.yellow("Received message on scrape_queue"))
            msg = json.loads(body)
            print(chalk.yellow(f"Message content: {msg}"))

            #use global shared paths provided in message 
            paths = msg.get('paths',{}) #empty dict if no 'paths'
            output_dir = msg['output_dir'] 

            
            
            # Extract and verify all required fields included in msg
            required_fields = ['query_hash', 'brand', 'category', 'output_dir']
            for field in required_fields:
                #if missing reqd field = fail and notify
                if field not in msg:

                    raise KeyError(f"Missing required field: {field}")
            
            
            print(chalk.blue("Starting scrape process..."))
            scraped_file = run_italist_scraper(
                msg['brand'],
                msg['category'],
                output_dir, 
                msg['query_hash'],
                msg.get('local_test'))
            
            #if scraped file is none, scraper didnt produce file
            if not scraped_file:
                raise Exception ("Scraper failed to produce output file")
            
            #if scraped_file path DNE 
            if not os.path.exists(scraped_file):
                raise Exception (f"Scraper output file not found: {scraped_file}")

            complete_msg = {
                'type': 'SCRAPE',
                'status':'PASS',
                'query_hash': msg.get('query_hash'),
                'output_dir': output_dir,
                'specific_item': msg.get('specific_item'),  # Forward specific_item if present
                'scraped_file': scraped_file,
                'paths':paths #include paths in msg to next worker
            }
            
            print(chalk.blue(f"Publishing Scrape SUCCESS Msg: {complete_msg}"))
            PROCESS_publish_to_queue(complete_msg)
            print(chalk.green("SCRAPE_COMPLETE message sent to process_queue."))

        except Exception as e:

            fail_msg = {
                'type': 'SCRAPE',
                'status':'FAIL',
                'query_hash': msg.get('query_hash'),
                'output_dir': output_dir,
                'specific_item': msg.get('specific_item'), 
                'scraped_file': scraped_file,
                'error': str(e),
                'paths': paths
            }
            
           
            print(chalk.blue(f"Publishing Scrape FAIL Msg: {fail_msg}"))
            PROCESS_publish_to_queue(fail_msg)
            print(chalk.green("SCRAPE_COMPLETE message sent to process_queue."))

            print(chalk.red(f"Error processing message: {e}"))
            import traceback
            print(chalk.red(f"Traceback: {traceback.format_exc()}"))
            
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # RabbitMQ setup
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()

        # Declare queue with all parameters explicit
        channel.queue_declare(
            queue='scrape_queue',
            durable=True,
            exclusive=False,
            auto_delete=False
        )

        # Basic QoS and consume setup
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue='scrape_queue',
            on_message_callback=callback,
            auto_ack=False
        )
        
        print(chalk.green("Clearing queue"))
        channel.queue_purge(queue='scrape_queue')

        print(chalk.blue('(SCRAPE_WORKER)[*] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()

    except Exception as e:
        print(chalk.red(f"Error during RabbitMQ setup: {e}"))
        import traceback
        print(chalk.red(f"Traceback: {traceback.format_exc()}"))
    finally:
        if connection and connection.is_open:
            try:
                connection.close()
                print(chalk.blue("Connection closed successfully"))
            except Exception as e:
                print(chalk.red(f"Error closing connection: {e}"))

if __name__ == "__main__":
    print(chalk.green("Starting scrape worker..."))
    main()