import sys,csv,json,os
from typing import Dict, Set
import pika
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree  # For removing directories
import boto3

# For local development
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# For Docker
if os.getenv('RUNNING_IN_DOCKER') == '1' and '/app' not in sys.path:
    sys.path.insert(0, '/app')



def ensure_init_files():
    """
    for each dir in list dir, 
    check if each dir has __init__.py,
    if not make one. 

    This avoids the module not found issue that results
    from copying specific files without including __init__.py

    """
    #get current dir contents
    dirs = os.listdir()

    for dir in dirs:

        #get full path
        full_path = os.path.abspath(dir)

        #check if its a dir
        if os.path.isdir(full_path):
            
            #build path for __init__ file in curr dir
            init_file_path = os.path.join(full_path,'__init__.py')

            #check if init file exists
            if not os.path.exists(init_file_path):
                print(f"Creating __init__.py file for dir: {dir}")

                with open(init_file_path, 'w') as f:
                    pass
            else:
                print(f"__init__.py already exists for {dir}")

ensure_init_files()  



from config.config import BASE_DIR, RBMQ_DIR  
from rbmq.scrape_producer import SCRAPE_publish_to_queue
from selenium_scraper_container.utils.ScraperUtils import ScraperUtils
from selenium_scraper_container.scrapers.italist_scraper import ItalistScraper
from rbmq.process_producer import PROCESS_publish_to_queue

from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection


import sys, csv, json, os
import pika
from simple_chalk import chalk
from datetime import datetime
from typing import Dict, List, Optional, Set

# Import statements remain the same...

class ScraperOrchestrator:
    def __init__(self):
        self.scrapers = {
            'italist': ItalistScraper,
            # Add other scrapers here
        }
        # Track failures per query hash
        self.failed_scrapers: Dict[str, Set[str]] = {}
        # Track error messages for failed scrapers
        self.scraper_errors: Dict[str, Dict[str, str]] = {}
        
    def get_active_scrapers(self) -> List[str]:
        """Returns list of currently active scraper names"""
        return list(self.scrapers.keys())
    
    def record_failure(self, query_hash: str, scraper_name: str, error_msg: str):
        """Record a scraper failure for a specific query"""
        if query_hash not in self.failed_scrapers:
            self.failed_scrapers[query_hash] = set()
            self.scraper_errors[query_hash] = {}
            
        self.failed_scrapers[query_hash].add(scraper_name)
        self.scraper_errors[query_hash][scraper_name] = error_msg
    
    def get_failed_scrapers(self, query_hash: str) -> List[str]:
        """Get list of failed scrapers for a query"""
        return list(self.failed_scrapers.get(query_hash, set()))
    
    def get_failure_details(self, query_hash: str) -> Dict[str, str]:
        """Get error messages for failed scrapers"""
        return self.scraper_errors.get(query_hash, {})
    
    def run_scraper(self, 
                   scraper_name: str, 
                   brand: str,
                   category: str,
                   output_dir: str,
                   query_hash: str,
                   local: bool) -> Optional[str]:
        """
        Runs a single scraper and returns the path to the scraped file
        """
        try:
            if scraper_name not in self.scrapers:
                raise ValueError(f"Unknown scraper: {scraper_name}")
            
            scraper_class = self.scrapers[scraper_name]
            scraper = scraper_class(brand, category, output_dir, query_hash, local)
            
            print(chalk.blue(f"Running {scraper_name} scraper with:"))
            print(chalk.blue(f"Brand: {brand}"))
            print(chalk.blue(f"Category: {category}"))
            print(chalk.blue(f"Output Dir: {output_dir}"))
            
            scraped_file = scraper.run()
            
            if scraped_file and os.path.exists(scraped_file):
                print(chalk.green(f"{scraper_name} completed successfully: {scraped_file}"))
                return scraped_file
            else:
                error_msg = f"{scraper_name} completed but produced no results"
                print(chalk.yellow(error_msg))
                self.record_failure(query_hash, scraper_name, error_msg)
                return None
                
        except Exception as e:
            error_msg = f"Error running {scraper_name}: {str(e)}"
            print(chalk.red(error_msg))
            self.record_failure(query_hash, scraper_name, error_msg)
            return None

    def run_all_scrapers(self, 
                        brand: str,
                        category: str,
                        output_dir: str,
                        query_hash: str,
                        local: bool) -> Dict[str, Optional[str]]:
        """
        Runs all active scrapers and returns a dictionary of results
        """
        results = {}
        
        for scraper_name in self.get_active_scrapers():
            scraped_file = self.run_scraper(
                scraper_name, brand, category, output_dir, query_hash, local
            )
            if scraped_file:
                results[scraper_name] = scraped_file
                
        return results

def main():
    orchestrator = ScraperOrchestrator()
    
    def callback(ch, method, properties, body):
        try:
            print(chalk.yellow("Received message on scrape_queue"))
            # msg = json.loads(body)
            
            #retrieve query from s3 bucket
            s3 = boto3.client('s3')
            response = s3.get_object(
                Bucket=s3.get_object(
                    Bucket='scraper-data-bucket',
                    Key=f'queries/{query_hash}/params.json'
                )
          
            )

            #convery query data to json
            msg = json.loads(response['Body'].read())

            # Verify required fields in 
            required_fields = ['query_hash', 'brand', 'category', 'output_dir']
            for field in required_fields:
                if field not in msg:
                    raise KeyError(f"Missing required field: {field}")
            
            #extract specific fields from query 
            paths = msg.get('paths', {})
            output_dir = msg['output_dir']
            query_hash = msg['query_hash']
            
            print(chalk.blue("Starting scrape processes..."))
            
            # Run all scrapers
            scraped_files = orchestrator.run_all_scrapers(
                msg['brand'],
                msg['category'],
                output_dir,
                query_hash,
                msg.get('local_test', True)
            )
            
            # Get failure information
            failed_scrapers = orchestrator.get_failed_scrapers(query_hash)
            failure_details = orchestrator.get_failure_details(query_hash)
            
            # Check if any scrapers succeeded
            if not scraped_files:
                raise Exception(f"All scrapers failed. Failures: {failure_details}")
            
            # Send success message with all scraped files and failure info
            complete_msg = {
                'type': 'SCRAPE',
                'status': 'PASS',
                'query_hash': query_hash,
                'output_dir': output_dir,
                'specific_item': msg.get('specific_item'),
                'scraped_files': scraped_files,
                'failed_scrapers': list(failed_scrapers),
                'failure_details': failure_details,
                'paths': paths
            }
            
            print(chalk.blue(f"Publishing Scrape SUCCESS Msg: {complete_msg}"))
            PROCESS_publish_to_queue(complete_msg)
            
        except Exception as e:
            fail_msg = {
                'type': 'SCRAPE',
                'status': 'FAIL',
                'query_hash': msg.get('query_hash'),
                'output_dir': msg.get('output_dir'),
                'specific_item': msg.get('specific_item'),
                'scraped_files': {},
                'failed_scrapers': orchestrator.get_failed_scrapers(query_hash),
                'failure_details': orchestrator.get_failure_details(query_hash),
                'error': str(e),
                'paths': msg.get('paths', {})
            }
            
            print(chalk.blue(f"Publishing Scrape FAIL Msg: {fail_msg}"))
            PROCESS_publish_to_queue(fail_msg)
            
            print(chalk.red(f"Error processing message: {e}"))
            import traceback
            print(chalk.red(f"Traceback: {traceback.format_exc()}"))
            
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # RabbitMQ setup remains the same...

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