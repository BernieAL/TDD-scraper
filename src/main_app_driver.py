import os
import csv
import sys
import pika
import json
import asyncio
from datetime import datetime
from simple_chalk import chalk

# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

curr_dir = os.path.dirname(os.path.abspath(__file__))
user_category_data_file = os.path.join(curr_dir, 'input_data', 'search_criteria.csv')
scraped_data_dir_raw = os.path.join(curr_dir, 'scrape_file_output', 'raw')
scraped_data_dir_filtered = os.path.join(curr_dir, 'scrape_file_output', 'filtered')

# Import the ScraperUtils class from utils
from selenium_scraper_container.utils.ScraperUtils import ScraperUtils

# Import the scraper classes to construct instances of 
from selenium_scraper_container.scrapers.italist_scraper import ItalistScraper
from analysis.compare_data import compare_driver
from rbmq.price_change_producer import PRICE_publish_to_queue
from rbmq.scrape_producer import SCRAPE_publish_to_queue
from rbmq.compare_producer import COMPARE_publish_to_queue

# Initialize the ScraperUtils instance
utils = ScraperUtils(scraped_data_dir_raw, scraped_data_dir_filtered)

# Global flags to track completion of workers
worker_status = {
    "price_worker_complete": False,
    "compare_worker_complete": False,
}



# Function to reset worker status
def reset_worker_status():
    worker_status["price_worker_complete"] = False
    worker_status["compare_worker_complete"] = False



def wait_until_process_complete(query_hash=None):
    """
    Waits asynchronously for completion messages from the process queue for the provided query_hash.
    """

    loop = asyncio.get_event_loop()

    # Set up connection and queue listener
    connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='process_queue', durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body)

        # Handle SCRAPE_COMPLETE message
        if message.get('type') == 'SCRAPE_COMPLETE' and message.get('query_hash') == query_hash:
            print(chalk.green(f":::Received SCRAPE completion message for query_hash: {query_hash}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
            # You can add any necessary logic for when the scrape is complete here
            return True

        # Handle COMPARE_COMPLETE message
        elif message.get('type') == 'COMPARE_COMPLETE' and message.get('query_hash') == query_hash:
            print(chalk.green(f":::Received COMPARE completion message for query_hash: {query_hash}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
            # Send final signal after compare is complete
            PRICE_publish_to_queue(
                {'type': 'PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY',
                 'email': '(main)balmanzar883@gmail.com',
                 'query_hash': query_hash})

            return True

    # Start consuming messages and wait until the specific completion message is received
    print(chalk.blue(f":::Waiting for completion message for query_hash: {query_hash}"))
    channel.basic_consume(queue='process_queue', on_message_callback=callback)

    # Start consuming and block until the message is processed
    channel.start_consuming()

    # Close the connection once finished
    connection.close()


def check_all_workers_complete(query_hash):
    if worker_status["price_worker_complete"] and worker_status["compare_worker_complete"]:
        # Send the end signal
        PRICE_publish_to_queue({'type': 'PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY', 'email': '(main)balmanzar883@gmail.com', 'query_hash': query_hash})
        reset_worker_status()  # Reset after sending the signal

def scrape_process_2(brand, category, specific_item):
    current_date = datetime.now().strftime('%Y-%d-%m')
    query = f"{brand}_{category}"  # e.g., Prada_bags, Gucci_shirts
    query_hash = utils.generate_hash(query, specific_item, current_date)
    output_dir = utils.make_scraped_sub_dir_raw(brand, category, query_hash)
    print(output_dir)

    msg = {
        'brand': brand,
        'category': category,
        'output_dir': output_dir,
        'specific_item': specific_item,
        'query_hash': query_hash,
        'local_test': True
    }
    SCRAPE_publish_to_queue(msg)
    wait_until_process_complete(query_hash)

    if specific_item is not None:
        filtered_subdir = utils.make_filtered_sub_dir(brand, category, scraped_data_dir_filtered, query_hash)
        for root, subdir, files in os.walk(output_dir):
            for raw_file in files:
                raw_file_path = os.path.join(root, raw_file)
                utils.filter_specific(raw_file_path, specific_item, filtered_subdir, query_hash)
        return query_hash, filtered_subdir

    return query_hash, output_dir

def driver_function():
    with open(user_category_data_file, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header line

        for file_row in csv_reader:
            if not file_row or not any(file_row):
                continue
            
            try:
                brand = file_row[0].strip().upper()
                category = file_row[1].strip().upper()
                specific_item = file_row[2].strip().upper() if len(file_row) > 2 else None
                print(chalk.red(f"(MAIN) SPECIFIC ITEM - {specific_item}"))

                manual_file_test = {
                    '153d5b66': '/home/ubuntu/Documents/Projects/TDD-scraper/src/scrape_file_output/filtered/FILTERED_PRADA_2024-07-11_BAGS_153d5b66',
                    '684f7807': '/home/ubuntu/Documents/Projects/TDD-scraper/src/scrape_file_output/filtered/FILTERED_PRADA_2024-07-11_BAGS_684f7807'
                }

                for query_hash, output_dir in manual_file_test.items():
                    print(f"Query Hash: {query_hash}")
                    print(f"File Path: {output_dir}")

                    COMPARE_publish_to_queue({'type': 'POPULATED_OUTPUT_DIR', 'output_dir': output_dir,
                                               'query_hash': query_hash,
                                               'specific_item': specific_item})

                    wait_until_process_complete(query_hash)

            except Exception as e:
                print(f"Scrape process failure: {e}")

        print("Processed all rows in input file")

# Example usage in your code
if __name__ == "__main__":
    driver_function()
