import os
import csv
import sys,time
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

# scraped_data_dir_raw = os.path.join(curr_dir, 'scrape_file_output', 'raw')
# scraped_data_dir_filtered = os.path.join(curr_dir, 'scrape_file_output', 'filtered')


from shared_paths import RAW_SCRAPE_DIR,FILTERED_DATA_DIR,REPORTS_ROOT_DIR,SOLD_REPORTS_DIR,PRICE_REPORTS_DIR,ARCHIVE_DIR

# shared_data_dir = os.path.join(curr_dir,'..','shared_data')
# project_root = os.path.dirname(os.path.abspath(__file__))

scraped_data_dir_raw = RAW_SCRAPE_DIR
scraped_data_dir_filtered = FILTERED_DATA_DIR




# Import the ScraperUtils class from utils
from selenium_scraper_container.utils.ScraperUtils import ScraperUtils

# Import the scraper classes to construct instances of 
# from selenium_scraper_container.scrapers.italist_scraper import ItalistScraper
# from analysis.compare_data import compare_driver
from rbmq.price_change_producer import PRICE_publish_to_queue
from rbmq.scrape_producer import SCRAPE_publish_to_queue
from rbmq.compare_producer import COMPARE_publish_to_queue

# Initialize the ScraperUtils instance
utils = ScraperUtils(scraped_data_dir_raw, scraped_data_dir_filtered)

subprocess_status = {
    'SCRAPE' :False,
    'FILTER' : False,
    'COMPARE' : False,
    'EMAIL' : False
}

empty_sources = []  # Track sources that produced no results for query (general or specific)


def empty_file_check(output_dir,file,query_hash):

    """
    checks if given file has no content other than headers
    """
    
    
    try:
        with open(file) as f:
            reader = csv.reader(f)

            #skip headers
            next(reader) #date
            next(reader) # query
            next(reader) #headers
            next(reader) #delimiter

            #check for any addtl rows besides headers
            if not list(reader):
                return "EMPTY_FILE"
            
            return "NOT_EMPTY"
        
    except Exception as e:
        print(chalk.red(f"Error checking file:{e}"))
        return "FILE_CHECK_ERROR"

    



def scrape_process_2(brand, category, specific_item, local_test=True):
    current_date = datetime.now().strftime('%Y-%d-%m')
    query = f"{brand}_{category}"
    query_hash = utils.generate_hash(query, specific_item, current_date)
    output_dir = utils.make_scraped_sub_dir_raw(brand, category, query_hash)
    print(output_dir)

    msg = {
        'brand': brand,
        'category': category,
        'output_dir': output_dir,
        'specific_item': specific_item,
        'query_hash': query_hash,
        'local_test': local_test,
        'paths': {
            'raw_scrape_dir': RAW_SCRAPE_DIR,
            'filtered_data_dir': FILTERED_DATA_DIR,
            'reports_dir': REPORTS_ROOT_DIR,
            'sold_reports_dir': SOLD_REPORTS_DIR,
            'price_reports_dir': PRICE_REPORTS_DIR,
            'archive_dir': ARCHIVE_DIR
        }  # Your paths dict
    }

    # Track results for each source
    sources_with_no_raw_data = []      # Sources that produced no raw data
    sources_with_no_filtered_data = []  # Sources that had raw data but no filtered matches
    sources_with_results = []          # Sources that succeeded (either raw or filtered depending on spec_item)

    SCRAPE_publish_to_queue(msg)
    
    # Get scrape results from all sources
    scraped_files = wait_until_process_complete(query_hash, "SCRAPE")
    
    if subprocess_status['SCRAPE'] == False or not scraped_files:
        print(chalk.red(f"[ERROR] Scrape failed for query {query_hash}"))
        return query_hash, None

    # Process each source's raw file
    for scraped_file in scraped_files:
        source = scraped_file.split('_')[1]
        
        # Check raw scrape results
        raw_check_result = empty_file_check(output_dir, scraped_file, query_hash)
        if raw_check_result != "NOT_EMPTY":
            print(chalk.yellow(f"[INFO] Raw scrape produced no results for {source}"))
            sources_with_no_raw_data.append(source)
            continue
        
        # If we need to filter
        if specific_item is not None and specific_item.strip():
            filtered_subdir = utils.make_filtered_sub_dir(brand, category, scraped_data_dir_filtered, query_hash)
            filtered_file = utils.filter_by_specific_item(scraped_file, specific_item, filtered_subdir, query_hash)
            
            filtered_check_result = empty_file_check(filtered_subdir, filtered_file, query_hash)
            if filtered_check_result != "NOT_EMPTY":
                print(chalk.yellow(f"[INFO] No filtered results for {source}"))
                sources_with_no_filtered_data.append(source)
                continue
            
            sources_with_results.append(source)
        else:
            # No filtering needed, raw data is sufficient
            sources_with_results.append(source)

    # Process is successful if any source provided usable results
    subprocess_status['FILTER'] = len(sources_with_results) > 0

    print(chalk.blue("Processing Summary:"))
    print(chalk.blue(f"Sources with no raw data: {', '.join(sources_with_no_raw_data)}"))
    print(chalk.blue(f"Sources with no filtered matches: {', '.join(sources_with_no_filtered_data)}"))
    print(chalk.blue(f"Sources with usable results: {', '.join(sources_with_results)}"))

    if not sources_with_results:
        print(chalk.yellow(f"[INFO] No results from any source"))
        return query_hash, filtered_subdir if specific_item else output_dir

    return query_hash, filtered_subdir if specific_item else output_dir

def wait_until_process_complete(query_hash=None, expected_subprocess=None):
    """
    Waits for a specific message type for the given query_hash.
    """
    connection_params = pika.ConnectionParameters(
        host='localhost', 
        port=5672, 
        credentials=pika.PlainCredentials('guest', 'guest')
    )
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='process_queue', durable=True)

    scraped_file = None

    # Add timeout to wait for process to send completion msg
    timeout = 300  # 5 minutes timeout
    start_time = time.time()
    
    def callback(ch, method, properties, body):
        
        try:
            message = json.loads(body)
            print(chalk.blue(f"Received message: {message}"))

            # Always acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

            msg_type = message.get('type')
            msg_query_hash = message.get('query_hash')

            # Check if msg matches query response we're waiting for
            if msg_query_hash == query_hash and msg_type == expected_subprocess:
                    print(chalk.green(f":::Received {expected_subprocess} for query_hash: {query_hash}"))
                
                    if expected_subprocess == "SCRAPE" and message.get('status') == 'PASS':
                    #check status of subprocess, did it pass or fail? -. Ex. if SCRAPE has error - would be FAIL

                        if message.get('status')== 'PASS':
                            nonlocal scraped_file
                            scraped_file = message.get('scraped_file')
                            subprocess_status[expected_subprocess] = True
                            channel.stop_consuming()
                        elif message.get('status') == 'FAIL':
                            channel.stop_consuming()
                            subprocess_status[expected_subprocess] = False

                    else:
                        if message.get('status')=='PASS':
                            subprocess_status[expected_subprocess]= True
                            channel.stop_consuming()
                        elif message.get('status') == 'FAIL':
                            channel.stop_consuming()
                            subprocess_status[expected_subprocess]= False

            #if msg not expected type for query hash
            else:
                print(chalk.red(f"Recieved {msg_type} but Expected {expected_subprocess} for query_hash: {query_hash}"))
                channel.stop_consuming()
                raise Exception(message.get('error', 'Unknown error occurred'))

            # Check for timeout on current subprocess
            if time.time() - start_time > timeout:
                print(chalk.yellow(f"Timeout waiting for {expected_subprocess}"))
                channel.stop_consuming()
                raise TimeoutError(f"Timeout waiting for {expected_subprocess}")

        except Exception as e:
            print(chalk.red(f"Error processing message: {e}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        print(chalk.blue(f":::Waiting for {expected_subprocess} message for query_hash: {query_hash}"))
        channel.basic_consume(queue='process_queue', on_message_callback=callback)
        channel.start_consuming()


        return [scraped_file] if scraped_file else None
    except Exception as e:
        print(chalk.red(f"Error in message consumption: {e}"))
    finally:
        try:
            connection.close()
        except Exception as e:
            print(chalk.red(f"Error closing connection: {e}"))

def driver_function_from_input_file():
    with open(user_category_data_file, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header line

        email = 'balmanzar883@gmail.com'

        for file_row in csv_reader:
            if not file_row or not any(file_row):
                continue
            
            try:
                brand = file_row[0].strip().upper()
                category = file_row[1].strip().upper()
                specific_item = file_row[2].strip().upper() if len(file_row) > 2 else None
                print(chalk.red(f"(MAIN) SPECIFIC ITEM - {specific_item}"))

                # First run scraping process and wait for completion
                query_hash, output_dir = scrape_process_2(brand, category, specific_item)
                
                # After scrape is complete, publish compare message
                COMPARE_publish_to_queue({
                    'type': 'POPULATED_OUTPUT_DIR', 
                    'output_dir': output_dir,
                    'query_hash': query_hash,
                    'specific_item': specific_item
                })
                
                # Wait for compare completion
                wait_until_process_complete(query_hash, "COMPARE")
                
                # Signal price worker that all files are processed
                PRICE_publish_to_queue({
                    "type": "PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY",
                    "query_hash": query_hash,
                    "brand": brand,
                    "category": category,
                    "specific_item": specific_item,
                    "email":email
                })
                
                # Wait for email confirmation
                wait_until_process_complete(query_hash, "EMAIL")

            except Exception as e:
                print(chalk.red(f"Scrape process failure: {e}"))

        print("Processed all rows in input file")

def driver_function_from_input_file_2():

    with open(user_category_data_file, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header line

        email = 'balmanzar883@gmail.com'

        for file_row in csv_reader:
            if not file_row or not any(file_row):
                continue
            
            try:
                brand = file_row[0].strip().upper()
                category = file_row[1].strip().upper()
                specific_item = file_row[2].strip().upper() if len(file_row) > 2 else None
                print(chalk.red(f"(MAIN) SPECIFIC ITEM - {specific_item}"))

                # First run scraping process and wait for completion
                query_hash, output_dir = scrape_process_2(brand, category, specific_item)

                if subprocess_status['SCRAPE'] == False:
                    print(chalk.red(f"[ERROR] Scrape failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {specific_item}"))
                    continue
                
                elif subprocess_status['FILTER'] == False:
                    print(chalk.red(f"[ERROR] Filter failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {specific_item}"))
                    continue

                # After scrape is complete, publish compare message
                COMPARE_publish_to_queue({
                    'type': 'POPULATED_OUTPUT_DIR', 
                    'output_dir': output_dir,
                    'query_hash': query_hash,
                    'specific_item': specific_item
                })
                
                # Wait for compare completion
                wait_until_process_complete(query_hash, "COMPARE")
                
                if subprocess_status['COMPARE'] == False:
                    print(chalk.red(f"[ERROR] Compare failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {specific_item}"))
                    continue
                
                # Signal price worker that all files are processed
                PRICE_publish_to_queue({
                    "type": "PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY",
                    "query_hash": query_hash,
                    "brand": brand,
                    "category": category,
                    "specific_item": specific_item,
                    "email": email
                })
                
                # Wait for email confirmation
                wait_until_process_complete(query_hash, "EMAIL")
                
                if subprocess_status['EMAIL'] == False:
                    print(chalk.red(f"[ERROR] Email failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {specific_item}"))
                    continue
                
                print(chalk.green(f"[SUCCESS] Complete process successful for query {query_hash}"))

            except Exception as e:
                print(chalk.red(f"[ERROR] Process failure for query {query_hash}: {e}"))

        print(chalk.green("Processed all rows in input file"))

def driver_function_from_search_form(msg):

    brand = msg['brand'].strip().upper()
    category = msg['category'].strip().upper()
    spec_item = msg['spec_item'].strip().upper() if msg['spec_item'] else None
    requester_email = msg['user_email']
    search_id = msg['search_id']

    LOCAL_TEST = True
    query_hash, output_dir = scrape_process_2(brand, category, spec_item,LOCAL_TEST)

    if subprocess_status['SCRAPE'] == False:
        print(chalk.red(f"[ERROR] Scrape failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {spec_item}"))
        
        
        return

    elif subprocess_status['FILTER'] == False:
        print(chalk.red(f"[ERROR] Filter failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {spec_item}"))
        
        
        return

    # After scrape is complete, publish compare message
    COMPARE_publish_to_queue({
        'type': 'POPULATED_OUTPUT_DIR', 
        'output_dir': output_dir,
        'query_hash': query_hash,
        'specific_item': spec_item,
        'paths': {
            'raw_scrape_dir': RAW_SCRAPE_DIR,
            'filtered_data_dir': FILTERED_DATA_DIR,
            'reports_dir': REPORTS_ROOT_DIR,
            'sold_reports_dir': SOLD_REPORTS_DIR,
            'price_reports_dir': PRICE_REPORTS_DIR,
            'archive_dir': ARCHIVE_DIR
        }
    })
    
    # Wait for compare completion
    wait_until_process_complete(query_hash, "COMPARE")
    
    if subprocess_status['COMPARE'] == False:
        print(chalk.red(f"[ERROR] Compare failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {spec_item}"))
        return

    # Signal price worker that all files for current query are processed
    PRICE_publish_to_queue({
        "type": "PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY",
        "query_hash": query_hash,
        "brand": brand,
        "category": category,
        "specific_item": spec_item,
        "email": requester_email,
        'paths':{
            'raw_scrape_dir': RAW_SCRAPE_DIR,
            'filtered_data_dir': FILTERED_DATA_DIR,
            'reports_dir': REPORTS_ROOT_DIR,
            'sold_reports_dir': SOLD_REPORTS_DIR,
            'price_reports_dir': PRICE_REPORTS_DIR,
            'archive_dir': ARCHIVE_DIR
            }
    })
    
    # Wait for email process to complete
    wait_until_process_complete(query_hash, "EMAIL")
    
    if subprocess_status['EMAIL'] == False:
        print(chalk.red(f"[ERROR] Email failed for query {query_hash} - Brand: {brand}, Category: {category}, Item: {spec_item}"))
        return
    
    print(chalk.green(f"[SUCCESS] Complete process successful for query {query_hash}"))
# Example usage in your code
if __name__ == "__main__":
    driver_function_from_search_form()
    # driver_function_from_input_file()