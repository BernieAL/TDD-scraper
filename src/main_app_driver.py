# src/main_app_driver.py
import os,csv,sys,pika,json
from datetime import datetime
from simple_chalk import chalk

# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

curr_dir = os.path.dirname(os.path.abspath(__file__))
user_category_data_file = os.path.join(curr_dir, 'input_data', 'search_criteria.csv')
scraped_data_dir_raw = os.path.join(curr_dir, 'scrape_file_output','raw')
scraped_data_dir_filtered = os.path.join(curr_dir, 'scrape_file_output','filtered')

# Import the ScraperUtils class from utils
from selenium_scraper_container.utils.ScraperUtils import ScraperUtils

# Import the scraper classes to construct instances of 
from selenium_scraper_container.scrapers.italist_scraper import ItalistScraper

#import comparision function
from analysis.compare_data import compare_driver


from rbmq.price_change_producer import PRICE_publish_to_queue
from rbmq.scrape_producer import SCRAPE_publish_to_queue
from rbmq.compare_producer import COMPARE_publish_to_queue


# Initialize the ScraperUtils instance
utils = ScraperUtils(scraped_data_dir_raw,scraped_data_dir_filtered)




# def run_scrapers(output_subdir, brand, category, specific_item):
#     try:
#         italist_driver(output_subdir, brand, category, specific_item, True)
#     except ImportError:
#         raise ImportError("italist_driver scraper could not be imported")
#     except Exception as e:
#         print(f"Error while running italist scraper: {e}")





def scrape_process(brand,category,specific_item):
        
        
        # Initialize the variables
        scraped_file = None
        filtered_file = None
        current_date = datetime.now().strftime('%Y-%d-%m')
     
        
        query = f"{brand}_{category}" #Prada_bags , Gucci_shirts
    
             

        query_hash = utils.generate_hash(query,specific_item,current_date)

        output_dir = utils.make_scraped_sub_dir_raw(brand,category,query_hash)
        print(output_dir)



        italist_scraper = ItalistScraper(brand,category,output_dir,query_hash,True) #if True, use local site copy
        scraped_file = italist_scraper.run()
        print(chalk.green(f"(MAIN_DRIVER){scraped_file}"))

      
        
        if specific_item != None:
            filtered_sub_dir = utils.make_filtered_sub_dir(brand,category,scraped_data_dir_filtered,query_hash)
            filtered_file = (utils.filter_specific(scraped_file,specific_item,filtered_sub_dir,query_hash))
            compare_driver(filtered_file,specific_item)

            # # #manual testing price change
            # filtered_file = os.path.join(scraped_data_dir_filtered,'FILTERED_prada_2024-14-10_bags_f3f28ac8','FILTERED_italist_prada_2024-14-10_bags_f3f28ac8.csv')
            # compare_driver(filtered_file)
        else:
            
            compare_driver(scraped_file,specific_item)
            
            # #manual testing price change
            # scraped_file = os.path.join(scraped_data_dir_raw,'RAW_SCRAPE_prada_2024-14-10_bags_f3f28ac8','RAW_italist_prada_2024-14-10_bags_f3f28ac8.csv')
            # compare_driver(scraped_file)

        return filtered_file if not scraped_file else scraped_file
        # return None



def wait_until_query_scrape_complete(query_hash=None):
     
    #listen for completion msg from scrape_worker published

    """
    Waits for a specific completion message from the scrape_completion_queue that matches the provided query_hash.
    """
    # Set up connection and queue listener
    connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='scrape_queue', durable=True)

    
    def callback(ch, method, properties, body):
        message = json.loads(body)
        
        # Check if the message matches the expected query_hash
        if message.get('type') == 'SCRAPE_COMPLETE' and message.get('query_hash') == query_hash:
            print(chalk.green(f":::Received completion message for query_hash: {query_hash}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
            channel.stop_consuming()  # Stop listening as the required message is received


    # Start consuming messages and wait until the specific completion message is received
    print(chalk.blue(f":::Waiting for completion message for query_hash: {query_hash}"))
    channel.basic_consume(queue='scrape_queue', on_message_callback=callback)
    channel.start_consuming()
    connection.close()


def wait_until_compare_process_complete(query_hash=None):
    """
    Waits for a specific completion message from the compare queue that matches the provided query_hash.

    compare recieves output dir and for each file, passes to compare driver
    which calls price change worker etc.
    """
    
    # Set up connection and queue listener
    connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='compare_queue', durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        
        # Check if the message matches the expected query_hash
        if message.get('type') == 'COMPARE_COMPLETE' and message.get('query_hash') == query_hash:
            print(chalk.green(f":::Received completion message for query_hash: {query_hash}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
            channel.stop_consuming()  # Stop listening as the required message is received

    # Start consuming messages and wait until the specific completion message is received
    print(chalk.blue(f":::Waiting for completion message for query_hash: {query_hash}"))
    channel.basic_consume(queue='compare_queue', on_message_callback=callback)
    channel.start_consuming()
    connection.close()

def wait_until_process_complete(query_hash=None):
    """
    Waits for a specific completion message from the compare queue that matches the provided query_hash.

    compare recieves output dir and for each file, passes to compare driver
    which calls price change worker etc.
    """
    
    # Set up connection and queue listener
    connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='process_queue', durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        

        # Check if the message matches the expected query_hash
        if message.get('type') == 'SCRAPE_COMPLETE' and message.get('query_hash') == query_hash:
            print(chalk.green(f":::Received SCRAPE completion message for query_hash: {query_hash}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
            channel.stop_consuming()  # Stop listening as the required message is received

        # Check if the message matches the expected query_hash
        elif message.get('type') == 'COMPARE_COMPLETE' and message.get('query_hash') == query_hash:
            print(chalk.green(f":::Received COMPARE completion message for query_hash: {query_hash}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
            channel.stop_consuming()  # Stop listening as the required message is received

    # Start consuming messages and wait until the specific completion message is received
    print(chalk.blue(f":::Waiting for completion message for query_hash: {query_hash}"))
    channel.basic_consume(queue='process_queue', on_message_callback=callback)
    channel.start_consuming()
    connection.close()


def scrape_process_2(brand,category,specific_item):
      
    # Initialize the variables
    scraped_file = None
    filtered_subdir = None
    current_date = datetime.now().strftime('%Y-%d-%m')
    
    
    query = f"{brand}_{category}" #Prada_bags , Gucci_shirts
            
    #if True, use local site copy
    query_hash = utils.generate_hash(query,specific_item,current_date)

    output_dir = utils.make_scraped_sub_dir_raw(brand,category,query_hash)
    print(output_dir)


    msg = {
            'brand':brand,
            'category':category,
            'output_dir':output_dir,
            'specific_item':specific_item,
            'query_hash':query_hash,
            'local_test':True
        }
    SCRAPE_publish_to_queue(msg)
    #wwhen this completes, output_dir will be populated with raw scraped files
    wait_until_process_complete(query_hash)
    
    #eval if filtering for specific query needed
    if specific_item != None:
            
            #create the sub dir to hold the filtered files
            filtered_subdir = utils.make_filtered_sub_dir(brand,category,scraped_data_dir_filtered,query_hash)

            for root,subdir,files in os.walk(output_dir):
                 
                 for raw_file in files:
                    raw_file_path = os.path.join(root,raw_file)
                    utils.filter_specific(raw_file_path,specific_item,filtered_subdir,query_hash)
            
            return query_hash,filtered_subdir
           
    
    return query_hash,output_dir

def driver_function():
    

    
    with open(user_category_data_file, 'r', newline='', encoding='utf-8') as file:
        

        csv_reader = csv.reader(file)
        #skip first line - file headers
        next(csv_reader)
        
        for file_row in csv_reader:
            
            #if file_row empty, continue
            if not file_row or not any(file_row):
                continue
            
            try:

                #extract brand,category, and spec item

                brand = file_row[0].strip().upper()
                category = file_row[1].strip().upper()

                #if file doesnt have spec item , use None
                specific_item = file_row[2].strip().upper() if len(file_row) > 2 else None
                print(specific_item)
                print(chalk.red(f"(MAIN) SPECIFIC ITEM- {specific_item}"))

                # query_hash,output_dir = scrape_process_2(brand, category, specific_item)

                # print(chalk.green(f"Output DIR {output_dir}"))



                #FILTERED SOLD TEST - using manually modified filtered scrape file
                query_hash='e9ace73b'
                output_dir = "/home/ubuntu/Documents/Projects/TDD-scraper/src/scrape_file_output/filtered/FILTERED_PRADA_2024-03-11_BAGS_1897e733"

                """
                because output dir holds all output files from all the scrapers,
                pass output dir to compare
                """
                
                COMPARE_publish_to_queue({'type':'POPULATED_OUTPUT_DIR','output_dir':output_dir,
                'query_hash':query_hash,
                'specific_item':specific_item})

                wait_until_process_complete(query_hash)

                PRICE_publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR QUERY","email":"balmanzar883@gmail.com",'brand':brand,'category':category,'query_hash':query_hash})

            except Exception as e:
                print(f"scrape_process failure {e}")

        
        print("processed all rows in input file")


# Example usage in your code
if __name__ == "__main__":
    # brand = 'Prada'
    
    # category = 'Bags'
    # output_dir = utils.make_scraped_sub_dir(brand, category)  # Call the method from ScraperUtils
    driver_function()
    # run_scrapers(output_dir, brand, category, None)
