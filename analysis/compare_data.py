import os
import sys
import csv
import json
import psycopg2
from datetime import datetime
from simple_chalk import chalk

# Get parent directory and add it to sys.path for importing other modules
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from db.db_utils import (
    DB_fetch_product_ids_prices_dates, 
    DB_bulk_update_existing, 
    bulk_insert_new,
    DB_bulk_update_sold,
    DB_get_sold
)
from rbmq.price_change_producer import PRICE_publish_to_queue

# Directory setup for input/output
curr_dir = os.path.dirname(os.path.abspath(__file__))
file_output_dir = os.path.join(curr_dir, '..', 'src', 'file_output')


def parse_file_name(file):
    """
    filenames recieved are in the same format.
    possible filenames recieved:
        FILTERED_italist_prada_2024-14-10_bags_c4672843.csv
        RAW_italist_prada_2024-14-10_bags_f3f28ac8.csv
    """
    file_path_tokens = file.split('/')[-1]
    file_name_tokens = file_path_tokens.split('_')
    source = file_name_tokens[1]
    brand = file_name_tokens[2]
    date = file_name_tokens[3]
    category = file_name_tokens[4]
    query_hash = file_name_tokens[-1].split('.')[0]

    print(chalk.blue(f"file_name_tokens {file_name_tokens}"))
    print(chalk.blue(f"source: {source}"))
    print(chalk.blue(f"brand {brand}"))
    print(chalk.blue(f"date: {date}"))
    print(chalk.blue(f"category {category}"))
    print(chalk.blue(f"query hash: {query_hash}"))

    return {
        'brand':brand,
        'source':source,
    }

def db_data_to_dictionary(existing_db_data_list):


    """

    existing_product_id_prices_dates pre-loads all product ids, prices, and dates

    convert from db data from list of dicts to dict into dictionary for easy lookup

    db data as list of dicts:
        db data comes in the format of list
            [
                {'product_id': '14699240-14531549', 'curr_price': Decimal('418.00'), 'curr_scrape_date': datetime.date(2024, 9, 25), 'prev_price': Decimal('418.00'), 'prev_scrape_date': datetime.date(2024, 9, 25)}, 
                {'product_id': '14696885-14529194', 'curr_price': Decimal('3955.00'), 'curr_scrape_date': datetime.date(2024, 9, 25), 'prev_price': Decimal('3955.00'), 'prev_scrape_date': datetime.date(2024, 9, 25)}, 
                {'product_id': '14696866-14529175', 'curr_price': Decimal('3714.00'), 'curr_scrape_date': datetime.date(2024, 9, 25), 'prev_price': Decimal('3714.00'), 'prev_scrape_date': datetime.date(2024, 9, 25)},
            ]

    
    desired output Ex.
        Each product id is key of its own dictionary, where the values are in another dictionary themselves
            existing_product_id_prices_dict = {
                '1124423-134234': {'curr_price':1003.00,
                                'curr_scrape_date':2024/9/3,
                                'prev_price':1100,
                                'prev_scrape_date':2024/9/15
                                },
                '1124423-134234': {'curr_price':1003.00,
                                'curr_scrape_date':2024/9/3,
                                'prev_price':1100,
                                'prev_scrape_date':2024/9/15
                                },

            }
    """
    try:
        existing_db_data_dict = {}
        # print(chalk.red(existing_db_data_list))

        for prod in existing_db_data_list:
            product_id = prod['product_id'] 
            curr_price = float(prod['curr_price'])
            curr_scrape_date = prod['curr_scrape_date']
            prev_price = float(prod['prev_price'])
            prev_scrape_date = prod['prev_scrape_date']
        
    

            existing_db_data_dict[product_id] = {
                'curr_price': curr_price,
                'curr_scrape_date': curr_scrape_date,
                'prev_price': prev_price,
                'prev_scrape_date': prev_scrape_date
            }
            
        return existing_db_data_dict 
    except Exception as e:
         print(chalk.red(f"Error converting DB data to dictionary: {e}"))

def compare_scraped_data_to_db(input_file, existing_product_data_dict,source,spec_item=None):
    """
    Compares scraped product data to existing database records. Updates existing products, adds new products, 
    and marks products as sold if they are no longer listed.

    :param input_file: Path to the scraped data file.
    :param existing_product_data_dict: Dictionary of existing product data from the database.
    """
    
    
    if spec_item != None:
        print(chalk.yellow(f"PROCESSING FILTERED QUERY - SPEC ITEM {spec_item}"))
    else:
         print(chalk.yellow(f"PROCESSING GENERAL QUERY - NO SPEC ITEM"))
    try:
        updated_products = []
        new_products = []

        with open(input_file, mode='r') as file:
            csv_reader = csv.reader(file)

            # Get scrape date from file header
            scrape_date_line = next(csv_reader)
            scrape_date = scrape_date_line[0].split(':')[1].strip()
            scrape_date = datetime.strptime(scrape_date, '%Y-%d-%m').date()
            print(f"Scrape Date: {scrape_date}")

            # Get query info
            query_line = next(csv_reader)
            query = query_line[0].split(':')[1].strip()
            print(f"Query: {query}")

            # Process CSV headers and rows
            headers = next(csv_reader)
            next(csv_reader)  # Skip delimiter row
            csv_reader = csv.DictReader(file, fieldnames=headers)

            for row in csv_reader:
                print(chalk.red(f"ID: {row['product_id']}, Brand: {row['brand']}, "
                f"Product: {row['product_name']}, Curr Price: {row['curr_price']}"))

                # Compare against existing product data from DB
                if row['product_id'] in existing_product_data_dict:
                    process_existing_product(row, existing_product_data_dict, updated_products, scrape_date,input_file,source)
                else:
                    process_new_product(row, scrape_date, new_products,source)

        # Bulk update and insert operations
        if len(new_products) > 0:
            bulk_insert_new(new_products)
        
        #all remaining in dict were not found in todays scrape - meaning they were sold
        items_not_found = existing_product_data_dict
        DB_bulk_update_sold(items_not_found)

        #get sold items for this src and push to queue
        sold_items = DB_get_sold(source,spec_item)
        print(chalk.green(f"SOLD ITEMS: {sold_items}\n ---------------" ))

        #push sold_items to queue
        PRICE_publish_to_queue({"type":"PROCESSING SOLD ITEMS COMPLETE","sold_items":sold_items,"source_file": input_file})
        


        # After processing all products, send completion signal
        PRICE_publish_to_queue({"type": "PROCESSING SCRAPED FILE COMPLETE", "source_file": input_file})

    except Exception as e:
        print(chalk.red(f"Error comparing scraped data: {e}"))
        raise


def process_existing_product(row, existing_product_data_dict, updated_products, scrape_date,input_file,source):
    """
    Processes an existing product by checking for price changes and updating the necessary fields.

    :param row: Scraped data row from the CSV.
    :param existing_product_data_dict: Dictionary of existing products from the DB.
    :param updated_products: List to hold updated product data.
    :param scrape_date: Date of the current scrape.
    """
    product_data = existing_product_data_dict[row['product_id']]
    if float(row['curr_price']) != product_data['curr_price']:
        # Update price and date
        product_data['prev_price'] = product_data['curr_price']
        product_data['curr_price'] = float(row['curr_price'])
        product_data['prev_scrape_date'] = product_data['curr_scrape_date']
        product_data['curr_scrape_date'] = scrape_date

        temp = {
            'product_id': row['product_id'],
            'curr_price': product_data['curr_price'],
            'curr_scrape_date': scrape_date,
            'prev_price': product_data['prev_price'],
            'prev_scrape_date': product_data['prev_scrape_date']
        }

        #add to updated list - for price and scrape dates to be updated
        updated_products.append(temp)
        # **temp unpacks all key value pairs from temp dict and adds prod_name,listing_url to it as new dict. 
        #this is like spread operator in js
        PRICE_publish_to_queue({'product_name': row['product_name'],**temp,  'listing_url': row['listing_url'],"source_file": input_file,"source":source})
    else:
        # Update  product scrape dates if price has not changed
        product_data['prev_scrape_date'] = product_data['curr_scrape_date']
        product_data['curr_scrape_date'] = scrape_date
        updated_products.append({
            'product_id': row['product_id'],
            'curr_price': product_data['curr_price'],
            'curr_scrape_date': scrape_date,
            'prev_price': product_data['prev_price'],
            'prev_scrape_date': product_data['prev_scrape_date']
        })

    print(chalk.yellow(f"Updated product: {row['product_id']}"))
    existing_product_data_dict.pop(row['product_id'])


def process_new_product(row, scrape_date, new_products):
    """
    Processes a new product that was not found in the existing database records.

    :param row: Scraped data row from the CSV.
    :param scrape_date: Date of the current scrape.
    :param new_products: List to hold new product data.
    """

    print(chalk.red(row['source']))
    temp = {
        'product_id': row['product_id'],
        'brand': row['brand'],
        'product_name': row['product_name'],
        'curr_price': float(row['curr_price']),
        'curr_scrape_date': scrape_date,
        'prev_price': float(row['curr_price']),
        'prev_scrape_date': scrape_date,
        'sold_date': None,
        'sold': False,
        'listing_url':row['listing_url'],
        'source':row['source']
    }
    new_products.append(temp)
    print(chalk.green(f"New product added: {row['product_id']}"))


def compare_driver(scraped_data_file_path,spec_item=None):
    """
    Drives the comparison process for a given scraped data file.

    :param scraped_data_file_path: Path to the scraped data file.
    """


    try:
        
        res = parse_file_name(scraped_data_file_path)
        brand = (res['brand']).upper()
        source = (res['source']).upper()
      
        #only get db records that match this spec item
        db_data = DB_fetch_product_ids_prices_dates(brand,source,spec_item)

        #convert retrieved db data to dictionary
        existing_product_ids_prices_dict = db_data_to_dictionary(db_data)
        print(existing_product_ids_prices_dict)
        
        compare_scraped_data_to_db(scraped_data_file_path,existing_product_ids_prices_dict,source,spec_item)

    except Exception as e:
        print(chalk.red(f"Error in comparison driver: {e}"))
        raise



if __name__ == "__main__":
    
   
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    scrape_data_dir = os.path.join('src','scrape_file_output')
    print(scrape_data_dir)
   
    
    # general_input_file_path = os.path.join(curr_dir,'..',scrape_data_dir,'raw','RAW_SCRAPE_PRADA_2024-24-10_BAGS_a44eabd1','RAW_ITALIST_PRADA_2024-24-10_BAGS_a44eabd1.csv')
    # filtered_input_file_path =  os.path.join(curr_dir,'..',scrape_data_dir,'filtered','FILTERED_PRADA_2024-25-10_BAGS_962a1246','FILTERED_ITALIST_PRADA_2024-25-10_BAGS_962a1246.csv')
    
    # compare_driver(filtered_input_file_path,'EMBROIDERED FABRIC SMALL SYMBOLE SHOPPING BAG')
    # PRICE_publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR QUERY","email":"balmanzar883@gmail.com","source_file": filtered_input_file_path})



    test_output_dir_path = os.path.join(scrape_data_dir,'filtered','RAW_SCRAPE_PRADA_2024-30-10_BAGS_7eabf40e')
    print(test_output_dir_path)
    for root,subdirs,files in os.walk(test_output_dir_path):
        for file in files:
            print(file)

    # compare_driver(general_input_file_path)
    # publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR QUERY","email":"balmanzar883@gmail.com","source_file": general_input_file_path})