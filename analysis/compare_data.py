import os
import sys
import csv
import json
import psycopg2
from datetime import datetime,date
from simple_chalk import chalk

# Get parent directory and add it to sys.path for importing other modules
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_SCRAPE_DATE = None

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from db.db_utils import (
    DB_fetch_product_ids_prices_dates, 
    DB_bulk_update_existing, 
    DB_bulk_insert_new,
    DB_bulk_update_sold,
    DB_get_sold_daily
)
from rbmq.price_change_producer import PRICE_publish_to_queue

# Directory setup for input/output
curr_dir = os.path.dirname(os.path.abspath(__file__))
file_output_dir = os.path.join(curr_dir, '..', 'src', 'file_output')


def JSON_serializer(input_dict):

    for key,inner_dict in input_dict.items():
        for inner_key,value in inner_dict.items():
            if isinstance(value,(datetime,date)):
                inner_dict[inner_key] = value.strftime('%Y-%m-%d')
        return input_dict

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

def db_data_to_dictionary_old(existing_db_data_list):


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

        for prod in existing_db_data_list:
            product_id = prod['product_id'] 
            # Round all prices to 2 decimal places
            curr_price = round(float(prod['curr_price']), 2)
            curr_scrape_date = prod['curr_scrape_date']
            prev_price = round(float(prod['prev_price']), 2)
            prev_scrape_date = prod['prev_scrape_date']
        
            print(chalk.blue(f"Converting DB data for {product_id}:"))
            print(chalk.blue(f"  Current price: {curr_price} ({type(curr_price)})"))
            print(chalk.blue(f"  Previous price: {prev_price} ({type(prev_price)})"))

            existing_db_data_dict[product_id] = {
                'curr_price': curr_price,
                'curr_scrape_date': curr_scrape_date,
                'prev_price': prev_price,
                'prev_scrape_date': prev_scrape_date
            }
            
        return existing_db_data_dict 
    except Exception as e:
         print(chalk.red(f"Error converting DB data to dictionary: {e}"))

def process_existing_product(row, existing_product_data_dict, updated_products, scrape_date,input_file,source):
    """
    Processes an existing product by checking for price changes and updating the necessary fields.

    :param row: Scraped data row from the CSV.
    :param existing_product_data_dict: Dictionary of existing products from the DB.
    :param updated_products: List to hold updated product data.
    :param scrape_date: Date of the current scrape.
    """
    product_data = existing_product_data_dict[row['product_id']]
    
    #Convert prices to float and round to handle precision issues
    current_file_price = round(float(row['curr_price']), 2)
    current_db_price = round(product_data['curr_price'], 2)
    
    print(chalk.blue(f"Comparing prices for {row['product_id']}:"))
    print(chalk.blue(f"  File price: {current_file_price} ({type(current_file_price)})"))
    print(chalk.blue(f"  DB price: {current_db_price} ({type(current_db_price)})"))
    
    # Compare rounded float values
    if current_file_price != current_db_price:
        print(chalk.yellow(f"Price change detected: {current_db_price} -> {current_file_price}"))
        # Update price and date
        product_data['prev_price'] = product_data['curr_price']
        product_data['curr_price'] = current_file_price
        product_data['prev_scrape_date'] = product_data['curr_scrape_date']
        product_data['curr_scrape_date'] = scrape_date

        temp = {
            'product_id': row['product_id'],
            'curr_price': product_data['curr_price'],
            'curr_scrape_date': scrape_date,
            'prev_price': product_data['prev_price'],
            'prev_scrape_date': product_data['prev_scrape_date']
        }

        updated_products.append(temp)

        #temp** unpacks rest of unchanged values in dict
        PRICE_publish_to_queue({
            'type': 'PRODUCT_PRICE_CHANGE',
            'product_name': row['product_name'],
            **temp,
            'listing_url': row['listing_url'],
            'source': source
        })
        print(chalk.yellow(f"Published price change for {temp['product_id']}"))
    else:
        print(chalk.blue(f"No price change needed for {row['product_id']}"))
        # Only update scrape dates
        product_data['prev_scrape_date'] = product_data['curr_scrape_date']
        product_data['curr_scrape_date'] = scrape_date
        updated_products.append({
            'product_id': row['product_id'],
            'curr_price': current_db_price,  # Use the rounded DB price
            'curr_scrape_date': scrape_date,
            'prev_price': product_data['prev_price'],
            'prev_scrape_date': product_data['prev_scrape_date']
        })

    print(chalk.yellow(f"Updated product: {row['product_id']}"))
    existing_product_data_dict.pop(row['product_id'])


def process_new_product(row, scrape_date, new_products,source):
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




def compare_scraped_data_to_db(input_file, existing_product_data_dict, source, query_hash, spec_item=None):
    """
    Compares scraped product data to existing database records.
    """
    try:
        if spec_item is not None:
            print(chalk.yellow(f"PROCESSING_FILTERED_QUERY - SPEC ITEM {spec_item}"))
        else:
            print(chalk.yellow(f"PROCESSING_GENERAL_QUERY - NO SPEC ITEM"))

        updated_products = []
        new_products = []
        scrape_date = None  # Initialize scrape_date

        with open(input_file, mode='r') as file:
            csv_reader = csv.reader(file)

            # Get scrape date from file header
            scrape_date_line = next(csv_reader)
            scrape_date = scrape_date_line[0].split(':')[1].strip()
            scrape_date = datetime.strptime(scrape_date, '%Y-%d-%m').date()
            global FILE_SCRAPE_DATE
            FILE_SCRAPE_DATE = scrape_date
            print(f"Scrape Date: {scrape_date}")

            # Get query info
            query_line = next(csv_reader)
            query = query_line[0].split(':')[1].strip()
            print(f"Query: {query}")

            # Process CSV headers and rows
            headers = next(csv_reader)
            next(csv_reader)  # Skip delimiter row
            csv_reader = csv.DictReader(file, fieldnames=headers)
            

            #check if file has any addtl data rows - in even filtered result returned empty file
            data = list(csv_reader)
            
            if not data:
                print(chalk.yellow("File is empty (no data rows)"))
                # Send both completion messages for empty file
                PRICE_publish_to_queue({
                    "type": "PROCESSING_SOLD_ITEMS_COMPLETE",
                    "sold_items_dict": {},
                    "query_hash": query_hash
                })
                PRICE_publish_to_queue({
                    "type": "PROCESSING_SCRAPED_FILE_COMPLETE",
                    "query_hash": query_hash,
                    "product_name": spec_item,
                    "scrape_file_empty": True,
                    "source": source
                })
                return

            # Process each row from data, since we already converted csv reader to list and stored in data
            for row in data:
                print(chalk.red(f"ID: {row['product_id']}, Brand: {row['brand']}, "
                      f"Product: {row['product_name']}, Curr Price: {row['curr_price']}"))

                if row['product_id'] in existing_product_data_dict:
                    process_existing_product(row, existing_product_data_dict, updated_products, scrape_date, input_file, source)
                else:
                    process_new_product(row, scrape_date, new_products, source)

            # Database operations
            db_success = True
            try:
                # Bulk update existing products
                if updated_products:
                    if not DB_bulk_update_existing(updated_products):
                        raise Exception("DB update operation failed")

                # Bulk insert new products
                if new_products:
                    if not DB_bulk_insert_new(new_products):
                        raise Exception("DB insert operation failed")

                print(chalk.green("[SUCCESS] DB operations were successful."))

                # Process sold items
                items_not_found = existing_product_data_dict
                print(chalk.cyan(f"db items not found in current scrape file {items_not_found}"))

                if items_not_found:  # Only process if there are items not found
                    # Update sold status
                    if not DB_bulk_update_sold(items_not_found):
                        raise Exception("Sold status update failed")
                
                    # Get sold items
                    sold_items_dict = DB_get_sold_daily(source, items_not_found, FILE_SCRAPE_DATE, spec_item)
                    print(chalk.green(f"SOLD ITEMS: {sold_items_dict}\n ---------------"))

                    # Push sold items to queue
                    
                    PRICE_publish_to_queue({
                        "type": "PROCESSING_SOLD_ITEMS_COMPLETE",
                        "sold_items_dict": sold_items_dict,
                        "query_hash": query_hash  # Add query_hash here
                    })
                else:
                      PRICE_publish_to_queue({
                        "type": "PROCESSING_SOLD_ITEMS_COMPLETE",
                        "sold_items_dict": {},
                        "query_hash": query_hash  # Add query_hash here
                    })

                # Always send completion signal
                PRICE_publish_to_queue({
                    "type": "PROCESSING_SCRAPED_FILE_COMPLETE",
                    "query_hash": query_hash,
                    "product_name": data[-1]['product_name']
                })
            except Exception as db_error:
                db_success = False
                print(chalk.red(f"DB operation failed: {db_error}"))
                raise  # Re-raise the exception to be caught by outer try block

    except Exception as e:
        print(chalk.red(f"Error comparing scraped data: {e}"))
        raise
def compare_driver(scraped_data_file_path,query_hash,spec_item=None):
    """
    Drives the comparison process for a given scraped data file.

    :param scraped_data_file_path: Path to the scraped data file.
    """


    try:
        
        res = parse_file_name(scraped_data_file_path)
        brand = (res['brand']).upper()
        source = (res['source']).upper()

        #intial msg for new query
        PRICE_publish_to_queue({
            "type":"NEW_QUERY",
            "source_file":scraped_data_file_path})

        #only get db records that match this spec item
        db_data = DB_fetch_product_ids_prices_dates(brand,source,spec_item)

        #convert retrieved db data to dictionary
        existing_product_ids_prices_dict = db_data_to_dictionary(db_data)
        print(existing_product_ids_prices_dict)
        
        compare_scraped_data_to_db(scraped_data_file_path,existing_product_ids_prices_dict,source,query_hash,spec_item)

    except Exception as e:
        print(chalk.red(f"Error in comparison driver: {e}"))
        raise



if __name__ == "__main__":
    
   
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    scrape_data_dir = os.path.join('src','scrape_file_output')
    print(scrape_data_dir)



    fitlered_file_1 = "/home/ubuntu/Documents/Projects/TDD-scraper/src/scrape_file_output/filtered/FILTERED_PRADA_2024-05-11_BAGS_662584ce/FILTERED_ITALIST_PRADA_2024-05-11_BAGS_662584ce.csv"
    
    filtered_file_2 = "/home/ubuntu/Documents/Projects/TDD-scraper/src/scrape_file_output/filtered/FILTERED_PRADA_2024-05-11_BAGS_bdfe5561/FILTERED_ITALIST_PRADA_2024-05-11_BAGS_bdfe5561.csv"
    
 
    compare_driver(fitlered_file_1,'662548ce','EMBROIDERED FABRIC SMALL SYMBOLE SHOPPING BAG')

    PRICE_publish_to_queue({"type":"PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY","email":"(main)balmanzar883@gmail.com",'brand':'PRADA','category':'BAGS','query_hash':'662548ce'})
    
    compare_driver(filtered_file_2,'bdfe5561','TOTE')

    PRICE_publish_to_queue({"type":"PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY","email":"(main)balmanzar883@gmail.com",'brand':'PRADA','category':'BAGS','query_hash':'bdfe5561'})
    

    
