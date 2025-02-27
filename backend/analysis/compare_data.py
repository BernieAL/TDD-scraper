
import os
import sys
import csv
import json
import psycopg2
from datetime import datetime,date
from simple_chalk import chalk

#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    
# For Docker
if os.getenv('RUNNING_IN_DOCKER') == '1' and '/app' not in sys.path:
    sys.path.insert(0, '/app')




FILE_SCRAPE_DATE = None


from db.db_utils import (
    DB_fetch_product_ids_prices_dates, 
    DB_bulk_update_existing, 
    DB_bulk_insert_new,
    DB_bulk_update_sold,
    DB_get_sold_daily
)
from rbmq.price_change_producer import PRICE_publish_to_queue

def parse_file_name(file):
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
        'brand': brand,
        'source': source,
    }

def db_data_to_dictionary(existing_db_data_list):
    try:
        existing_db_data_dict = {}
        for prod in existing_db_data_list:
            product_id = prod['product_id'] 
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

def process_existing_product(row, existing_product_data_dict, updated_products, scrape_date, input_file, source, paths):
    product_data = existing_product_data_dict[row['product_id']]
    
    current_file_price = round(float(row['curr_price']), 2)
    current_db_price = round(product_data['curr_price'], 2)
    
    print(chalk.blue(f"Comparing prices for {row['product_id']}:"))
    print(chalk.blue(f"  File price: {current_file_price} ({type(current_file_price)})"))
    print(chalk.blue(f"  DB price: {current_db_price} ({type(current_db_price)})"))
    
    if current_file_price != current_db_price:
        print(chalk.yellow(f"Price change detected: {current_db_price} -> {current_file_price}"))
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

        PRICE_publish_to_queue({
            'type': 'PRODUCT_PRICE_CHANGE',
            'product_name': row['product_name'],
            **temp,
            'listing_url': row['listing_url'],
            'source': source,
            'paths': paths
        })
        print(chalk.yellow(f"Published price change for {temp['product_id']}"))
    else:
        print(chalk.blue(f"No price change needed for {row['product_id']}"))
        product_data['prev_scrape_date'] = product_data['curr_scrape_date']
        product_data['curr_scrape_date'] = scrape_date
        updated_products.append({
            'product_id': row['product_id'],
            'curr_price': current_db_price,
            'curr_scrape_date': scrape_date,
            'prev_price': product_data['prev_price'],
            'prev_scrape_date': product_data['prev_scrape_date']
        })

    print(chalk.yellow(f"Updated product: {row['product_id']}"))
    existing_product_data_dict.pop(row['product_id'])

def process_new_product(row, scrape_date, new_products, source):
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
        'listing_url': row['listing_url'],
        'source': row['source']
    }
    new_products.append(temp)
    print(chalk.green(f"New product added: {row['product_id']}"))

def compare_scraped_data_to_db(input_file, existing_product_data_dict, source, query_hash, spec_item=None, paths=None):
    try:
        if spec_item is not None:
            print(chalk.yellow(f"PROCESSING_FILTERED_QUERY - SPEC ITEM {spec_item}"))
        else:
            print(chalk.yellow(f"PROCESSING_GENERAL_QUERY - NO SPEC ITEM"))

        updated_products = []
        new_products = []
        scrape_date = None


        with open(input_file, mode='r') as file:
            csv_reader = csv.reader(file)

            #get scrape date
            scrape_date_line = next(csv_reader)
            scrape_date = scrape_date_line[0].split(':')[1].strip()
            scrape_date = datetime.strptime(scrape_date, '%Y-%d-%m').date()
            global FILE_SCRAPE_DATE
            FILE_SCRAPE_DATE = scrape_date
            print(f"Scrape Date: {scrape_date}")

            #get query line 
            query_line = next(csv_reader)
            query = query_line[0].split(':')[1].strip()
            print(f"Query: {query}")

            #get csv headers
            headers = next(csv_reader)
            next(csv_reader)
            csv_reader = csv.DictReader(file, fieldnames=headers)
            data = list(csv_reader)
            
            if not data:
                print(chalk.yellow("Scraped File is empty (no data rows)"))
                PRICE_publish_to_queue({
                    "type": "PROCESSING_SOLD_ITEMS_COMPLETE",
                    "sold_items_dict": {},
                    "query_hash": query_hash,
                    "no_results_reason": "No items found matching your search criteria.",
                    "paths": paths
                })
                PRICE_publish_to_queue({
                    "type": "PROCESSING_SCRAPED_FILE_COMPLETE",
                    "query_hash": query_hash,
                    "product_name": spec_item,
                    "scrape_file_empty": True,
                    "source": source,
                    "empty_file_details": {
                        "source": source,
                        "search_term": spec_item,
                        "possible_reasons": [
                            "Item not available",
                            "Search terms may need adjustment",
                            "Technical issue"
                        ]
                    },
                    "paths": paths
                })
                return

            for row in data:
                print(chalk.red(f"ID: {row['product_id']}, Brand: {row['brand']}, Product: {row['product_name']}, Curr Price: {row['curr_price']}"))
                if row['product_id'] in existing_product_data_dict:
                    process_existing_product(row, existing_product_data_dict, updated_products, scrape_date, input_file, source, paths)
                else:
                    process_new_product(row, scrape_date, new_products, source)

            db_success = True
            try:
                #if db op failed
                if updated_products and not DB_bulk_update_existing(updated_products):
                    raise Exception("DB update operation failed")

                #if db op failed
                if new_products and not DB_bulk_insert_new(new_products):
                    raise Exception("DB insert operation failed")

                #else db ops successful
                print(chalk.green("[SUCCESS] DB operations were successful."))

                #remaining items in dict are SOLD - they were not found in curr date scrape - if found, they were popped
                items_not_found = existing_product_data_dict
                print(chalk.cyan(f"db items not found in current scrape file {items_not_found}"))

                #if items_not_found not empty, mark items as sold in db
                if items_not_found:
                    
                    #if db op failed
                    if not DB_bulk_update_sold(items_not_found):
                        raise Exception("Sold status update failed")

                    #get sold items that match curr query date ONLY
                    sold_items_dict = DB_get_sold_daily(source, items_not_found, FILE_SCRAPE_DATE, spec_item)
                    print(chalk.green(f"SOLD ITEMS: {sold_items_dict}\n ---------------"))
                    
                    #publish sold items PRICE queue
                    PRICE_publish_to_queue({
                        "type": "PROCESSING_SOLD_ITEMS_COMPLETE",
                        "sold_items_dict": sold_items_dict,
                        "query_hash": query_hash,
                        "paths": paths
                    })

                #no sold items, publish empty dict to PRICE queue
                else:
                    PRICE_publish_to_queue({
                        "type": "PROCESSING_SOLD_ITEMS_COMPLETE",
                        "sold_items_dict": {},
                        "query_hash": query_hash,
                        "paths": paths
                    })

                #processing of sold and price changes for this file are complete
                PRICE_publish_to_queue({
                    "type": "PROCESSING_SCRAPED_FILE_COMPLETE",
                    "query_hash": query_hash,
                    "product_name": data[-1]['product_name'],
                    "paths": paths
                })

        
            except Exception as db_error:
                db_success = False
                print(chalk.red(f"DB operation failed: {db_error}"))
                raise

    except Exception as e:
        print(chalk.red(f"Error comparing scraped data: {e}"))
        raise

def compare_driver(scraped_data_file_path, query_hash, msg, spec_item=None):
    try:
        res = parse_file_name(scraped_data_file_path)
        brand = (res['brand']).upper()
        source = (res['source']).upper()
        paths = msg.get('paths', {})

        PRICE_publish_to_queue({
            "type": "NEW_QUERY",
            "source_file": scraped_data_file_path,
            "paths": paths,
            "spec_item":spec_item
        })

        db_data = DB_fetch_product_ids_prices_dates(brand, source, spec_item)
        existing_product_ids_prices_dict = db_data_to_dictionary(db_data)
        print(existing_product_ids_prices_dict)
        
        compare_scraped_data_to_db(scraped_data_file_path, existing_product_ids_prices_dict, source, query_hash, spec_item, paths)

    except Exception as e:
        print(chalk.red(f"Error in comparison driver: {e}"))
        raise
