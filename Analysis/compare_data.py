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
    DB_bulk_update_sold
)
from rbmq.price_change_producer import publish_to_queue

# Directory setup for input/output
curr_dir = os.path.dirname(os.path.abspath(__file__))
file_output_dir = os.path.join(curr_dir, '..', 'src', 'file_output')


try:
    #1 - get all existing product_ids from db
    existing_product_ids_prices_dates_list = DB_fetch_product_ids_prices_dates('Prada') #returns list of dicts
    # print(existing_product_ids_prices_dates)
except Exception as e:
    print(chalk.red(f"Error fetching product data from DB: {e}"))
    sys.exit(1)




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

def compare_scraped_data_to_db(input_file, existing_product_data_dict):
    """
    Compares scraped product data to existing database records. Updates existing products, adds new products, 
    and marks products as sold if they are no longer listed.

    :param input_file: Path to the scraped data file.
    :param existing_product_data_dict: Dictionary of existing product data from the database.
    """
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
                    process_existing_product(row, existing_product_data_dict, updated_products, scrape_date,input_file)
                else:
                    process_new_product(row, scrape_date, new_products)

        # Bulk update and insert operations
        DB_bulk_update_existing(updated_products)
        bulk_insert_new(new_products)
        DB_bulk_update_sold(existing_product_data_dict)

        # After processing all products, send completion signal
        publish_to_queue({"type": "PROCESSING SCRAPED FILE COMPLETE", "source_file": input_file})

    except Exception as e:
        print(chalk.red(f"Error comparing scraped data: {e}"))
        raise


def process_existing_product(row, existing_product_data_dict, updated_products, scrape_date,input_file):
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
        updated_products.append(temp)
        # **temp unpacks all key value pairs from temp dict and adds prod_name,listing_url to it as new dict. 
        #this is like spread operator in js
        publish_to_queue({**temp, 'product_name': row['product_name'], 'listing_url': row['listing_url'],"source_file": input_file})
    else:
        # Update scrape dates if price has not changed
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
    temp = {
        'product_id': row['product_id'],
        'brand': row['brand'],
        'product_name': row['product_name'],
        'curr_price': float(row['curr_price']),
        'curr_scrape_date': scrape_date,
        'prev_price': float(row['curr_price']),
        'prev_scrape_date': scrape_date,
        'sold_date': None,
        'sold': False
    }
    new_products.append(temp)
    print(chalk.green(f"New product added: {row['product_id']}"))


def compare_driver(scraped_data_file_path):
    """
    Drives the comparison process for a given scraped data file.

    :param scraped_data_file_path: Path to the scraped data file.
    """
    try:
        existing_product_ids_prices_dict = db_data_to_dictionary(existing_product_ids_prices_dates_list)
        compare_scraped_data_to_db(scraped_data_file_path, existing_product_ids_prices_dict)
    except Exception as e:
        print(chalk.red(f"Error in comparison driver: {e}"))
        raise



if __name__ == "__main__":
    
    
    
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    scrape_data_dir_raw = os.path.join('src','scrape_file_output','raw')
    input_file_path = os.path.join(curr_dir,'..',scrape_data_dir_raw,'RAW_SCRAPE_prada_2024-14-10_bags_f3f28ac8','RAW_italist_prada_2024-14-10_bags_f3f28ac8.csv')
    # input_file_path = os.path.join(curr_dir,'..','src','file_output','italist_2024-30-09_prada_bags.csv')
    # print(os.path.isfile(input_file_path))
    compare_driver(input_file_path)

    publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR QUERY","email":"balmanzar883@gmail.com","source_file": input_file_path})