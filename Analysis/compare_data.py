
import os,sys,csv,json
import psycopg2
from datetime import datetime
from simple_chalk import chalk

#get parent dir  from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(parent_dir)

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

#now import will work
from db.db_utils import DB_fetch_product_ids_prices_dates, DB_bulk_update_existing, bulk_insert_new,DB_bulk_update_sold
from rbmq.price_change_producer import publish_to_queue

#get path to current file
curr_dir = os.path.dirname(os.path.abspath(__file__))

#use path to curr dir to build path to file_output dir
file_output_dir = os.path.join(curr_dir,'..','src','file_output')
print(os.path.isdir(file_output_dir))

input_file = os.path.join(file_output_dir,'italist_2024-26-09_prada_bags.csv')



#1 - get all existing product_ids from db
existing_product_ids_prices_dates_list = DB_fetch_product_ids_prices_dates('Prada') #returns list of dicts
# print(existing_product_ids_prices_dates)




"""
existing_product_id_prices_dates pre-loads all product ids, prices, and dates
"""
def db_data_to_dictionary(existing_db_data_list):


    """
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

    existing_db_data_dict = {}

    for prod in existing_db_data_list:
        product_id = prod['product_id'] 
        curr_price = float(prod['curr_price'])
        curr_scrape_date = prod['curr_scrape_date']
        prev_price = float(prod['prev_price'])
        prev_scrape_date = prod['prev_scrape_date']
        
    

        existing_db_data_dict[product_id] = {'curr_price':curr_price,
                                                    'curr_scrape_date':curr_scrape_date,
                                                    'prev_price':prev_price,
                                                    'prev_scrape_date':prev_scrape_date,
                                                    }
        
    return existing_db_data_dict 

#2 - read in scraped data and compare these to db product_ids to scraped product_ids
def compare_scraped_data_to_db(existing_product_data_dict):
    updated_products = []
    new_products = []
    scraped_data = []
    with open(input_file,mode='r') as file:
        
        #standard csv reader to skip first lines (meta data) which is csv scrape data and query info
        csv_reader = csv.reader(file)

        #get scrape data from first line
        scrape_date_line = next(csv_reader)
        scrape_date = scrape_date_line[0].split(':')[1].strip()
        scrape_date = datetime.strptime(scrape_date,'%Y-%d-%m').date()
        print("Scrape Date:", scrape_date)

        #get query from second line
        query_line = next(csv_reader)
        query = query_line[0].split(':')[1].strip()
        print("Query:",query)

        #gets header and goes to next line
        headers = next(csv_reader)  
        #skip "------- in csv"
        next(csv_reader)
    
        #switch to csv.DictReader for rest of row for reading product data
        csv_reader = csv.DictReader(file,fieldnames=headers)
        # Iterate over each row and print product data
        for row in csv_reader:
            
            print(chalk.red(f"ID: {row['product_id']}, "
                f"Brand: {row['brand']}, "
                f"Product: {row['product_name']},"
                f"Curr Price: {row['curr_price']},"))
        
            #check if scraped product is in db
            if row['product_id'] in existing_product_data_dict:
                print(f"product found in db")
                print(existing_product_data_dict[row['product_id']]['curr_price'])

                #if scraped price != db curr price, prev = curr , curr = scraped price
                if row['curr_price'] != existing_product_data_dict[row['product_id']]['curr_price']:
                    
                    #update prev to be curr before reassigning curr to scraped price
                    existing_product_data_dict[row['product_id']]['prev_price'] = existing_product_data_dict[row['product_id']]['curr_price']
                    
                    #update curr to be scraped price 
                    existing_product_data_dict[row['product_id']]['curr_price'] = row['curr_price']

                    #update dates
                    existing_product_data_dict[row['product_id']]['prev_scrape_date'] = existing_product_data_dict[row['product_id']]['curr_scrape_date']
                    existing_product_data_dict[row['product_id']]['curr_scrape_date'] = scrape_date

                    temp = {
                        'product_id':row['product_id'],               
                        'curr_price':float(row['curr_price']),
                        'curr_scrape_date': scrape_date,
                        'prev_price': existing_product_data_dict[row['product_id']]['prev_price'],
                        'prev_scrape_date': existing_product_data_dict[row['product_id']]['prev_scrape_date']
                    }

                    #add to updated_products list 
                    updated_products.append(temp)
                    
                    #publish product to queue for analysis
                    #temporary workaround  to include product_name in obj for brevity
                    temp2_w_product_name = {
                            'product_name':row['product_name'],
                            'product_id':row['product_id'],               
                            'curr_price':float(row['curr_price']),
                            'curr_scrape_date': scrape_date,
                            'prev_price': existing_product_data_dict[row['product_id']]['prev_price'],
                            'prev_scrape_date': existing_product_data_dict[row['product_id']]['prev_scrape_date'],
                            'listing_url':row['listing_url']
                        }
                    publish_to_queue(temp2_w_product_name)

                #if no change in prices - only update the dates
                else:
                    existing_product_data_dict[row['product_id']]['prev_scrape_date'] = existing_product_data_dict[row['product_id']]['curr_scrape_date']
                    existing_product_data_dict[row['product_id']]['curr_scrape_date'] = scrape_date
                    
                    #update price and scrape date from scrape data into db by default - even if price is the same 
                    #this simplifies logic in upudate function by not having to check if new price is populated or not.
                    temp = {
                        'product_id':row['product_id'],               
                        'curr_price':float(row['curr_price']),
                        'curr_scrape_date': scrape_date,
                        'prev_price': existing_product_data_dict[row['product_id']]['prev_price'],
                        'prev_scrape_date': existing_product_data_dict[row['product_id']]['prev_scrape_date']
                    }
                
                    updated_products.append(temp)

                print(chalk.yellow(f"updated product {temp}"))
                #mark as seen by popping 'seen' product from db dictionary - what remains are db items thaat are no longer on website anymore - meaning they are sold
                existing_product_data_dict.pop(row['product_id'])
                print(chalk.red(f"product popped - marked as seen"))
                
            
            #if scraped product not in db - this is a new product
            else:
                temp = {
                    'product_id':row['product_id'],
                    'brand':row['brand'],
                    'product_name':row['product_name'],
                    'curr_price':float(row['curr_price']),
                    'curr_scrape_date': scrape_date,
                    'prev_price': float(row['curr_price']),  #no prev price, use curr as initial val
                    'prev_scrape_date':scrape_date,  #no prev scrape date, use curr date as initial val
                    'sold_date':None,
                    'sold':False

                }
                print(chalk.green(f"new product to be added {temp}"))
                #if no, push product to new_products array to be bulk inserted to db later
                new_products.append(temp)

    # #empty check - if any values left, these were not found in scraped data, meaning they are sold           
    # print(f" sold items - {existing_product_id_prices_dict}")

    # # #4 - bulk update existing products
    # DB_bulk_update_existing(updated_products)
    # # print(f"items to be updated {updated_products}")

    # # 5- bulk insert new products
    # # print(f"new items to insert {new_products}")
    # bulk_insert_new(new_products)

    # #6 - mark items that remain in existing_product_ids as sold
    # DB_bulk_update_sold(existing_product_data_dict)

    #after processing all products - send completing signal to queue
    publish_to_queue({"type": "BATCH_COMPLETE", "source_file": input_file})
        


def compare_driver(input_file_path):

    """
    pass indiv file path?
    """



    existing_product_ids_prices_dict = db_data_to_dictionary(existing_product_ids_prices_dates_list)
    compare_scraped_data_to_db(existing_product_ids_prices_dict)

if __name__ == "__main__":
    
    
    
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    scrape_output_dir = os.path.join('src','file_output')
    input_file_path = os.path.join(curr_dir,'..',scrape_output_dir,'italist_2024-26-09_prada_bags.csv')
    # input_file_path = os.path.join(curr_dir,'..','src','file_output','italist_2024-30-09_prada_bags.csv')
    # print(os.path.isfile(input_file_path))
    compare_driver(input_file_path)