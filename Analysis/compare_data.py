
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
from db.db_utils import fetch_product_ids_prices_dates, bulk_update_existing, bulk_insert_new,bulk_update_sold


#get path to current file
curr_dir = os.path.dirname(os.path.abspath(__file__))

#use path to curr dir to build path to file_output dir
file_output_dir = os.path.join(curr_dir,'..','src','file_output')
print(os.path.isdir(file_output_dir))

test_input_file = os.path.join(file_output_dir,'italist_2024-26-09_prada_bags.csv')





#1 - get all existing product_ids from db
# existing_product_ids = set(fetch_product_ids('Prada'))
existing_product_ids_prices_dates = fetch_product_ids_prices_dates('Prada')
# print(existing_product_ids_and_prices)
#convert from list of dicts to dict
existing_product_id_prices_dict = {}

"""

existing_product_id_prices_dict pre-loads all product ids, prices, and dates

Each product id is key of its own dictionary, where the values are in another dictinary themselves

Ex.
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
for prod in existing_product_ids_prices_dates:
    product_id = prod['product_id'] 
    curr_price = float(prod['curr_price'])
    curr_scrape_date = prod['curr_scrape_date']
    prev_price = float(prod['prev_price'])
    prev_scrape_date = prod['prev_scrape_date']
    
 

    existing_product_id_prices_dict[product_id] = {'curr_price':curr_price,
                                                   'curr_scrape_date':curr_scrape_date,
                                                   'prev_price':prev_price,
                                                   'prev_scrape_date':prev_scrape_date,
                                                   }

#2 - read in scraped data and compare these to db product_ids to scraped product_ids
updated_products = []
new_products = []


scraped_data = []
with open(test_input_file,mode='r') as file:
    
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
        if row['product_id'] in existing_product_id_prices_dict:
            print(f"product found in db")
            print(existing_product_id_prices_dict[row['product_id']]['curr_price'])

            #if scraped price != db curr price, prev = curr , curr = scraped price
            if row['curr_price'] != existing_product_id_prices_dict[row['product_id']]['curr_price']:
                   
                   #update prev to be curr before reassigning curr to scraped price
                   existing_product_id_prices_dict[row['product_id']]['prev_price'] = existing_product_id_prices_dict[row['product_id']]['curr_price']
                   
                   #update curr to be scraped price 
                   existing_product_id_prices_dict[row['product_id']]['curr_price'] = row['curr_price']

                   #update dates
                   existing_product_id_prices_dict[row['product_id']]['prev_scrape_date'] = existing_product_id_prices_dict[row['product_id']]['curr_scrape_date']
                   existing_product_id_prices_dict[row['product_id']]['curr_scrape_date'] = scrape_date

                   temp = {
                    'product_id':row['product_id'],               
                    'curr_price':row['curr_price'],
                    'curr_scrape_date': scrape_date,
                    'prev_price': existing_product_id_prices_dict[row['product_id']]['prev_price'],
                    'prev_scrape_date': existing_product_id_prices_dict[row['product_id']]['prev_scrape_date']
                   }

                    
                   updated_products.append(temp)

            #if no change in prices - only update the dates
            else:
                existing_product_id_prices_dict[row['product_id']]['prev_scrape_date'] = existing_product_id_prices_dict[row['product_id']]['curr_scrape_date']
                existing_product_id_prices_dict[row['product_id']]['curr_scrape_date'] = scrape_date
                
                #update price and scrape date from scrape data into db by default - even if price is the same 
                #this simplifies logic in upudate function by not having to check if new price is populated or not.
                temp = {
                    'product_id':row['product_id'],               
                    'curr_price':row['curr_price'],
                    'curr_scrape_date': scrape_date,
                    'prev_price': existing_product_id_prices_dict[row['product_id']]['prev_price'],
                    'prev_scrape_date': existing_product_id_prices_dict[row['product_id']]['prev_scrape_date']
                }
            
                updated_products.append(temp)

            print(chalk.yellow(f"updated product {temp}"))
            #mark as seen by popping 'seen' product from db dictionary - what remains are db items thaat are no longer on website anymore - meaning they are sold
            existing_product_id_prices_dict.pop(row['product_id'])
            print(chalk.red(f"product popped - marked as seen"))
            
           
        #if scraped product not in db - this is a new product
        else:
            temp = {
                'product_id':row['product_id'],
                'brand':row['brand'],
                'product_name':row['product_name'],
                'curr_price':row['curr_price'],
                'curr_scrape_date': scrape_date,
                'prev_price': row['curr_price'],  #no prev price, use curr as initial val
                'prev_scrape_date':scrape_date,  #no prev scrape date, use curr date as initial val
                'sold_date':None,
                'sold':False

            }
            print(chalk.green(f"new product to be added {temp}"))
            #if no, push product to new_products array to be bulk inserted to db later
            new_products.append(temp)

# #empty check - if any values left, these were not found in scraped data, meaning they are sold           
# print(f" sold items - {existing_product_id_prices_dict}")

# #4 - bulk update existing products
# bulk_update_existing(updated_products)
# # print(f"items to be updated {updated_products}")

# # 5- bulk insert new products
# # print(f"new items to insert {new_products}")
# bulk_insert_new(new_products)

#6 - mark items that remain in existing_product_ids as sold
bulk_update_sold(existing_product_id_prices_dict)


        


