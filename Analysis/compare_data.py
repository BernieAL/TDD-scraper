
import os,sys,csv
import psycopg2
from datetime import datetime

#get parent dir  from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(parent_dir)

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

#now import will work
from db.db_utils import fetch_product_ids,fetch_product_ids_and_prices


#get path to current file
curr_dir = os.path.dirname(os.path.abspath(__file__))

#use path to curr dir to build path to file_output dir
file_output_dir = os.path.join(curr_dir,'..','src','file_output')
print(os.path.isdir(file_output_dir))

test_input_file = os.path.join(file_output_dir,'italist_2024-17-09_prada_bags.csv')





#1 - get all existing product_ids from db
# existing_product_ids = set(fetch_product_ids('Prada'))
existing_product_ids_and_prices = fetch_product_ids_and_prices('Prada')
print(existing_product_ids_and_prices)
#convert from list of dicts to dict
existing_product_id_prices_dict = {}
for e in existing_product_ids_and_prices:
    product_id = e['product_id']
    last_price = float(e['last_price'])
    existing_product_id_prices_dict[product_id] = last_price


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

    #switch to csv.DictReader for rest of row for reading product data
    csv_reader = csv.DictReader(file)
    # Iterate over each row and print product data
    for row in csv_reader:
        print(f"ID: {row['Product_ID']}, "
            f"Brand: {row['Brand']}, "
            f"Product: {row['Product_Name']}, "
            f"Curr Price: {row['Curr_Price']}, "
            f"Curr_Scrape_Date: {row['Curr_Scrape_Date']}, "
            f"Prev Price: {row['Prev_Date']}, "
            f"Prev_Scrape_Date: {row['Prev_Scrape_Date']}"
            )
      
        #check if scraped product is in db
        if row['Product_ID'] in existing_product_id_prices_dict:
            print(f"product found in db")
        
            #update price and scrape date from scrape data into db by default - even if price is the same 
            #this simplifies logic in upudate function by not having to check if new price is populated or not.
            temp = {
                'product_id':row['product_id'],
                'last_scrape_date': scrape_date,
                'last_price':row['Price'],
            }
            updated_products.push(temp)
            print(f"updated product")
            #mark as seen by popping 'seen' product from db dictionary - what remains are db items thaat are no longer on website anymore - meaning they are sold
            existing_product_id_prices_dict.pop(product_id)
            print(f"product popped")
            
           

        else:
            temp = {
                'product_id':row['product_id'],
                'brand':row['Brand'],
                # 'product_name':row['Product_Name'],
                'last_scrape_date': scrape_date,
                'last_price':row['Price'],
            }
            #if no, push product to new_products array to be bulk inserted to db later
            new_products.push(temp)

            
print(existing_product_id_prices_dict)


#4 - bulk insert new products

#5 - bulk update price changes

#6 - mark items that remain in existing_product_ids as sold

        


