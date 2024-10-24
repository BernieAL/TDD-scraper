import psycopg2
import os,sys
import csv 
from datetime import datetime


from connection import get_db_connection

#get parent dir  from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(parent_dir)

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


from queries import insertion_queries 

conn = get_db_connection()
cur = conn.cursor()

#read from src/file_output/ and insert into db
#this is to populate db with some initial data - for testing purposes ATM

#path to current file
curr_dir = os.path.dirname(os.path.abspath(__file__))
print(curr_dir)

#go up one dir 
file_output_dir_root = os.path.join(curr_dir,'..','src','scrape_file_output')

raw_root_dir = os.path.join(file_output_dir_root,'raw')
print(raw_root_dir)


sample_seed_data_path = os.path.join(raw_root_dir,'RAW_SCRAPE_prada_2024-23-10_bags_3d1a448c','RAW_ITALIST_prada_2024-23-10_bags_3d1a448c.csv')

with open(sample_seed_data_path,'r') as file:
    csv_reader = csv.reader(file)


    #get scraped date from first line of csv file
    scrape_date = next(csv_reader)
    scrape_date_str = ((scrape_date[0]).split(':'))[1].strip()
    scrape_date_obj = datetime.strptime(scrape_date_str,'%Y-%d-%m').date()
    print(f"file scrape date - {scrape_date_obj}")

    #skip the first header lines in file
    for i in range(1,4):
     next(csv_reader)
    

    for row in csv_reader:
        """csv row structure -> product_id,brand,product_name,curr_price,listing_url,source"""
        product_id = row[0].upper()
        brand = row[1].upper()
        product_name = row[2].upper()
        # curr_price = row[3].split()[1] #extract price from string Ex. "USD 1195"
        curr_price = row[3]
        prev_price = curr_price #no prev price, use curr as initial val

        curr_scrape_date = scrape_date_obj 
        prev_scrape_date = scrape_date_obj #no prev scrape date, use curr date as initial val

        sold_date = None
        sold = False
        
        listing_url = row[4]

        source = row[5]
        
        print(f"{product_id},{brand},{product_name},{curr_price},{curr_scrape_date},{prev_price},{prev_scrape_date},{sold_date},{sold},{listing_url}.{source}")

        cur.execute(insertion_queries.PRODUCT_INSERT_QUERY,(product_id,
                                                            brand,
                                                            product_name,
                                                            curr_price,
                                                            curr_scrape_date,
                                                            prev_price,
                                                            prev_scrape_date,
                                                            sold_date,
                                                            sold,
                                                            listing_url,
                                                            source))
        conn.commit()
        #insert into db
        
        
    conn.close()

        