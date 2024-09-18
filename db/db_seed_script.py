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

#go up one dir 
file_output_dir = os.path.join(curr_dir,'..','src','file_output')
# print(os.path.isdir(file_output_dir))


sample_file_path = os.path.join(file_output_dir,'italist_2024-17-09_prada_bags.csv')

with open(sample_file_path,'r') as file:
    csv_reader = csv.reader(file)


    #get scraped date from first line of csv file
    scrape_date = next(csv_reader)
    scrape_date_str = ((scrape_date[0]).split(':'))[1].strip()
    scrape_date_obj = datetime.strptime(scrape_date_str,'%Y-%d-%m').date()
    print(scrape_date_obj)

    #skip the fi header lines in file
    for i in range(1,4):
     next(csv_reader)
    

    for row in csv_reader:
       

        brand = row[0]
        product = row[1]
        price = row[2].split()[1] #extract price from string Ex. "USD 1195"
        product_id = row[3]
        
        cur.execute(insertion_queries.PRODUCT_INSERT_QUERY,(product_id,scrape_date_obj,price))
        conn.commit
        #insert into db
        
        # print(f"{product_id},{brand},{product},{price}")

        