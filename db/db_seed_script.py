"""
Script to seed database with initial product data from CSV files.
Reads raw scrape data and inserts into PostgreSQL database.
"""

# Standard library imports
import psycopg2
import os
import sys
import csv 
from datetime import datetime

# Local application imports
from connection import get_db_connection

# Setup path for importing from parent directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from queries import insertion_queries 

# Database connection setup
conn = get_db_connection()
cur = conn.cursor()

# File path setup
curr_dir = os.path.dirname(os.path.abspath(__file__))
file_output_dir_root = os.path.join(curr_dir, '..', 'src', 'scrape_file_output')
raw_root_dir = os.path.join(file_output_dir_root, 'raw')

# Path to sample data file
sample_seed_data_path = os.path.join(
    raw_root_dir,
    'RAW_SCRAPE_prada_2024-23-10_bags_3d1a448c',
    'RAW_ITALIST_prada_2024-23-10_bags_3d1a448c.csv'
)


def process_csv_file(file_path,cur,conn):

    """
    Process a single CSV file and insert its data into the database.
    
    Args:
        file_path: Path to CSV file
        cur: Database cursor
        conn: Database connection
    """

    try:
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            
            # Extract scrape date
            scrape_date = next(csv_reader)
            scrape_date_str = ((scrape_date[0]).split(':'))[1].strip()
            scrape_date_obj = datetime.strptime(scrape_date_str, '%Y-%d-%m').date()
            print(f"Processing file with scrape date: {scrape_date_obj}")
            
            # Skip header lines
            for _ in range(1, 4):
                next(csv_reader)
            
            # Process each product row
            for row in csv_reader:
                product_data = {
                    'product_id': row[0].upper(),
                    'brand': row[1].upper(),
                    'product_name': row[2].upper(),
                    'curr_price': row[3],
                    'prev_price': row[3],
                    'curr_scrape_date': scrape_date_obj,
                    'prev_scrape_date': scrape_date_obj,
                    'sold_date': None,
                    'sold': False,
                    'listing_url': row[4],
                    'source': row[5]
                }
                
                cur.execute(
                    insertion_queries.PRODUCT_INSERT_QUERY,
                    (product_data['product_id'],
                     product_data['brand'],
                     product_data['product_name'],
                     product_data['curr_price'],
                     product_data['curr_scrape_date'],
                     product_data['prev_price'],
                     product_data['prev_scrape_date'],
                     product_data['sold_date'],
                     product_data['sold'],
                     product_data['listing_url'],
                     product_data['source'])
                )
                conn.commit()
                
            print(f"Completed processing file: {os.path.basename(file_path)}")
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        conn.rollback()



def process_raw_directory(raw_dir_path):
    """
    Process all CSV files in all subdirectories of the raw directory.
    
    Args:
        raw_dir_path: Path to raw directory containing scrape subdirectories
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Walk through all subdirectories
        for root, dirs, files in os.walk(raw_dir_path):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    print(f"\nProcessing: {file_path}")
                    process_csv_file(file_path, cur, conn)
    
    except Exception as e:
        print(f"Error processing directory {raw_dir_path}: {str(e)}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Setup paths
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir_path = os.path.join(curr_dir, '..', 'src', 'scrape_file_output', 'raw')
    
    print(f"Starting to process files in: {raw_dir_path}")
    process_raw_directory(raw_dir_path)
    print("Completed processing all files")