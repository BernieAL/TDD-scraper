# src/main_app_driver.py
import os
import sys
from datetime import datetime

# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

curr_dir = os.path.dirname(os.path.abspath(__file__))
user_query_data_file = os.path.join(curr_dir, 'input_data', 'search_criteria.csv')
scraped_data_dir = os.path.join(curr_dir, 'file_output')

# Import the ScraperUtils class from utils
from utils.ScraperUtils import ScraperUtils

# Import the scraper logic
from src.scrapers.italist_scraper import italist_driver

recipient_email = "example@gmail.com"

# Initialize the ScraperUtils instance
utils = ScraperUtils(scraped_data_dir)




def run_scrapers(output_subdir, brand, query, specific_item):
    try:
        italist_driver(output_subdir, brand, query, specific_item, True)
    except ImportError:
        raise ImportError("italist_driver scraper could not be imported")
    except Exception as e:
        print(f"Error while running italist scraper: {e}")

def driver_function():
    with open(user_query_data_file, 'r', newline='', encoding='utf-8') as file:
        for row in file:

            if not row:
                continue

            tokens = row.split(',')
            
            if len(tokens) == 2:
                brand,query = tokens
                specific_item = None
            elif len(tokens) == 3:
                brand,query,specific_item = tokens   


            new_scraped_subdir = ScraperUtils.make_scraped_sub_dir(brand,query,specific_item)




            pass

# Example usage in your code
if __name__ == "__main__":
    brand = 'Prada'
    query = 'Bags'
    output_dir = utils.make_scraped_sub_dir(brand, query)  # Call the method from ScraperUtils
    run_scrapers(output_dir, brand, query, None)
