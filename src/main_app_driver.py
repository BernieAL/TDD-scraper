# src/main_app_driver.py
import os
import sys
from datetime import datetime

# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

curr_dir = os.path.dirname(os.path.abspath(__file__))
user_query_data_file = os.path.join(curr_dir, 'input_data', 'search_criteria.csv')
scraped_data_dir_raw = os.path.join(curr_dir, 'file_output','raw')
scraped_data_dir_filtered = os.path.join(curr_dir, 'file_output','filtered')

# Import the ScraperUtils class from utils
from utils.ScraperUtils import ScraperUtils

# Import the scraper classes to construct instances of 
from src.scrapers.italist_scraper import ItalistScraper

#import comparision function
from Analysis.compare_data import compare_driver

# Initialize the ScraperUtils instance
utils = ScraperUtils(scraped_data_dir_raw,scraped_data_dir_filtered)




# def run_scrapers(output_subdir, brand, query, specific_item):
#     try:
#         italist_driver(output_subdir, brand, query, specific_item, True)
#     except ImportError:
#         raise ImportError("italist_driver scraper could not be imported")
#     except Exception as e:
#         print(f"Error while running italist scraper: {e}")

def driver_function():
    
    with open(user_query_data_file, 'r', newline='', encoding='utf-8') as file:
        #skip first line oh headers
        next(file)
        for row in file:

            if not row:
                continue

            tokens = row.split(',')

            brand = tokens[0]
            query = tokens[1]
            
            #if row doesnt have specific item, set spec item as None
            specific_item = tokens[2] if len(tokens) > 2 else None

            output_dir = utils.make_scraped_sub_dir_raw(brand, query)
            # print(output_dir)
            italist_scraper = ItalistScraper(brand,query,output_dir,True)
            scraped_file = italist_scraper.run()
            # print(scraped_file)

            
            if specific_item != None:
                fitlered_sub_dir = utils.make_filtered_sub_dir(brand,query,specific_item)
                fitlered_file = (utils.filter_specific(scraped_file,specific_item,fitlered_sub_dir))
                compare_driver(fitlered_file)
            else:
                compare_driver(scraped_file)


# Example usage in your code
if __name__ == "__main__":
    brand = 'Prada'
    query = 'Bags'
    # output_dir = utils.make_scraped_sub_dir(brand, query)  # Call the method from ScraperUtils
    driver_function()
    # run_scrapers(output_dir, brand, query, None)
