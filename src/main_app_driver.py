# src/main_app_driver.py
import os
import sys
from datetime import datetime

# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

curr_dir = os.path.dirname(os.path.abspath(__file__))
user_category_data_file = os.path.join(curr_dir, 'input_data', 'search_criteria.csv')
scraped_data_dir_raw = os.path.join(curr_dir, 'scrape_file_output','raw')
scraped_data_dir_filtered = os.path.join(curr_dir, 'scrape_file_output','filtered')

# Import the ScraperUtils class from utils
from utils.ScraperUtils import ScraperUtils

# Import the scraper classes to construct instances of 
from src.scrapers.italist_scraper import ItalistScraper

#import comparision function
from Analysis.compare_data import compare_driver


from rbmq.price_change_producer import publish_to_queue


# Initialize the ScraperUtils instance
utils = ScraperUtils(scraped_data_dir_raw,scraped_data_dir_filtered)




# def run_scrapers(output_subdir, brand, category, specific_item):
#     try:
#         italist_driver(output_subdir, brand, category, specific_item, True)
#     except ImportError:
#         raise ImportError("italist_driver scraper could not be imported")
#     except Exception as e:
#         print(f"Error while running italist scraper: {e}")

def driver_function():
    
    current_date = datetime.now().strftime('%Y-%d-%m')


    with open(user_category_data_file, 'r', newline='', encoding='utf-8') as file:
        #skip first line oh headers
        next(file)
        for row in file:

            if not row:
                continue
            try:

                tokens = row.split(',')

                brand = tokens[0]
                category = tokens[1] #bags, shirts, etc
                
                query = f"{brand}_{category}" #Prada_bags , Gucci_shirts
            
                #if row doesnt have specific item, set spec item as None
                specific_item = tokens[2] if len(tokens) > 2 else None

                query_hash = utils.generate_hash(query,specific_item,current_date)

                output_dir = utils.make_scraped_sub_dir_raw(brand,category,query_hash)
                print(output_dir)
                italist_scraper = ItalistScraper(brand,category,output_dir,query_hash,True)
                scraped_file = italist_scraper.run()
                print(scraped_file)

                
                # if specific_item != None:
                #     fitlered_sub_dir = utils.make_filtered_sub_dir(brand,category,specific_item,query_hash)
                #     fitlered_file = (utils.filter_specific(scraped_file,specific_item,fitlered_sub_dir))
                #     compare_driver(fitlered_file)
                # else:
                #     compare_driver(scraped_file)
            except Exception as e:
                print(f"main_app_driver failure {e}")
        # publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR category","email":"balmanzar883@gmail.com"})
        
# Example usage in your code
if __name__ == "__main__":
    # brand = 'Prada'
    
    # category = 'Bags'
    # output_dir = utils.make_scraped_sub_dir(brand, category)  # Call the method from ScraperUtils
    driver_function()
    # run_scrapers(output_dir, brand, category, None)
