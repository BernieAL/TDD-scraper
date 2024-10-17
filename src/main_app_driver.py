# src/main_app_driver.py
import os,csv,sys
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


def scrape_process(brand,category,specific_item):
        
        
        # Initialize the variables
        scraped_file = None
        filtered_file = None
        current_date = datetime.now().strftime('%Y-%d-%m')
     
        
        query = f"{brand}_{category}" #Prada_bags , Gucci_shirts
    
             

        query_hash = utils.generate_hash(query,specific_item,current_date)

        output_dir = utils.make_scraped_sub_dir_raw(brand,category,query_hash)
        print(output_dir)
        italist_scraper = ItalistScraper(brand,category,output_dir,query_hash,False)
        scraped_file = italist_scraper.run()
        print(scraped_file)

      
        
        if specific_item != None:
            filtered_sub_dir = utils.make_filtered_sub_dir(brand,category,scraped_data_dir_filtered,query_hash)
            filtered_file = (utils.filter_specific(scraped_file,specific_item,filtered_sub_dir,query_hash))
            compare_driver(filtered_file)

            # # #manual testing price change
            # filtered_file = os.path.join(scraped_data_dir_filtered,'FILTERED_prada_2024-14-10_bags_f3f28ac8','FILTERED_italist_prada_2024-14-10_bags_f3f28ac8.csv')
            # compare_driver(filtered_file)
        else:
            
            compare_driver(scraped_file)
            
            # #manual testing price change
            # scraped_file = os.path.join(scraped_data_dir_raw,'RAW_SCRAPE_prada_2024-14-10_bags_f3f28ac8','RAW_italist_prada_2024-14-10_bags_f3f28ac8.csv')
            # compare_driver(scraped_file)

        return filtered_file if not scraped_file else scraped_file

def driver_function():
    
    
    with open(user_category_data_file, 'r', newline='', encoding='utf-8') as file:
        

        csv_reader = csv.reader(file)
        #skip first line - file headers
        next(csv_reader)
        
        for file_row in csv_reader:
            
            #if file_row empty, continue
            if not file_row or not any(file_row):
                continue
            
            try:

                #extract brand,category, and spec item

                brand = file_row[0].strip()
                category = file_row[1].strip()

                #if file doesnt have spec item , use None
                specific_item = file_row[2].strip() if len(file_row) > 2 else None
                output_file = scrape_process(brand, category, specific_item)

                #if filtered file use that, if not use scraped file as source file - queue MUST recieve a source file to parse from and build price report subdir
                publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR QUERY","email":"balmanzar883@gmail.com","source_file":output_file})
           
            except Exception as e:
                print(f"scrape_process failure {e}")

        
        print("processed all rows in input file")
# Example usage in your code
if __name__ == "__main__":
    # brand = 'Prada'
    
    # category = 'Bags'
    # output_dir = utils.make_scraped_sub_dir(brand, category)  # Call the method from ScraperUtils
    driver_function()
    # run_scrapers(output_dir, brand, category, None)
