# src/utils/scraper_utils.py

import os
import hashlib
from datetime import datetime

class ScraperUtils:
    
    def __init__(self, scraped_data_dir_raw):
        self.scraped_data_dir_raw = scraped_data_dir_raw

    def generate_hash(self, query, date):
        combined_str = f"{query}_{date}"
        return hashlib.sha256(combined_str.encode()).hexdigest()[:8]

    #make subdir in file_output/raw for current search query
    def make_scraped_sub_dir_raw(self, brand, query):
        current_date = datetime.now().strftime('%Y-%d-%m')
        try:
            query_hash = self.generate_hash(query, current_date)
            dir_name = f"RAW_SCRAPE_{brand}_{current_date}_{query}_{query_hash}"
            new_sub_dir = os.path.join(self.scraped_data_dir_raw, dir_name)
            
            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
            return new_sub_dir
        
        except Exception as e:
            print(f"Error while creating sub-directory for raw scrape: {e}")
            return None


    def make_filtered_sub_dir(self, brand, query):

        current_date = datetime.now().strftime('%Y-%d-%m') 
        
        try:
            query_hash = self.generate_hash(query, current_date)
            dir_name = f"FILTERED_{brand}_{current_date}_{query}_{query_hash}"
            new_sub_dir = os.path.join(self.scraped_data_dir,dir_name)
            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
            return new_sub_dir
        except Exception as e:
             print(f"Error while creating filtered sub-directory for raw scrape: {e}")
             return None

