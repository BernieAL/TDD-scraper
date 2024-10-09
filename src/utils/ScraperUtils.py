# src/utils/scraper_utils.py

import os
import hashlib
from datetime import datetime
import pandas as pd

class ScraperUtils:
    


    #scraped_data_root_dir is file_output/raw root 
    #filtered_data_dir is file_output/filtered root 

    def __init__(self, scraped_data_root_dir,filtered_data_root_dir):
        self.scraped_data_root_dir = scraped_data_root_dir
        self.filtered_data_root_dir = filtered_data_root_dir

    def generate_hash(self, query, date):
        combined_str = f"{query}_{date}"
        return hashlib.sha256(combined_str.encode()).hexdigest()[:8]


 


    #make subdir in file_output/raw for current search query
    def make_scraped_sub_dir_raw(self, brand, query):
        """
        Makes a new subdir in file_output/raw root for this specific item

        :param: brand 
        :param: query
        :param: filtered_data_root_dir is subdir in file_output/filtered
        
        """
        current_date = datetime.now().strftime('%Y-%d-%m')
        try:
            query_hash = self.generate_hash(query, current_date)
            dir_name = f"RAW_SCRAPE_{brand}_{current_date}_{query}_{query_hash}"
            new_sub_dir = os.path.join(self.scraped_data_root_dir, dir_name)
            
            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
            return new_sub_dir
        
        except Exception as e:
            print(f"Error while creating sub-directory for raw scrape: {e}")
            return None


    def make_filtered_sub_dir(self, brand, query,filtered_data_root_dir):
        """
        Makes a new subdir in file_output/filtered root for this specific item
    
        :param: brand 
        :param: query
        :param: filtered_data_root_dir is subdir in file_output/filtered
        
        """
        current_date = datetime.now().strftime('%Y-%d-%m') 
        
        try:
            query_hash = self.generate_hash(query, current_date)
            dir_name = f"FILTERED_{brand}_{current_date}_{query}_{query_hash}"
            new_sub_dir = os.path.join(self.filtered_data_root_dir,dir_name)

            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
            return new_sub_dir
        except Exception as e:
             print(f"Error while creating filtered sub-directory for raw scrape: {e}")
             return None
        
    def parse_file_name(self,file):

        file_path_tokens = file.split('/')[-1]
        file_name_tokens = file_path_tokens.split('_')
        source = file_name_tokens[0]
        date = file_name_tokens[1]
        query = f"{file_name_tokens[2]}_{file_name_tokens[3].split('.')[0]}"
        print(f"query {query}")

        return source,date,query

    def filter_specific(self,scraped_data_file,specific_item,filtered_subdir):
        
        
        try:
            df = pd.read_csv(scraped_data_file,skiprows=2)
            # print(df)
            df = df.dropna()

            filtered_df = df[df['product_name']==specific_item]
            # print(filtered_df)
            
            #if df filtering didnt fail, continue
            try:
                source,date,query = self.parse_file_name(scraped_data_file)
                hash = self.generate_hash(query,date)
                new_filtered_filename = f"FILTERED_{source}_{date}_{query}_{hash}"
                print(f"new filtered_filename {new_filtered_filename}")

                # new_subdir = self.make_filtered_sub_dir(source,date,query)
                # print(f"new filtered subdir {new_subdir}")
                
                filtered_df.to_csv(os.path.join(filtered_subdir,new_filtered_filename),index=False)
                
                return os.path.join(filtered_subdir,new_filtered_filename,'.csv')
            except Exception as e:
                print(f"filter product failed {e}")

        except Exception as e:
            print(f"file not found {e}")

        
