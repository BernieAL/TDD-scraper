# src/utils/scraper_utils.py

import os,csv,sys
import hashlib
from datetime import datetime
import pandas as pd
from simple_chalk import chalk


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


class ScraperUtils:
    


    #scraped_data_root_dir is scrape_file_output/raw root 
    #filtered_data_dir is scrape_file_output/filtered root 

    def __init__(self, scraped_data_root_dir,filtered_data_root_dir=None):
        self.scraped_data_root_dir = scraped_data_root_dir
        self.filtered_data_root_dir = filtered_data_root_dir

    def generate_hash(self,query,specific_item,date):
        """
        
        gen hash that will be used across any function that makes dirs or files
        single hash generated for single category
        specific_item may be none - Ex if category = prada bags

        query recieved in format of "{brand}_{category}"
        """
        combined_str = f"{query}_{specific_item}_{date}"
        return hashlib.sha256(combined_str.encode()).hexdigest()[:8]


    def save_to_file(self, data,brand,category,source,output_dir,query_hash,data_type):
        """Save the scraped data to a CSV file in the given directory."""
        current_date = datetime.now().strftime('%Y-%d-%m')

        # file_hash = self.generate_hash(category,current_date)

        #if data_type = 1 - data is filtered data, prepend 'fitlered' to file name
        if data_type == 1:
            output_file = os.path.join(output_dir, f"FILTERED_{source}_{brand}_{current_date}_{category}_{query_hash}.csv")
        #if data_type == 0, data is raw data, dont prepend anything to file name
        else:
            output_file = os.path.join(output_dir, f"RAW_{source}_{brand}_{current_date}_{category}_{query_hash}.csv")
       

        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                file.write(f"Scraped: {current_date} \n")
                file.write(f"category: {brand}-{category} \n")
                writer.writerow(['product_id','brand','product_name','curr_price','listing_url','source'])
                file.write('---------------------- \n')
                for row in data:
                    writer.writerow(row)

        print(f"Data successfully saved to {output_file}")
        return output_file


    #make subdir in scrape_file_output/raw for current search category
    def make_scraped_sub_dir_raw_old(self, brand,category,query_hash):
        """
        Makes a new subdir in scrape_file_output/raw root for this specific item

        :param: brand 
        :param: category
        :param: filtered_data_root_dir is subdir in scrape_file_output/filtered
        
        """
        current_date = datetime.now().strftime('%Y-%d-%m')
        try:
            # category_hash = self.generate_hash(category, current_date)
            dir_name = f"RAW_SCRAPE_{brand}_{current_date}_{category}_{query_hash}"
            new_sub_dir = os.path.join(self.scraped_data_root_dir, dir_name)
            
            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
            return new_sub_dir
        
        except Exception as e:
            print(f"Error while creating sub-directory for raw scrape: {e}")
            return None

    def make_scraped_sub_dir_raw(self, brand, category, query_hash):
        """
        Creates a new subdir in scrape_file_output/raw root for this specific item.

        :param brand: Brand name (e.g., Prada)
        :param category: Category of the product (e.g., Bags)
        :param query_hash: A hash for the query
        :return: The path of the created directory
        """
        current_date = datetime.now().strftime('%Y-%d-%m')
        try:
            # Build the directory name using the brand, query, and hash
            dir_name = f"RAW_SCRAPE_{brand}_{current_date}_{category}_{query_hash}"
            new_sub_dir = os.path.join(self.scraped_data_root_dir, dir_name)
            
            print(f"Attempting to create directory: {new_sub_dir}")
            
            # Check if the directory already exists; if not, create it
            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
                print(f"Directory created: {new_sub_dir}")
            else:
                print(f"Directory already exists: {new_sub_dir}")
            
            return new_sub_dir
        
        except Exception as e:
            print(f"Error while creating sub-directory for raw scrape: {e}")
            return None


    def make_filtered_sub_dir(self, brand, category,filtered_data_root_dir,query_hash):
        """
        Makes a new subdir in scrape_file_output/filtered root for this specific item
    
        :param: brand 
        :param: category
        :param: filtered_data_root_dir is subdir in scrape_file_output/filtered
        
        """
        current_date = datetime.now().strftime('%Y-%d-%m') 
        
        try:
            # category_hash = self.generate_hash(category, current_date)
            dir_name = f"FILTERED_{brand}_{current_date}_{category}_{query_hash}"
            new_sub_dir = os.path.join(self.filtered_data_root_dir,dir_name)

            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
            return new_sub_dir
        except Exception as e:
             print(f"Error while creating filtered sub-directory for raw scrape: {e}")
             return None
        
    def parse_file_name(self,file):
        
        """
        filenames recieved are in the same format.
        possible filenames recieved:
            FILTERED_italist_prada_2024-14-10_bags_c4672843.csv
            RAW_italist_prada_2024-14-10_bags_f3f28ac8.csv
        """
        file_path_tokens = file.split('/')[-1]
        file_name_tokens = file_path_tokens.split('_')
        source = file_name_tokens[1]
        brand = file_name_tokens[2]
        date = file_name_tokens[3]
        category = file_name_tokens[4]
        print(file)
        print(source)
        print(date)
        print(brand)
        print(category)

        query_hash = file_name_tokens[5]

        return source,date,brand,category

    def filter_specific2(self,scraped_data_file,specific_item,filtered_subdir,query_hash):
        
        #read metadata from input file, store, and use metadata again when creating filtered file
        try:
            csv_reader = csv.reader(scraped_data_file)

            # Get scrape date from file header
            scrape_date_line = next(csv_reader)
            scrape_date = scrape_date_line[0].split(':')[1].strip()
            scrape_date = datetime.strptime(scrape_date, '%Y-%d-%m').date()
            print(f"Scrape Date: {scrape_date}")

            # Get category info
            category_line = next(csv_reader)
            category = category_line[0].split(':')[1].strip()
            print(f"category: {category}")
            
            

            # Process CSV headers and rows
            headers = next(csv_reader)

        except Exception as e:
            print(f"header parsing error{e}")   



        #convert file to df
        try:
            df = pd.read_csv(scraped_data_file,skiprows=2)
            # print(df)
            df = df.dropna()

            filtered_df = df[df['product_name']==specific_item]
            print(filtered_df)
            
            #if df filtering didnt fail, continue with file creation
            try:
                #get source,date,category from input file name
                source,date,category = self.parse_file_name(scraped_data_file)
                print(chalk.red(f"FILTER SPECIFIC - {source}_{date}_{category}"))
                
                # #create unique hash for this date + category combination
                # hash = self.generate_hash(category,date)
                
                #build filename we are about to create
                new_filtered_filename = f"FILTERED_{source}_{date}_{category}_{query_hash}.csv"
                print(f"new filtered_filename: {new_filtered_filename}")

                #build filepath
                new_filtered_filepath = os.path.join(filtered_subdir,new_filtered_filename)

            except Exception as e:
                print(chalk.red(f"FAILED: output file creation {e}"))



            try:
                #convert df to list 
                df_list = filtered_df.values.tolist()

                self.save_to_file(df_list,'prada','bags',new_filtered_filename,filtered_subdir,query_hash,1)
                return new_filtered_filepath

            except Exception as e:
                  print(chalk.red(f"FAILED: converting file to df + filtering{e}"))


            # #writing metadata and df to output file
            # try:

            #     with open(new_filtered_filepath,'w') as file:
                    
            #         file.write()
                    
                 

            #         filtered_df.to_csv(new_filtered_filepath,index=False)
                
            #     return os.path.join(filtered_subdir,new_filtered_filename)
            # except Exception as e:
            #     print(chalk.red(f"FAILED: metadata + df writing to output file {e}"))

        except Exception as e:
            print(chalk.red(f"FAILED: converting file to df + filtering{e}"))

    def filter_specific(self, scraped_data_file, specific_item, filtered_subdir, query_hash):
        try:
            csv_reader = csv.reader(scraped_data_file)

            # Get scrape date from file header
            scrape_date_line = next(csv_reader)
            scrape_date = scrape_date_line[0].split(':')[1].strip()
            scrape_date = datetime.strptime(scrape_date, '%Y-%d-%m').date()
            print(f"Scrape Date: {scrape_date}")

            # Get category info
            category_line = next(csv_reader)
            category = category_line[0].split(':')[1].strip()
            print(f"Category: {category}")

            # Process CSV headers and rows
            headers = next(csv_reader)

        except Exception as e:
            print(f"Header parsing error: {e}")   

        # Convert file to DataFrame
        try:
            df = pd.read_csv(scraped_data_file, skiprows=2)
            df = df.dropna()

            # Filter DataFrame based on specific item
            filtered_df = df[df['product_name'] == specific_item]
            print(filtered_df)

            # If filtering succeeds, continue with file creation
            try:
                # Parse file name to get source, date, category
                source,date,brand,category = self.parse_file_name(scraped_data_file)
        
            except Exception as e:
                print(chalk.red(f"FAILED: output file creation {e}"))

            try:
                # Convert DataFrame to list
                df_list = filtered_df.values.tolist()

                # Pass only  target dir to `save_to_file`, not the full path
                output_file_path = self.save_to_file(df_list, brand, category, source, filtered_subdir, query_hash,1)
                # return new_filtered_filepath  
                return output_file_path


            except Exception as e:
                print(chalk.red(f"FAILED: converting file to df + filtering {e}"))

        except Exception as e:
            print(chalk.red(f"FAILED: converting file to df + filtering {e}"))

        
if __name__ == "__main__":


    curr_dir = os.path.dirname(os.path.abspath(__file__))
    scraped_data_dir_raw = os.path.join(curr_dir,'..','scrape_file_output','raw')
    # print(os.path.isdir(scraped_data_dir_raw))
    scraped_data_dir_filtered = os.path.join(curr_dir,'..','scrape_file_output','filtered')
    # print(os.path.isdir(scraped_data_dir_filtered))


    input_file = f"RAW_SCRAPE_prada_2024-14-10_bags_f3f28ac8/RAW_italist_prada_2024-14-10_bags_f3f28ac8.csv"
    input_file_path = os.path.join(scraped_data_dir_raw,input_file)
    # print(os.path.isfile(input_file_path))

    filtered_subdir = os.path.join(scraped_data_dir_filtered,"FILTERED_prada_2024-14-10_bags_f3f28ac8")
    # print(os.path.isdir(filtered_subdir))
    spec_item = 'Shoulder Bag'
    utils = ScraperUtils(scraped_data_dir_raw,scraped_data_dir_filtered)
    
    current_date = datetime.now().strftime('%Y-%d-%m')

    brand = 'Prada'
    category = 'bags'
    query = f"{brand}_{category}" #Prada_bags , Gucci_shirts
    query_hash = utils.generate_hash(query,None,current_date)

    utils.make_scraped_sub_dir_raw(brand,category,query_hash)

    print(utils.parse_file_name(input_file_path))

    utils.filter_specific(input_file_path,spec_item,filtered_subdir,query_hash)

