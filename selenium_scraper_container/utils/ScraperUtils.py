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
        """
        Save the scraped data to a CSV file with consistent formatting and no empty lines.
        
        Args:
            data: List of data rows to write
            brand: Brand name
            category: Product category
            source: Data source
            output_dir: Output directory path
            query_hash: Query hash identifier
            data_type: 0 for raw data, 1 for filtered data
        """
        current_date = datetime.now().strftime('%Y-%d-%m')

        

        # file_hash = self.generate_hash(category,current_date)

        #if data_type = 1 - data is filtered data, prepend 'fitlered' to file name
        if data_type == 1:
            output_file = os.path.join(output_dir, f"FILTERED_{source}_{brand}_{current_date}_{category}_{query_hash}.csv")

        #if data_type == 0, data is raw data, dont prepend anything to file name
        else:
            output_file = os.path.join(output_dir, f"RAW_{source}_{brand}_{current_date}_{category}_{query_hash}.csv")
       
       #if this specific file exists already, dont overwrite it.
        if not (os.path.exists(output_file) and os.path.isfile(output_file)):
            print(f"NO PREV SCRAPE FILE FOR THIS QUERY HASH")
            with open(output_file, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    
                    writer.writerow([f"Scraped: {current_date}"])
                    writer.writerow([f"category: {brand}-{category}"])
                    writer.writerow(['product_id','brand','product_name','curr_price','listing_url','source'])
                    writer.writerow(['----------------------'])
    
                    #filter out empty rows and write data
                    for row in data:

                        """
                        if any(element meets condition for elements in iterable)
                        converts each x to str
                        strip whitespace from both ends
                        any() returns true if any of the values are non empty strings after stripping
                        """
                        if any(str(x).strip() for x in row):

                            #standardize string vals to uppercase                       
                            processed_row = [
                                str(element).upper() if isinstance(element,str) else element
                                for element in row
                            ]
                            writer.writerow(processed_row)
                    print(f"Data successfully saved to {output_file}")
                    return output_file
        else:
            print(f"EXISTS - PREV SCRAPE FILE FOR THIS QUERY HASH")
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


    def make_filtered_sub_dir(self, brand, category,filtered_data_root_dir,query_hash,query_date):
        """
        Makes a new subdir in scrape_file_output/filtered root for this specific item
    
        :param: brand 
        :param: category
        :param: filtered_data_root_dir is subdir in scrape_file_output/filtered
        
        """
        current_date = datetime.now().strftime('%Y-%d-%m') if not query_date else query_date
        
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
            FILTERED_ITALIST_PRADA_2024-24-10_BAGS_0c87ba98.csv
            RAW_ITALIST_PRADA_2024-24-10_BAGS_0c87ba98.csv
        """
        file_path_tokens = file.split('/')[-1]
        file_name_tokens = file_path_tokens.split('_')
        source = file_name_tokens[1]
        brand = file_name_tokens[2]
        date = file_name_tokens[3]
        category = file_name_tokens[4]
        # print(file)
        # print(source)
        # print(date)
        # print(brand)
        # print(category)

        query_hash = file_name_tokens[5].split('.')[0]
        print(query_hash)

        return source,date,brand,category,query_hash

   
    def filter_by_specific_item(self, scraped_data_file, specific_item, filtered_subdir, query_hash):
       
        """
        Filter scraped data by specific item and create a clean filtered file.
        
        Args:
            scraped_data_file: Path to raw scraped data file
            specific_item: Item name to filter by
            filtered_subdir: Output directory for filtered file
            query_hash: Query hash identifier
        """
        # Convert file to DataFrame
        try:
            #read csv file with custom header handling
            df = pd.read_csv(scraped_data_file, skiprows=2)
            df = df.dropna()

            # Filter DataFrame based on specific item
            filtered_df = df[df['product_name'] == specific_item]
            print(filtered_df)

            # If filtering succeeds, continue with file creation
            try:
                # Parse file name to get source, date, category
                source,date,brand,category,query_hash = self.parse_file_name(scraped_data_file)
        
            except Exception as e:
                print(chalk.red(f"FAILED: output file creation - {e}"))

            try:
                # Convert DataFrame to list
                df_list = filtered_df.values.tolist()

                # Pass only  target dir to `save_to_file`, not the full path
                output_file_path = self.save_to_file(df_list, brand, category, source, filtered_subdir, query_hash,1)
                # return new_filtered_filepath  
                return output_file_path


            except pd.errors.EmptyDataError:
                print(chalk.red("Input file is empty"))
                raise

        except Exception as e:
            print(chalk.red(f"Error in filter_by_specific_item: {e}"))
            raise

        
if __name__ == "__main__":


    def find_project_root():
        """
        Function searches for proj root dir

        Using already created .marker file '.PROJECT_ROOT' at project root level
        we look for this marker file
        when we find it, we know we are at proj root level
        
        """

        #get curr dir of this abs path of file
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        
        #init proj root to start at curr dir - temporarily
        #will be updated as we search upwards
        proj_root = curr_dir

        
        #we continue searching until we find dir containing .PROJECT_ROOT or we hit filesystem root '/'
        while True:
            
            #combine curr dir with .PROJECT_ROOT, check if file exists in this curr proj_root
            if os.path.exists(os.path.join(proj_root,'.PROJECT_ROOT')):
                #if exists, means we found it, return curr dir as proj root
                return proj_root
            

            #if not found yet, get parent of curr proj root value 
            parent = os.path.dirname(proj_root)

            #reached fileystem root '/'
            if parent == proj_root:
                raise RuntimeError("Could not find project root")
            

            #If we haven't found the marker and haven't hit filesystem root
            #update proj root to be curr parent value and keep searching
            proj_root = parent



    proj_root = find_project_root()
    print(proj_root)

    # Ensure the project root is accessible
    sys.path.append(proj_root)

    
    from shared_paths import RAW_SCRAPE_DIR,FILTERED_DATA_DIR,REPORTS_ROOT_DIR,SOLD_REPORTS_DIR,PRICE_REPORTS_DIR,ARCHIVE_DIR


    PARENT_scraped_data_dir_raw = RAW_SCRAPE_DIR
    PARENT_scraped_data_dir_filtered = FILTERED_DATA_DIR
  
    utils = ScraperUtils(PARENT_scraped_data_dir_raw,PARENT_scraped_data_dir_filtered)
    
    
    test_input_RAW_file = f"RAW_SCRAPE_PRADA_2024-01-12_BAGS_027c1ceb/RAW_ITALIST_PRADA_2024-01-12_BAGS_027c1ceb.csv"
    test_input_RAW_file_path = os.path.join(PARENT_scraped_data_dir_raw,test_input_RAW_file)
    print(os.path.isfile(test_input_RAW_file_path))


    # current_date = datetime.now().strftime('%Y-%d-%m')
    current_date = '2024-01-12'
    spec_item = 'TOTE'
    
    brand = 'PRADA'
    category = 'BAGS'
    query = f"{brand}_{category}" #Prada_bags , Gucci_shirts
    # query_hash = utils.generate_hash(query,spec_item,current_date)
    query_hash = '0271ceb'

    # # # # utils.make_scraped_sub_dir_raw(brand,category,query_hash)
    filtered_subdir = utils.make_filtered_sub_dir(brand,category,PARENT_scraped_data_dir_filtered,query_hash,current_date)
    # print(os.path.exists(filtered_subdir))
 


    # # source,date,brand,category,query_hash = utils.parse_file_name(input_file_path)
    
   

    utils.filter_by_specific_item(test_input_RAW_file_path,spec_item,filtered_subdir,query_hash)

