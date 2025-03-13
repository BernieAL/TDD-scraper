# src/utils/scraper_utils.py

import os,csv,sys
import hashlib
from datetime import datetime
import pandas as pd
from simple_chalk import chalk
import shutil
from pathlib import Path


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


class ScraperUtils:
    """
    Utility class for managing scraper file operations and directory structure.

    Directory Structure:
        scraper_worker/
        ├── tmp/                    # Temporary storage for scraped files before S3 upload
        ├── utils/                  # Utility functions and helpers
        ├── scrapers/              # Individual scraper implementations
        └── scrape_file_output/    # Local file storage (when not using S3)
            ├── raw/               # Raw scraper output files
            └── filtered/          # Filtered/processed data files

    Attributes:
        scraper_root_dir (Path): Root directory of scraper_worker module
        temp_dir (Path): Temporary directory for staging files before S3 upload
        scraped_data_root_dir (str): Directory for raw scraper output
        filtered_data_root_dir (str): Directory for filtered data
        s3_client: Optional boto3 S3 client for uploads
    """
    


    #scraped_data_root_dir is scrape_file_output/raw root 
    #filtered_data_dir is scrape_file_output/filtered root 

    def __init__(self, scraped_data_root_dir, filtered_data_root_dir=None, s3_client=None):
        # Get current file's directory (utils/)
        current_dir = Path(__file__).parent
        
        # Navigate up to scraper_worker root
        self.scraper_root_dir = current_dir.parent.parent
        
        self.scraped_data_root_dir = scraped_data_root_dir
        self.filtered_data_root_dir = filtered_data_root_dir
        self.s3_client = s3_client
        
        # Create temp directory for staging files before S3 upload
        self.temp_dir = self.scraper_root_dir / 'tmp'
        self.temp_dir.mkdir(exist_ok=True)

    def generate_hash(self,query,specific_item,date):
        """
        
        gen hash that will be used across any function that makes dirs or files
        single hash generated for single category
        specific_item may be none - Ex if category = prada bags

        query recieved in format of "{brand}_{category}"
        """
        combined_str = f"{query}_{specific_item}_{date}"
        return hashlib.sha256(combined_str.encode()).hexdigest()[:8]
    
  
    def make_data_source_output_dir(self,source):

        """
        Creates subdir based on src inside of temp/
        This dir will be used to store the scraped data specific to a src
        Ex. 
            If Data source is Italist.com -> create temp/italist/
                store data there.
        """

        data_src_dir = self.temp_dir / source
        data_src_dir.mkdir(exist_ok=True)
        return data_src_dir

    def save_to_file(self, data, brand, category, source, output_dir, query_hash, data_type):
        """
        Save data to local temp file in source-specific directory
        
        Directory Structure:
            tmp/
            ├── italist/              # Italist.com scraped data
            │   ├── RAW_ITALIST_PRADA_2024-01-15_BAGS_a7b2c.csv
            │   └── FILTERED_ITALIST_PRADA_2024-01-15_BAGS_d8e3f.csv
            ├── farfetch/             # Farfetch.com scraped data
            └── mytheresa/           # MyTheresa.com scraped data
        """
        current_date = datetime.now().strftime('%Y-%d-%m')
        
        # Generate filename
        prefix = "FILTERED_" if data_type == 1 else "RAW_"
        filename = f"{prefix}{source}_{brand}_{current_date}_{category}_{query_hash}.csv"
        
        # Get source-specific directory
        source_dir = self.make_data_source_output_dir(source.lower())
        
        # Save to source directory
        temp_file = source_dir / filename
        
        with open(temp_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([f"Scraped: {current_date}"])
            writer.writerow([f"category: {brand}-{category}"])
            writer.writerow(['product_id','brand','product_name','curr_price','listing_url','source'])
            writer.writerow(['----------------------'])

            for row in data:
                if any(str(x).strip() for x in row):
                    processed_row = [
                        str(element).upper() if isinstance(element,str) else element
                        for element in row
                    ]
                    writer.writerow(processed_row)
                
        print(f"Data successfully saved to {temp_file}")
        return temp_file, filename
    


    def upload_to_s3(self, file_path: str, s3_key: str) -> bool:
        """Upload file to S3"""
        if not self.s3_client:
            return False
            
        try:
            with open(file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=os.environ['S3_BUCKET'],
                    Key=s3_key,
                    Body=f
                )
            print(f"Uploaded to S3: {s3_key}")
            return True
        except Exception as e:
            print(f"S3 upload failed: {e}")
            return False

    def cleanup(self):
        """Remove temporary files"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

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


    def make_filtered_sub_dir(self, brand, category,filtered_data_root_dir,query_hash,query_date=None):
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
                output_file_path, filename = self.save_to_file(df_list, brand, category, source, filtered_subdir, query_hash,1)
                # return new_filtered_filepath  
                return output_file_path, filename


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

