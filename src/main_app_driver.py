"""
whole app process originates here 

this driver will orchestrate reading input data - which is brands and categories to target and generate reports for
"""



import requests,os,csv,sys
from datetime import datetime
import time,random, pytest,hashlib
import pandas as pd
from simple_chalk import chalk

# from scrapers.italist_scraper import italist_driver

#get parent dir  from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# print(parent_dir)

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


curr_dir= os.path.dirname(os.path.abspath(__file__))
# print(curr_dir)
user_query_data_file = os.path.join(curr_dir,'input_data','search_criteria.csv')
# print(os.path.isfile(user_query_data_file))

scraped_data_dir = os.path.join(curr_dir,'file_output')
# print(scraped_data_dir)

# from Analysis.compare_data import compare_driver
from rbmq.price_change_producer import publish_to_queue


recipient_email = "balmanzar883@gmail.com"


def generate_hash(query,date):

    #combine query and date to create a hash string
    combined_str = f"{query}_{date}"
    return hashlib.sha256(combined_str.encode()).hexdigest()[:8]

def make_filtered_sub_dir(source,date,query):

    """

    after specific filtering takes place 
    fitlered csv will be stored in new filtered subdir inside of file_output
    this function creates the new sub dir to store the csv in
    """

    query_hash = generate_hash(query,date)
    
    #FILTERED_2024-24-09_prada_bags_<item_query_hash>
    dir_name = f"FILTERED_{date}_{query}_{query_hash}"
    new_sub_dir = os.path.join(scraped_data_dir,dir_name)
    if not os.path.exists(new_sub_dir):
        os.makedirs(new_sub_dir)

    # print(new_sub_dir)
    return new_sub_dir




def parse_file_name(file):

    file_path_tokens = file.split('/')[-1]
    file_name_tokens = file_path_tokens.split('_')
    source = file_name_tokens[0]
    date = file_name_tokens[1]
    query = f"{file_name_tokens[2]}_{file_name_tokens[3].split('.')[0]}"
    print(f"query {query}")

    return source,date,query



def filter_specific(scraped_data_file,specific_item):

    try:
        df = pd.read_csv(scraped_data_file,skiprows=2)
        df = df.dropna()
        # print(df.head())

        fitlered_df = df[df['product_name'] == specific_item]
        # print(fitlered_df)
    except Exception as e:
        print(f"file not found {e}")
    # print(scraped_data_file)
    try:
       
        
        source,date,query = parse_file_name(scraped_data_file)
        hash = generate_hash(query,date)
        new_filtered_filename = f"FILTERED_{source}_{date}_{query}_{hash}"
        print(f"new filtered_filename {new_filtered_filename}")

        #create subdir to store filtered files
        new_subdir = make_filtered_sub_dir(source,date,query)
        print(f"new filtered subdir {new_subdir}")
        
        print(os.path.join(new_subdir,new_filtered_filename))
        fitlered_df.to_csv(os.path.join(new_subdir,new_filtered_filename))
    except Exception as e:
        pass



dummy_scrape_data_file_path = os.path.join(scraped_data_dir,'italist_2024-24-09_prada_bags.csv')
print(os.path.isfile(dummy_scrape_data_file_path))
filter_specific(dummy_scrape_data_file_path,'Brown Suede Prada Buckle Large Handbag') 

def read_user_input_data(user_query_data_file):
    """
    Reads and parses user input data from a CSV file. The data can have 2 or 3 parts 
    (brand and query, or brand, query, and specific item).

    :param user_query_data_file: Path to the CSV file containing user queries.
    :return: List of tuples where each tuple is (brand, query, specific_item).
             specific_item is None if not provided.
    """
    parsed_data = []
    
    try:
        with open(user_query_data_file, 'r', newline='', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            header = next(csv_reader)  # Skip the header row
            for row in csv_reader:
                if not row or all(field.strip() == '' for field in row):
                    print("Skipping empty row.")
                    continue  # Skip empty or whitespace-only rows
                
                # Handle 2-part and 3-part input
                if len(row) == 2:
                    brand, query = row
                    specific_item = None
                elif len(row) == 3:
                    brand, query, specific_item = row
                else:
                    print(f"Invalid row format: {row}")
                    continue
                
                # Validate input fields
                if not brand.strip() or not query.strip():
                    print(f"Invalid brand or query: {brand}, {query}")
                    continue

                parsed_data.append((brand.strip(), query.strip(), specific_item.strip() if specific_item else None))

        return parsed_data

    except Exception as e:
        print(f"Error reading user input data: {e}")
        return []  # Return an empty list if an error occurs

def run_scrapers_with_data(parsed_input_data):
    """
    Receives parsed data and runs the scraper functions accordingly.

    :param parsed_data: List of tuples containing (brand, query, specific_item).
    """

    
    for entry in parsed_input_data:
        brand, query, specific_item = entry
        
        print(f"Running scrapers for: {brand}, {query}, {specific_item}")
        
        # Call scraper functions based on the parsed input
        if specific_item:
            # If specific item is provided (3-part query)

            #perform scrape
            # italist_driver(brand, query, specific_item, local)
            dummy_scrape_data_file_path = os.path.join(scraped_data_dir,'italist_2024-24-09_prada_bags.csv')
            print(os.path.isfile(dummy_scrape_data_file_path))

            #then pass file for specific filtering - store in filtered dir
            filtered_dir = filter_specific(source,dummy_scrape_data_file_path)
                #create filtered dir for this date and query 

            #return filtered_dir to be used in comparison
            return filtered_dir
           
        else:
            # If only brand and query are provided (2-part query)
            # italist_driver(brand, query, local)
            pass  # Replace with actual function call
        
   
def run_comparisons(scraped_data_dir):
    """
    Function to trigger comparison logic for all scraped data.
    """
    # Logic to run the comparison across multiple CSV files
    print("Running comparison on all scraped data...")

    for dirpath,subdirs,files in os.walk(scraped_data_dir):
        for scraped_file in files:
            file_path = os.path.join(dirpath,scraped_file)
            compare_driver(file_path)

    #once all scraped files processed - send signal to attach price diff reports to email and send
    #signal sent directly to queue worker
    publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR QUERY","email":recipient_email})

def driver_function():
    """
    Main function to orchestrate the whole process.
    """
    brand = "prada"
    query = "bags"
    local = True


    input_data = read_user_input_data(user_query_data_file)


    # # Step 1: Run all scrapers
    run_scrapers_with_data(input_data)
    
    # # Step 2: Run comparison on scraped data
    # run_comparisons(scraped_data_dir)



if __name__ == "__main__":
    
    # brand = "prada"
    # query = "bags"
    # local = True
    # run_scrapers(brand,query,local)

    pass
    # driver_function()