import pandas as pd 
from simple_chalk import chalk
import os,sys


"""
define function to receive price difference products - should be incoming as list
"""

# def make_output_dir():

#     current_dir = os.path.abspath(os.path.dirname(__file__))
#     root_dir = os.path.abspath(os.path.join(current_dir,".."))

#     output_dir = os.path.join(root_dir,"OUTPUT_price_changes")
s
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)

#     return output_dir

def make_output_filename(output_dir,source_filename):
    """
    creates output file name for output data 
    recieves scraped filename currently being from comparison

    Append 'PRICE_CHANGE' to scraped filename recieved from the comparison
    
    :param output_dir: dir where output file will be stored
    :param source_filename: original scraped filename
    :return: full output file path for the processed data
    """  

    try:
        source_filename_extracted = source_filename.split('/')[-1]
        # print(source_filename)
        output_file_name = f"PRICE_CHANGE_{source_filename_extracted}"
        output_file_path = os.path.join(output_dir,output_file_name)
        return output_file_path
    except Exception as e:
        print(chalk.red(f"Error creating output filename: {e}"))
        raise

# output_dir = make_output_dir()
# t = make_output_filename(output_dir,"/home/ubuntu/Documents/Projects/TDD-scraper/Analysis/../src/file_output/italist_2024-26-09_prada_bags.csv")
# print(t)


def get_all_messages(product_msg_list):
    """
    Retrieves all product messages from given list

    :param product_msg_list: list of product messages
    :return: same product message list
    """
    return product_msg_list

def calculate_price_change(df):
    
    """
    Calculates percentage change between curr and prev prices

    :param df: Datafram containing product data
    :return: DF with price change percentages
    
    """
    try:

        # Calculate percentage change
        df['price_change_percent'] = ((df['curr_price'] - df['prev_price']) / df['prev_price']) * 100
        
        # Add a '+' or '-' sign based on the percentage change
        df['price_change_percent_signed'] = df['price_change_percent'].apply(
            lambda x: f"+{x:.2f}%" if x >= 0 else f"{x:.2f}%"
        )
        return df
    
    except Exception as e:
         print(chalk.red(f"Error calculating price change: {e}"))


def filter_percent_changes(df, threshold=5):
    """
    Filters our products with price change less than given percent threschold

    :param df: Df containg product data
    :param threschold: min percent change to include a product
    :return: Filtered Dataframe
    
    """
    return df[abs(df['price_change_percent_signed'].str.rstrip('%').astype(float)) >= threshold]

def reorder_columns(df):
    """
    Reorders the DataFrame columns so that 'price_change_percent_signed' is first.

    :param df: DataFrame containing product data.
    :return: DataFrame with reordered columns.
    """
    try:
        # Drop the original price_change_percent column
        df = df.drop(columns=['price_change_percent'])
        #restructure cols to be new col + all old cols
        cols = ['price_change_percent_signed'] + [col for col in df.columns if col != 'price_change_percent_signed']
        return df[cols]
    except Exception as e:
        print(chalk.red(f"Error reordering columns: {e}"))
        


# # Function to save DataFrame to CSV
# def save_to_csv(df, filename):
#     df.to_csv(filename, index=False)
#     print(f"Filtered DataFrame saved to '{filename}'")

# Main function to run the process
def calc_percentage_diff_driver(output_dir,product_data,source_file):
    

    """
    Main driver to calculate percentage difference and save to report csv
    
    
    :param output_dir: Directory where output files are stored.
    :param product_data: List of product data for price comparison.
    :param source_file: The original file being processed.
    :return: None

    """
    # #create output dir doesnt exist
    # output_dir = make_output_dir()
    try:
        #make output file path
        output_file = make_output_filename(output_dir,source_file)
    
        
        # Step 1: Get the data
        products = get_all_messages(product_data)
        # print(chalk.green(products))
        
        # Step 2: Create DataFrame
        df = pd.DataFrame(products)
        
        # Step 3: Calculate the price change and drop the original percentage column
        df = calculate_price_change(df)
        
        # Step 4: Reorder columns
        df = reorder_columns(df)
        
        # Step 5: Filter out changes less than 5%
        df_filtered = filter_percent_changes(df, threshold=5)
        
        # Step 6: Save the filtered DataFrame to CSV
        report_file_path = output_file
        df_filtered.to_csv(report_file_path,index=False)
        
        print(f"Filtered DataFrame saved to '{report_file_path}'")
    except:
        print(chalk.red(f"Error in the price difference calculation process: {e}"))
       

# if __name__ == "__main__":

# temp_data = [
#     {'product_id': '14699240-14531549', 'curr_price': 438.9, 'curr_scrape_date': '2024-09-26', 'prev_price': 418.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14696885-14529194', 'curr_price': 3560.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3955.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14696866-14529175', 'curr_price': 3899.7, 'curr_scrape_date': '2024-09-26', 'prev_price': 3714.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14696865-14529174', 'curr_price': 4212.9, 'curr_scrape_date': '2024-09-26', 'prev_price': 4681.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14687555-14519864', 'curr_price': 4508.7, 'curr_scrape_date': '2024-09-26', 'prev_price': 4294.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14687437-14519746', 'curr_price': 1056.6, 'curr_scrape_date': '2024-09-26', 'prev_price': 1174.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14679027-14511336', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14679026-14511335', 'curr_price': 3134.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3134.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14670757-14503066', 'curr_price': 1749.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1749.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14664354-14496663', 'curr_price': 1877.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1877.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14664353-14496662', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14664352-14496661', 'curr_price': 3134.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3134.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14650505-14482814', 'curr_price': 3361.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3361.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14650492-14482801', 'curr_price': 2189.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2189.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14645226-14477535', 'curr_price': 2537.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2537.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14628287-14460596', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14625136-14457445', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14625132-14457441', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14625131-14457440', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14625130-14457439', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14625129-14457438', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14597352-14429661', 'curr_price': 4198.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 4198.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14573761-14406070', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14572513-14404822', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14568709-14401018', 'curr_price': 3955.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3955.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14568707-14401016', 'curr_price': 4986.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 4986.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14565352-14397661', 'curr_price': 3424.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3424.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14565351-14397660', 'curr_price': 3811.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3811.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14565350-14397659', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14565349-14397658', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14565340-14397649', 'curr_price': 2941.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2941.0, 'prev_scrape_date': '2024-09-25'},
#     {'product_id': '14565339-14397648', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'}]

    
#     input_file = "/home/ubuntu/Documents/Projects/TDD-scraper/Analysis/../src/file_output/italist_2024-26-09_prada_bags.csv"
#     calc_percentage_diff_driver(temp_data,'italist_2024-25-09_prada_bags.csv')
#     # # print( calc_percentage_diff_driver(temp_data))
#     # make_output_dir()
#     # print(make_output_filename('italist_2024-25-09_prada_bags.csv'))
    




