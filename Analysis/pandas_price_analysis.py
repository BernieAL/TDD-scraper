import pandas as pd 
from simple_chalk import chalk


"""
define function to receive price difference products - should be incoming as list


"""

# temp_data = [{'product_id': '14558494-14390803', 'curr_price': 2554.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 554.0, 'prev_scrape_date': '2024-09-25'}, 
#              {'product_id': '14557360-14389669', 'curr_price': 2554.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2554.0, 'prev_scrape_date': '2024-09-25'}, 
#              {'product_id': '14554718-14387027', 'curr_price': 2511.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2511.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14551812-14384121', 'curr_price': 2189.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2189.0, 'prev_scrape_date': '2024-09-25'}, 
#              {'product_id': '14533201-14365510', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14517370-14349679', 'curr_price': 1684.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1684.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14517368-14349677', 'curr_price': 2554.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2154.0, 'prev_scrape_date': '2024-09-25'}, 
#              {'product_id': '14517365-14349674', 'curr_price': 2844.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2844.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14463824-14296133', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14455300-14287609', 'curr_price': 2457.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2457.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14445396-14277705', 'curr_price': 2361.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2361.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14385668-14217977', 'curr_price': 1684.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1684.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14633205-14465514', 'curr_price': 2163.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2163.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14627108-14459417', 'curr_price': 3477.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3477.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14627105-14459414', 'curr_price': 3982.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3982.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14602957-14435266', 'curr_price': 2091.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2091.0, 'prev_scrape_date': '2024-09-25'},
#              {'product_id': '14602956-14435265', 'curr_price': 3361.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3361.0, 'prev_scrape_date': '2024-09-25'}]


# Function to get all product messages
def get_all_messages(product_msg_list):
    return product_msg_list

# Function to calculate percentage change and drop price_change_percent after creating signed version
def calculate_price_change(df):
    # Calculate percentage change
    df['price_change_percent'] = ((df['curr_price'] - df['prev_price']) / df['prev_price']) * 100
    
    # Add a '+' or '-' sign based on the percentage change
    df['price_change_percent_signed'] = df['price_change_percent'].apply(
        lambda x: f"+{x:.2f}%" if x >= 0 else f"{x:.2f}%"
    )
    
    
    return df

# Function to filter out rows with percent changes less than 5%
def filter_percent_changes(df, threshold=5):
    return df[abs(df['price_change_percent_signed'].str.rstrip('%').astype(float)) >= threshold]

# Function to reorder columns
def reorder_columns(df):
     # Drop the original price_change_percent column
    df = df.drop(columns=['price_change_percent'])
    cols = ['price_change_percent_signed'] + [col for col in df.columns if col != 'price_change_percent_signed']
    return df[cols]

# Function to save DataFrame to CSV
def save_to_csv(df, filename):
    df.to_csv(filename, index=False)
    print(f"Filtered DataFrame saved to '{filename}'")

# Main function to run the process
def calc_percentage_diff_driver(product_data):
    # Step 1: Get the data
    products = get_all_messages(product_data)
    
    # Step 2: Create DataFrame
    df = pd.DataFrame(products)
    
    # Step 3: Calculate the price change and drop the original percentage column
    df = calculate_price_change(df)
    
    # Step 4: Reorder columns
    df = reorder_columns(df)
    
    # Step 5: Filter out changes less than 5%
    df_filtered = filter_percent_changes(df, threshold=5)
    
    # Step 6: Save the filtered DataFrame to CSV
    save_to_csv(df_filtered, 'filtered_products.csv')
    
    return df_filtered

if __name__ == "__main__":
    
    print( calc_percentage_diff_driver(temp_data))