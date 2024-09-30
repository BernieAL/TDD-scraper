import pandas as pd 
from simple_chalk import chalk


"""
define function to receive price difference products - should be incoming as list


"""

temp_data = [
    {'product_id': '14699240-14531549', 'curr_price': 438.9, 'curr_scrape_date': '2024-09-26', 'prev_price': 418.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14696885-14529194', 'curr_price': 3560.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3955.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14696866-14529175', 'curr_price': 3899.7, 'curr_scrape_date': '2024-09-26', 'prev_price': 3714.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14696865-14529174', 'curr_price': 4212.9, 'curr_scrape_date': '2024-09-26', 'prev_price': 4681.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14687555-14519864', 'curr_price': 4508.7, 'curr_scrape_date': '2024-09-26', 'prev_price': 4294.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14687437-14519746', 'curr_price': 1056.6, 'curr_scrape_date': '2024-09-26', 'prev_price': 1174.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14679027-14511336', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14679026-14511335', 'curr_price': 3134.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3134.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14670757-14503066', 'curr_price': 1749.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1749.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14664354-14496663', 'curr_price': 1877.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1877.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14664353-14496662', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14664352-14496661', 'curr_price': 3134.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3134.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14650505-14482814', 'curr_price': 3361.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3361.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14650492-14482801', 'curr_price': 2189.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2189.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14645226-14477535', 'curr_price': 2537.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2537.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14628287-14460596', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14625136-14457445', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14625132-14457441', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14625131-14457440', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14625130-14457439', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14625129-14457438', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14597352-14429661', 'curr_price': 4198.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 4198.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14573761-14406070', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14572513-14404822', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14568709-14401018', 'curr_price': 3955.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3955.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14568707-14401016', 'curr_price': 4986.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 4986.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14565352-14397661', 'curr_price': 3424.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3424.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14565351-14397660', 'curr_price': 3811.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3811.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14565350-14397659', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14565349-14397658', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14565340-14397649', 'curr_price': 2941.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2941.0, 'prev_scrape_date': '2024-09-25'},
    {'product_id': '14565339-14397648', 'curr_price': 2747.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2747.0, 'prev_scrape_date': '2024-09-25'}]



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

# # Function to save DataFrame to CSV
# def save_to_csv(df, filename):
#     df.to_csv(filename, index=False)
#     print(f"Filtered DataFrame saved to '{filename}'")

# Main function to run the process
def calc_percentage_diff_driver(product_data):
    # Step 1: Get the data
    products = get_all_messages(product_data)
    print(chalk.green(products))
    
    # Step 2: Create DataFrame
    df = pd.DataFrame(products)
    
    # Step 3: Calculate the price change and drop the original percentage column
    df = calculate_price_change(df)
    
    # Step 4: Reorder columns
    df = reorder_columns(df)
    
    # Step 5: Filter out changes less than 5%
    df_filtered = filter_percent_changes(df, threshold=5)
    
    # Step 6: Save the filtered DataFrame to CSV
    report_file_path = 'filtered_products.csv'
    df_filtered.to_csv(report_file_path,index=False)
    
    print(f"Filtered DataFrame saved to '{report_file_path}'")
    
    return report_file_path

if __name__ == "__main__":
    
    print( calc_percentage_diff_driver(temp_data))