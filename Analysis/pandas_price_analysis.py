import pandas as pd 
from simple_chalk import chalk


"""
define function to receive price difference products - should be incoming as list


"""

temp_data = [{'product_id': '14558494-14390803', 'curr_price': 2554.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 554.0, 'prev_scrape_date': '2024-09-25'}, 
             {'product_id': '14557360-14389669', 'curr_price': 2554.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2554.0, 'prev_scrape_date': '2024-09-25'}, 
             {'product_id': '14554718-14387027', 'curr_price': 2511.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2511.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14551812-14384121', 'curr_price': 2189.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2189.0, 'prev_scrape_date': '2024-09-25'}, 
             {'product_id': '14533201-14365510', 'curr_price': 2167.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2167.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14517370-14349679', 'curr_price': 1684.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1684.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14517368-14349677', 'curr_price': 2554.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2154.0, 'prev_scrape_date': '2024-09-25'}, 
             {'product_id': '14517365-14349674', 'curr_price': 2844.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2844.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14463824-14296133', 'curr_price': 2071.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2071.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14455300-14287609', 'curr_price': 2457.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2457.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14445396-14277705', 'curr_price': 2361.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2361.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14385668-14217977', 'curr_price': 1684.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 1684.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14633205-14465514', 'curr_price': 2163.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2163.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14627108-14459417', 'curr_price': 3477.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3477.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14627105-14459414', 'curr_price': 3982.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3982.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14602957-14435266', 'curr_price': 2091.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 2091.0, 'prev_scrape_date': '2024-09-25'},
             {'product_id': '14602956-14435265', 'curr_price': 3361.0, 'curr_scrape_date': '2024-09-26', 'prev_price': 3361.0, 'prev_scrape_date': '2024-09-25'}]

def get_all_messages(product_msg_list):

    return product_msg_list

def calc_price_percentage(recd_products):
    # Get all product messages
    products = get_all_messages(recd_products)

    # Create DataFrame
    df = pd.DataFrame(products)

    # Calculate the percentage change between previous and current price
    df['price_change_percent'] = ((df['curr_price'] - df['prev_price']) / df['prev_price']) * 100

    # Filter for changes of 10% or more
    ten_percent_changes = df[abs(df['price_change_percent']) >= 10]

    # Filter for changes between 5% and 10%
    five_percent_changes = df[(abs(df['price_change_percent']) >= 5) & (abs(df['price_change_percent']) < 10)]

    # Print results
    print("\nProducts with price changes >= 10%:")
    print(ten_percent_changes[['product_id', 'curr_price', 'prev_price', 'price_change_percent']])

    print("\nProducts with price changes >= 5% and < 10%:")
    print(five_percent_changes[['product_id', 'curr_price', 'prev_price', 'price_change_percent']])

if __name__ == "__main__":
    calc_price_percentage(temp_data)