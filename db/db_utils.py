import psycopg2,sys,os
from simple_chalk import chalk
import time

#get parent dir  from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(parent_dir)

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


# Connect to PostgreSQL
DB_CONFIG = psycopg2.connect(
    host="localhost",
    port="5433",
    database="designer_products",
    user="admin",
    password="admin!"
)



def get_db_connection(**DB_CONFIG):
    
    conn = None
    try:
        print(chalk.green('Waiting before connecting to PostgreSQL database..'))
        time.sleep(10)  # Add a delay of 10 seconds (adjust as needed)

        #connect to PostgreSQL server
        print(chalk.green('Connecting to PostgreSQL database..'))
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="designer_products",
            user="admin",
            password="admin!"
        )

        #create cursor
        cur = conn.cursor()

        #execute a statement
        print(chalk.green('postgreSQL database version:'))
        cur.execute('SELECT version()')

        #display PostgreSQL database server version
        db_version = cur.fetchone()
        print(chalk.green(f"SUCCESS {db_version}"))

        return conn
    


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def fetch_product_ids(brand):

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        query = "SELECT product_id FROM products WHERE brand = %s"
        cur.execute(query,(brand,))
        result = cur.fetchall()
        # return [row[0] for row in result]
    except Exception as e:
        print(f"(fetch_products_ids) Error: {Exception}")


def fetch_product_ids_and_prices(brand):

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        query = "SELECT product_id, curr_price FROM products WHERE brand = %s"
        cur.execute(query,(brand,))
        result = cur.fetchall()
        print(result)
        # for row in result:
        #     product_id,last_price = row
        #     print(f"Product ID: {product_id}, Last Price: {last_price}")

        # Convert to list of dictionaries
        products = [{'product_id':row[0], 'curr_price': (row[1])} for row in result]

        return products
        
    except Exception as e:
        print(f"(fetch_products_ids_and_prices) Error: {Exception}")

def DB_fetch_product_ids_prices_dates(brand):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        query = "SELECT product_id, curr_price, curr_scrape_date, prev_price, prev_scrape_date FROM products WHERE brand = %s"
        cur.execute(query,(brand,))
        result = cur.fetchall()

    
        # for row in result:
        #    print(row)

        # Convert to list of dictionaries
        products = [
                    {'product_id':row[0], 
                     'curr_price': (row[1]),
                     'curr_scrape_date':(row[2]),
                     'prev_price':(row[3]),
                     'prev_scrape_date':(row[4])
                     } for row in result
                    ]

        return products
        
    except Exception as e:
        print(f"(fetch_products_ids_and_prices) Error: {Exception}")

  
def DB_bulk_update_existing(update_products):
    """
    Recieves list of dicts containing products to be updated in the db
    Will be dict containing most recent price and date of scrape

    Ex.
        [
            {
                'product_id':row['product_id'],
                'last_scrape_date': 'scrape_date',
                'last_price':row['Price'],
            },
            {},
            ....
        ]
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # print(f"products to be updated {update_products} ----------")
        update_query =  """
                        UPDATE products
                        SET curr_price = %s,
                            curr_scrape_date = %s,
                            prev_price = %s,
                            prev_scrape_date = %s
                        WHERE product_id = %s
                        """
        
        #create list of tuples from update_products list of dicts     
        update_data_as_tuples = [(product['curr_price'],
                               product['curr_scrape_date'],
                               product['prev_price'],
                               product['prev_scrape_date'],
                               product['product_id']) 
                               for product in update_products]
        
        # print(chalk.red(update_data_tuples))
        cur.executemany(update_query,update_data_as_tuples)
        conn.commit()
    except Exception as e:
        print(f"(BULK_UPDATE) - An error occurred: {e}")
        conn.rollback()  # Rollback in case of error
    
    # Print a success message if no exception occurred
    print(f"Bulk update successful. {cur.rowcount} rows updated.")


def DB_bulk_update_sold(sold_products):

    """sold_products is exisint_product_id_prices_dict rec'd from compare_data.py
        
       Structure is:
        existing_product_id_prices_dict[product_id] = {'curr_price':curr_price,
                                                        'curr_scrape_date':curr_scrape_date,
                                                        'prev_price':prev_price,
                                                        'prev_scrape_date':prev_scrape_date,
                                                        }
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        update_sold_query = """
                            UPDATE products
                            SET sold_date = %s,
                                sold = %s
                            WHERE product_id = %s
                            """
        update_data_as_tuples = []
        for k,v in sold_products.items():
            product_id = k
            sold_date = v['curr_scrape_date']
            sold = True
            update_data_as_tuples.append((sold_date,sold,product_id))

        print(update_data_as_tuples)
        # #create list of tuples from sold_products list of dicts     
        # update_data_as_tuples = [(product['curr_scrape_date'], # using curr as sold_date
        #                           True,                        #  setting from F to T
        #                           product['product_id']) 
        #                        for product in sold_products]
        cur.executemany(update_sold_query,update_data_as_tuples)
        conn.commit()
    except Exception as e:
        print(f"(BULK_UPDATE-SOLD) - An error occurred: {e}")
        conn.rollback()  # Rollback in case of error
    
     # Print a success message if no exception occurred
    print(f"Bulk update of sold products successful. {cur.rowcount} rows updated.")

def bulk_insert_new(new_products):
    
    """
    Recieves list of dicts containing  new products to be inserted into db
    Ex.
    Ex.
     [
        {
            'product_id':row['product_id'],
            'brand':row['Brand'],
            #'Product_name':row['Product_Name'],
            'last_scrape_date': 'scrape_date',
            'last_price':row['Price'],
        }
     ]
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        new_product_query = """
                            INSERT into products(
                                product_id,
                                brand,
                                product_name,
                                curr_price,
                                curr_scrape_date,
                                prev_price,
                                prev_scrape_date
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            """

        new_products_list_as_tuples =[(product['product_id'],
                                    product['brand'],
                                    product['product_name'],
                                    product['curr_price'],
                                    product['curr_scrape_date'],
                                    product['prev_price'],
                                    product['prev_scrape_date']) 
                                    for product in new_products]

        cur.executemany(new_product_query,new_products_list_as_tuples)
        conn.commit()
    except Exception as e:
        print(f"(BULK_INSERT_NEW) - An error occurred: {e}")
        conn.rollback()  # Rollback in case of error

     # Print a success message if no exception occurred
    print(f"Bulk insert successful. {cur.rowcount} rows inserted.")


if __name__ == "__main__":
    # print(fetch_product_ids('Prada'))
    # print(fetch_product_ids_and_prices('Prada'))
    print(DB_fetch_product_ids_prices_dates('Prada'))
       