import psycopg2,sys,os
from simple_chalk import chalk
import time,json
import decimal
from datetime import date
from dotenv import load_dotenv, find_dotenv

#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    
# For Docker
if os.getenv('RUNNING_IN_DOCKER') == '1' and '/app' not in sys.path:
    sys.path.insert(0, '/app')


DB_URI = os.getenv('DB_URI')

# # Connect to PostgreSQL
# DB_CONFIG = psycopg2.connect(
#     host="localhost",
#     port="5433",
#     database="designer_products",
#     user="admin",
#     password="admin!"
# )



def get_db_connection(**DB_CONFIG):
    
    conn = None
    try:
        print(chalk.green('Waiting before connecting to PostgreSQL database..'))
        time.sleep(10)  # Add a delay of 10 seconds (adjust as needed)

        #connect to PostgreSQL server
        print(chalk.green('Connecting to PostgreSQL database..'))
        # conn = psycopg2.connect(
        #     host="localhost",
        #     port="5433",
        #     database="designer_products",
        #     user="admin",
        #     password="admin!"
        # )




        # conn = psycopg2.connect(os.environ['DB_URI'])
        print(f"DB_URI {DB_URI}")
        conn = psycopg2.connect(DB_URI)    

        

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




def DB_fetch_product_ids_prices_dates(brand,source,spec_item=None):
    conn = get_db_connection()


    #if spec_item specified, get db records matching spec_item product_name
    if spec_item != None:

        try:
            cur = conn.cursor()
            query = "SELECT product_id, curr_price, curr_scrape_date, prev_price, prev_scrape_date FROM products WHERE brand = %s and product_name = %s"
            cur.execute(query,(brand,spec_item))
            result = cur.fetchall()

            # Convert result from list of tuples to list of dictionaries
            products_to_list_of_dicts = [
                        {'product_id':row[0], 
                        'curr_price': (row[1]),
                        'curr_scrape_date':(row[2]),
                        'prev_price':(row[3]),
                        'prev_scrape_date':(row[4])
                        } for row in result
                        ]

            print(chalk.blue(f"retrieved db items {products_to_list_of_dicts}"))
            return products_to_list_of_dicts
            
        except Exception as e:
            print(f"(fetch_products_ids_and_prices) Error: {Exception}")

    else:
        #if not spec_item specified, get all records matching source (Ex. Italist)
        try:
            cur = conn.cursor()
            query = "SELECT product_id, curr_price, curr_scrape_date, prev_price, prev_scrape_date FROM products WHERE brand = %s and source = %s"
            cur.execute(query,(brand,source))
            result = cur.fetchall()

            # Convert result from list of tuples to list of dictionaries
            products_to_list_of_dicts = [
                        {'product_id':row[0], 
                        'curr_price': (row[1]),
                        'curr_scrape_date':(row[2]),
                        'prev_price':(row[3]),
                        'prev_scrape_date':(row[4])
                        } for row in result
                        ]

            print(chalk.blue(f"retrieved db items {products_to_list_of_dicts}"))
            return products_to_list_of_dicts
            
        except Exception as e:
            print(f"(fetch_products_ids_and_prices) Error: {Exception}")


  
def DB_bulk_update_existing(update_products):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        update_query = """
                        UPDATE products
                        SET curr_price = %s,
                            curr_scrape_date = %s,
                            prev_price = %s,
                            prev_scrape_date = %s
                        WHERE product_id = %s
                        """
        
        # Create list of tuples from update_products list of dicts     
        update_data_as_tuples = [(product['curr_price'],
                                  product['curr_scrape_date'],
                                  product['prev_price'],
                                  product['prev_scrape_date'],
                                  product['product_id']) 
                                 for product in update_products]
        
        cur.executemany(update_query, update_data_as_tuples)
        conn.commit()

        if cur.rowcount > 0:
            print(f"Bulk update successful. {cur.rowcount} rows updated.")
            return True
        else:
            raise Exception("No rows were updated during bulk update.")

    except Exception as e:
        print(f"(BULK_UPDATE) Error: {e}")
        conn.rollback()
        raise Exception(f"BULK_UPDATE_FAILED: {e}")  # Raising detailed exception

def DB_bulk_update_sold(sold_products):
    if not sold_products:
        return True
    
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
        for k, v in sold_products.items():
            product_id = k
            sold_date = v['curr_scrape_date']
            sold = True
            update_data_as_tuples.append((sold_date, sold, product_id))

        cur.executemany(update_sold_query, update_data_as_tuples)
        conn.commit()

        if cur.rowcount > 0:
            print(f"Bulk update of sold products successful. {cur.rowcount} rows updated.")
            return True
        else:
            raise Exception("No rows were updated for sold status.")

    except Exception as e:
        print(f"(BULK_UPDATE-SOLD) Error: {e}")
        conn.rollback()
        raise Exception(f"BULK_UPDATE_SOLD_FAILED: {e}")  # Raising detailed exception
    finally:
        cur.close()
        conn.close()

  
def DB_bulk_insert_new(new_products):
    try:

        if not new_products:
            return True
        conn = get_db_connection()
        cur = conn.cursor()

        new_product_query = """
            INSERT INTO products(product_id,
                                brand,
                                product_name,
                                curr_price,
                                curr_scrape_date,
                                prev_price,
                                prev_scrape_date,
                                sold_date,
                                sold,
                                listing_url,
                                source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        new_products_list_as_tuples = [(product['product_id'],
                                        product['brand'],
                                        product['product_name'],
                                        product['curr_price'],
                                        product['curr_scrape_date'],
                                        product['prev_price'],
                                        product['prev_scrape_date'],
                                        product['sold_date'],
                                        product['sold'],
                                        product['listing_url'],
                                        product['source'])
                                       for product in new_products]

        cur.executemany(new_product_query, new_products_list_as_tuples)
        conn.commit()

        if cur.rowcount > 0:
            print(f"Bulk insert successful. {cur.rowcount} rows inserted.")
            return True
        else:
            raise Exception("No rows were inserted during bulk insert.")

    except Exception as e:
        print(f"(BULK_INSERT_NEW) Error: {e}")
        conn.rollback()
        raise Exception(f"BULK_INSERT_FAILED: {e}")  # Raising detailed exception
    finally:
        cur.close()
        conn.close()

# Helper function to convert Decimal and Date to JSON-serializable format
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, date):
        return obj.isoformat()  # Convert datetime.date to ISO format string
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def DB_get_sold_daily(source, items_not_found_dict, file_scrape_date, spec_item=None):
    conn = None
    cur = None
    try:
        # Use the existing get_db_connection function
        conn = get_db_connection()
        cur = conn.cursor()

        if not items_not_found_dict:  # If no items to check, return empty dict
            return {}

        # Prepare queries
        spec_item_sold_query = """
            SELECT * FROM products 
            WHERE sold = %s 
            AND source = %s 
            AND product_name = %s 
            AND sold_date = %s
        """
        general_sold_query = """
            SELECT * FROM products 
            WHERE sold = %s 
            AND source = %s 
            AND sold_date = %s
        """

        print(chalk.magenta(f"Checking sold items for: {items_not_found_dict}"))

        # Execute appropriate query based on spec_item
        if spec_item is not None:
            cur.execute(spec_item_sold_query, ('t', source, spec_item, file_scrape_date))
        else:
            cur.execute(general_sold_query, ('t', source, file_scrape_date))

        result = cur.fetchall()
        print(chalk.red(f"Result from db: {result}"))

        if not result:  # If no results found, return empty dict
            return {}

        # Convert results to dictionary
        sold_items_to_dict = {}
        for row in result:
            product_id = row[0]
            sold_items_to_dict[product_id] = {
                'product_name': row[2],
                'curr_price': float(row[3]) if isinstance(row[3], decimal.Decimal) else row[3],
                'curr_scrape_date': row[4].strftime('%Y-%m-%d') if row[4] else None,
                'prev_price': float(row[5]) if isinstance(row[5], decimal.Decimal) else row[5],
                'prev_scrape_date': row[6].strftime('%Y-%m-%d') if row[6] else None,
                'sold_date': row[7].strftime('%Y-%m-%d') if row[7] else None,
                'sold': 'True' if row[8] else 'False',
                'url': row[9],
                'source': row[10]
            }

        return sold_items_to_dict

    except Exception as e:
        print(chalk.red(f"Error in DB_get_sold_daily: {e}"))
        if conn:
            conn.rollback()
        return {}  # Return empty dict on error
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
       


def DB_bulk_update_sold(sold_products):
    try:
        if not sold_products:
            return True #True for success case
        
        conn = get_db_connection()
        cur = conn.cursor()



        #first get current sold status for all products - we want to avoid processing already sold items
        product_ids = list(sold_products.keys())
        check_status_query = """
            SELECT product_id, sold, sold_date
            FROM products
            WHERE product_id = ANY(%s)
        """
        
        cur.execute(check_status_query, (product_ids,))
        current_statuses = cur.fetchall()

         # Convert to dict for easy lookup
        already_sold = {
            row[0]: {'sold': row[1], 'sold_date': row[2]} 
            for row in current_statuses 
            if row[1]  # only include if sold is True
        }

        # Filter out products that are already marked as sold
        products_to_update = {}
        for product_id, product_data in sold_products.items():
            if product_id in already_sold:
                print(chalk.yellow(f"Skipping {product_id} - already marked sold on {already_sold[product_id]['sold_date']}"))
                continue
            products_to_update[product_id] = product_data

        if not products_to_update:
            print(chalk.yellow("No new products to mark as sold"))
            return True
        
        print(chalk.blue(f"Found {len(already_sold)} already sold items"))

        update_sold_query = """
                            UPDATE products
                            SET sold_date = %s,
                                sold = %s
                            WHERE product_id = %s
                            """
        update_data_as_tuples = []
        for k, v in sold_products.items():
            product_id = k
            sold_date = v['curr_scrape_date']
            sold = True
            update_data_as_tuples.append((sold_date, sold, product_id))

        cur.executemany(update_sold_query, update_data_as_tuples)
        conn.commit()

        if cur.rowcount > 0:
            print(f"Bulk update of sold products successful. {cur.rowcount} rows updated.")
            return True
        else:
            raise Exception("No rows were updated for sold status.")

    except Exception as e:
        print(f"(BULK_UPDATE-SOLD) Error: {e}")
        conn.rollback()
        raise Exception(f"BULK_UPDATE_SOLD_FAILED: {e}")  # Raising detailed exception
    finally:
        cur.close()
        conn.close()

      

    # print(chalk.blue(f"sold_items as dict {sold_items_to_dict}"))

    # # Serialize and pretty print the result with the custom JSON serializer
    # print(json.dumps(sold_items, indent=2, default=decimal_default))


if __name__ == "__main__":
    # print(DB_get_sold("ITALIST"))
    # print(DB_get_sold("ITALIST","EMBROIDERED FABRIC SMALL SYMBOLE SHOPPING BAG"))
    print(DB_fetch_product_ids_prices_dates('PRADA','ITALIST','EMBROIDERED FABRIC SMALL SYMBOLE SHOPPING BAG'))