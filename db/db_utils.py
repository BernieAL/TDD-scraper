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
        query = "SELECT product_id, last_price FROM products WHERE brand = %s"
        cur.execute(query,(brand,))
        result = cur.fetchall()
        
        # for row in result:
        #     product_id,last_price = row
        #     print(f"Product ID: {product_id}, Last Price: {last_price}")

        # Convert to list of dictionaries
        products = [{'product_id':row[0], 'last_price': (row[1])} for row in result]

        return products
        
    except Exception as e:
        print(f"(fetch_products_ids) Error: {Exception}")
    

if __name__ == "__main__":
    # print(fetch_product_ids('Prada'))
    print(fetch_product_ids_and_prices('Prada'))
       