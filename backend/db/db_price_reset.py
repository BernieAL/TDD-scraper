"""
this is just for testing - it will reset the curr prices of each item in the db to 0
This is for testing locally
"""

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
    

def reset_all_prices_to_999():
    try:
        conn = get_db_connection()
        update_query = """UPDATE products SET curr_price = 999;"""
        cur = conn.cursor()
        cur.execute(update_query)

        conn.commit()
        print("All product prices have been reset to 0.")
        
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed.")
    
    except Exception as e:
        print(f"reset to 999 - process failed{e}")


if __name__ == "__main__":

    reset_all_prices_to_999()