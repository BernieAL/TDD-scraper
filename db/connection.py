import psycopg2
from simple_chalk import chalk
import time

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="designer_products",
    user="admin",
    password="admin!"
)


def get_db_connection():
    
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
    