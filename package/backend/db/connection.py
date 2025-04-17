import psycopg2
from simple_chalk import chalk
import time,sys,os
from dotenv import load_dotenv,find_dotenv

#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    
# For Docker
if os.getenv('RUNNING_IN_DOCKER') == '1' and '/app' not in sys.path:
    sys.path.insert(0, '/app')


from config import DB_URI

# # Connect to PostgreSQL
# conn = psycopg2.connect(
#     host="localhost",
#     port="5433",
#     database="designer_products",
#     user="admin",
#     password="admin!"
# )


def get_db_connection():
    
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
    