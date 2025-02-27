import sys, csv, json, os
import pika
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree

# For local development
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# For Docker
if os.getenv('RUNNING_IN_DOCKER') == '1' and '/app' not in sys.path:
    sys.path.insert(0, '/app')

def ensure_init_files():
    """
    for each dir in list dir, 
    check if each dir has __init__.py,
    if not make one. 

    This avoids the module not found issue that results
    from copying specific files without including __init__.py

    """
    #get current dir contents
    dirs = os.listdir()

    for dir in dirs:

        #get full path
        full_path = os.path.abspath(dir)

        #check if its a dir
        if os.path.isdir(full_path):
            
            #build path for __init__ file in curr dir
            init_file_path = os.path.join(full_path,'__init__.py')

            #check if init file exists
            if not os.path.exists(init_file_path):
                print(f"Creating __init__.py file for dir: {dir}")

                with open(init_file_path, 'w') as f:
                    pass
            else:
                print(f"__init__.py already exists for {dir}")

ensure_init_files()  


from src.main_app_driver import driver_function_from_search_form
from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection

def exit_with_error(msg):
    print(chalk.red(f"[CRITICAL ERROR] {msg}"))
    sys.exit(1)

def main():
    def callback(ch, method, properties, body):  
        try:
            msg = json.loads(body)
            print(f"\n{chalk.green('[NEW msg]')} Received: {msg}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

            driver_function_from_search_form(msg)
        except Exception as e:
            print(chalk.red(f"Error processing msg: {e}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # RabbitMQ setup
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()

        channel.queue_declare(queue='user_search_requests', durable=True)  
        channel.basic_qos(prefetch_count=1) 
        channel.basic_consume(
            queue='user_search_requests', 
            on_message_callback=callback
        )
    
        print(chalk.green("Clearing queue"))
        channel.queue_purge(queue='user_search_requests')
        print(chalk.blue('[SYSTEM START] Waiting for msgs. To exit press CTRL+C'))
        channel.start_consuming()
    except Exception as e:
        exit_with_error(f"[ERROR] RabbitMQ setup or msg consumption failed: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()