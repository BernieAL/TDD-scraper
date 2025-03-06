import pika
import os
import sys,csv
import json
from simple_chalk import chalk
import boto3

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



from analysis.compare_data import compare_driver
from rbmq.price_change_producer import PRICE_publish_to_queue
from rbmq.process_producer import PROCESS_publish_to_queue
from config.connections import create_rabbitmq_connection


process_info = {
    "query_hash": None,
    "output_dir": None,
    "specific_item": None,
    "paths": {},
    "price_changes": []
}

def reset_process_info():
    process_info.update({
        "query_hash": None,
        "output_dir": None,
        "specific_item": None,
        "price_changes": [],
        "paths": {}
    })

def safe_compare_driver(data_stream, query_hash, msg, specific_item):
    """Updated to handle S3 data stream"""
    try:
        # Read data from S3 stream
        csv_data = data_stream.read().decode('utf-8').splitlines()
        reader = csv.reader(csv_data)

        # Skip headers
        next(reader) #skipping date
        next(reader) #skipping query
        next(reader) #skipping headers
        next(reader) #skipping delim

        # Check if empty
        if not list(reader):
            print(chalk.yellow("File is empty (only contains headers)"))
            source = msg.get('source', 'unknown')
            PRICE_publish_to_queue({
                "type": "PROCESSING_SCRAPED_FILE_COMPLETE",
                "query_hash": query_hash,
                "product_name": specific_item,
                "scrape_file_empty": True,
                "source": source
            })
            return True

        # Process data
        compare_driver(csv_data, query_hash, msg, specific_item)
        return True
    except Exception as e:
        if "duplicate key value" in str(e):
            print(chalk.yellow(f"Skipping duplicate product in {msg.get('source', 'unknown')}"))
            return True
        elif "cur referenced before assignment" in str(e):
            print(chalk.red(f"Database connection error: {e}"))
            return False
        else:
            print(chalk.red(f"Unexpected error in compare_driver: {e}"))
            return False

def main():
    def callback(ch, method, properties, body):
        try:
            print(chalk.yellow("Received message on compare_queue"))
            msg = json.loads(body)
            print(chalk.yellow(f"Message content: {msg}"))

            # Get environment variables
            bucket = os.environ['S3_BUCKET']
            query_hash = os.environ['QUERY_HASH']
            raw_path = os.environ['RAW_PATH']
            analysis_path = os.environ['ANALYSIS_PATH']
            reports_path = os.environ['REPORTS_PATH']

            if msg.get('type') == 'POPULATED_OUTPUT_DIR':
                try:
                    reset_process_info()
                    process_info.update({
                        'query_hash': query_hash,
                        'output_dir': raw_path,  # Use S3 raw path
                        'specific_item': msg.get('specific_item'),
                        'price_changes': [],
                        'paths': {
                            'raw_path': raw_path,
                            'analysis_path': analysis_path,
                            'reports_path': reports_path
                        }
                    })

                    # Get files from S3 instead of local directory
                    s3 = boto3.client('s3')
                    response = s3.list_objects_v2(
                        Bucket=bucket,
                        Prefix=raw_path
                    )

                    successful_files = 0
                    total_files = 0

                    for obj in response.get('Contents', []):
                        total_files += 1
                        file_key = obj['Key']
                        
                        # Download file from S3 for processing
                        data = s3.get_object(
                            Bucket=bucket,
                            Key=file_key
                        )
                        
                        if safe_compare_driver(
                            data['Body'],  # Pass S3 file data
                            query_hash,
                            {'paths': process_info['paths']},
                            process_info['specific_item']
                        ):
                            successful_files += 1

                    if successful_files == 0:
                        raise Exception("No files were processed successfully")

                    print(chalk.green(f"Successfully processed {successful_files} out of {total_files} files"))

                    if process_info['price_changes']:
                        PRICE_publish_to_queue({
                            'type': 'PRICE_CHANGES_SUMMARY',
                            'query_hash': process_info['query_hash'],
                            'changes': process_info['price_changes'],
                            'paths': process_info['paths']
                        })

                    PROCESS_publish_to_queue({
                        'type': 'COMPARE',
                        'status': 'PASS',
                        'query_hash': process_info['query_hash'],
                        'output_dir': process_info['output_dir'],
                        'specific_item': msg.get('specific_item'),
                        'paths': process_info['paths']
                    })

                except Exception as e:
                    print(chalk.red(f"Error processing POPULATED_OUTPUT_DIR message: {e}"))
                    if process_info['query_hash']:
                        PROCESS_publish_to_queue({
                            'type': 'COMPARE',
                            'status': 'FAIL',
                            'query_hash': process_info['query_hash'],
                            'output_dir': process_info['output_dir'],
                            'specific_item': msg.get('specific_item'),
                            'paths': process_info['paths']
                        })

            elif msg.get('type') == 'PRICE_WORKER_COMPLETE':
                print(chalk.green("Received PRICE_WORKER_COMPLETE confirmation \n --------------- "))

            elif msg.get('type') == 'PRODUCT_PRICE_CHANGE':
                if process_info['query_hash']:
                    process_info['price_changes'].append(msg)
                    print(chalk.green(f"Added price change for product: {msg.get('product_id')}"))

        except Exception as e:
            print(chalk.red(f"Error processing message: {e}"))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    connection = None
    try:
        
        connection = create_rabbitmq_connection()
        channel = connection.channel()
        
        channel.queue_declare(queue='compare_queue', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='compare_queue', on_message_callback=callback)

        print(chalk.green("Clearing queue"))
        channel.queue_purge(queue='compare_queue')
        print(chalk.blue('(COMPARE_WORKER)[*] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()

    except Exception as e:
        print(chalk.red(f"Error in RabbitMQ setup: {e}"))
    finally:
        if connection and connection.is_open:
            try:
                connection.close()
            except Exception as e:
                print(chalk.red(f"Error closing connection: {e}"))

if __name__ == "__main__":
    main()
