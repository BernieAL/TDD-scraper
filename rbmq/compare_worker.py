import pika
import os
import sys
import json
from simple_chalk import chalk

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from analysis.compare_data import compare_driver
from rbmq.price_change_producer import PRICE_publish_to_queue
from rbmq.process_producer import PROCESS_publish_to_queue

# Global state management
process_info = {
    "query_hash": None,
    "output_dir": None,
    "specific_item": None
}

def reset_process_info():
    for key in process_info:
        process_info[key] = None

def safe_compare_driver(file_path, query_hash, specific_item):
    """Wrapper function to handle database errors gracefully"""
    try:
        compare_driver(file_path, query_hash, specific_item)
        return True
    except Exception as e:
        if "duplicate key value" in str(e):
            # Log but don't fail on duplicate keys
            print(chalk.yellow(f"Skipping duplicate product in {file_path}"))
            return True
        elif "cur referenced before assignment" in str(e):
            # Database connection issue
            print(chalk.red(f"Database connection error: {e}"))
            return False
        else:
            print(chalk.red(f"Unexpected error in compare_driver: {e}"))
            return False

def main():
    def callback(ch, method, properties, body):  # Make sure body is included as parameter
        try:
            print(chalk.yellow("Received message on compare_queue"))
            msg = json.loads(body)  # Now body will be defined
            print(chalk.yellow(f"Message content: {msg}"))
            
            if msg.get('type') == 'POPULATED_OUTPUT_DIR':
                try:
                    reset_process_info()
                    
                    process_info.update({
                        'output_dir': msg['output_dir'],
                        'query_hash': msg['query_hash'],
                        'specific_item': msg.get('specific_item'),
                        'price_changes': []  # Add tracking for price changes
                    })

                    print(chalk.blue(f"Processing output directory: {process_info['output_dir']}"))
                    
                    if os.path.isdir(process_info['output_dir']):
                        successful_files = 0
                        total_files = 0
                        
                        #read files in output dir
                        for root, _, files in os.walk(process_info['output_dir']):
                            for file in files:
                                total_files += 1
                                file_path = os.path.join(root, file)
                                print(chalk.blue(f"Processing file: {file_path}"))
                                
                                if safe_compare_driver(
                                    file_path,
                                    process_info['query_hash'],
                                    process_info['specific_item']
                                ):
                                    successful_files += 1
                        
                        # Only send COMPARE_COMPLETE if processing was successful
                        if successful_files > 0:
                            print(chalk.green(f"Successfully processed {successful_files} out of {total_files} files"))
                            
                            # Send price changes summary if any were detected
                            if process_info['price_changes']:
                                PRICE_publish_to_queue({
                                    'type': 'PRICE_CHANGES_SUMMARY',
                                    'query_hash': process_info['query_hash'],
                                    'changes': process_info['price_changes']
                                })
                            
                            PROCESS_publish_to_queue({
                                'type': 'COMPARE',
                                'status':'PASS',
                                'query_hash': process_info['query_hash'],
                                'output_dir': process_info['output_dir'],
                                'specific_item': msg.get('specific_item'),  # Forward specific_item if present
                            })
                            
                        else:
                            raise Exception("No files were processed successfully")
                    else:
                        raise Exception(f"Directory not found: {process_info['output_dir']}")

                except Exception as e:
                    print(chalk.red(f"Error processing POPULATED_OUTPUT_DIR message: {e}"))
                    # Send error message
                    if process_info['query_hash']:
                        fail_msg = {
                            'type': 'COMPARE',
                            'status':'FAIL',
                            'query_hash': process_info['query_hash'],
                            'output_dir': process_info['output_dir'],
                            'specific_item': msg.get('specific_item'),  # Forward specific_item if present
                        }
                        PROCESS_publish_to_queue(fail_msg)

                        

            elif msg.get('type') == 'PRICE_WORKER_COMPLETE':
                print(chalk.green("Received PRICE_WORKER_COMPLETE confirmation \n --------------- "))

            elif msg.get('type') == 'PRODUCT_PRICE_CHANGE':
                # Track price changes
                if process_info['query_hash']:
                    process_info['price_changes'].append(msg)
                    print(chalk.green(f"Added price change for product: {msg.get('product_id')}"))

        except Exception as e:
            print(chalk.red(f"Error processing message: {e}"))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # RabbitMQ setup
    connection = None
    try:
        connection_params = pika.ConnectionParameters(
            host='localhost',
            port=5672,
            credentials=pika.PlainCredentials('guest', 'guest'),
            heartbeat=600
        )
        connection = pika.BlockingConnection(connection_params)
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