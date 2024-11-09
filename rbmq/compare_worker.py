import pika
import os
import sys
import json
from simple_chalk import chalk


# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from analysis.compare_data import compare_driver
from rbmq.price_change_producer import PRICE_publish_to_queue
from rbmq.process_producer import PROCESS_publish_to_queue

# Globals for tracking
process_info = {
    "brand": None,
    "category": None,
    "query_hash": None,
    "date": None,
    "price_report_subdir": None,
    "sold_report_subdir": None,
    "price_report_root_dir": None,
    "sold_report_root_dir": None,
    "source": None,
}

def main():
    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(chalk.yellow(f"Message received: {msg}"))

            if msg.get('type') == 'POPULATED_OUTPUT_DIR':
                output_dir = msg['output_dir']
                process_info['query_hash'] = msg['query_hash']
                spec_item = msg['specific_item']

                # Process each file in output_dir using compare_driver
                if os.path.isdir(output_dir):
                    for root, subdirs, files in os.walk(output_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            print(chalk.blue(f"Processing file: {file_path}"))
                            compare_driver(file_path, process_info['query_hash'], spec_item)
                    print("Finished processing files in output_dir.")

            elif msg.get('type') == 'PRICE_WORKER_COMPLETE':
                print('RECEIVED MSG FROM PRICE CHANGE WORKER')
                # Send COMPARE_COMPLETE message to main PROCESS queue
                PROCESS_publish_to_queue({'type': 'COMPARE_COMPLETE', 'query_hash': process_info['query_hash']})
                
                # PRICE_publish_to_queue(
                # {'type': 'PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY',
                #  'email': '(main)balmanzar883@gmail.com',
                #  'query_hash': process_info['query_hash']})
                print(chalk.green("COMPARE_COMPLETE message sent to process_queue."))

        except Exception as e:
            print(chalk.red(f"Error processing message: {e}"))

    # RabbitMQ setup
    try:
        connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        channel.queue_declare(queue='compare_queue', durable=True)

        # Start consuming messages
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='compare_queue', on_message_callback=callback)

        print(chalk.green("Clearing queue"))
        channel.queue_purge(queue='compare_queue')

        print(chalk.blue('(COMPARE_WORKER)[*] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()

    except Exception as e:
        print(chalk.red(f"Error during RabbitMQ setup or message consumption: {e}"))
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()
