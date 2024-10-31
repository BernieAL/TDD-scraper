import pika
import os,sys
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
import json

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from analysis.compare_data import compare_driver
from rbmq.price_change_producer import PRICE_publish_to_queue
from rbmq.compare_producer import COMPARE_publish_to_queue


def main():
    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(chalk.yellow(f"Message received: {msg}"))

            output_dir = msg['output_dir']
            query_hash = msg['query_hash']
            spec_item = msg['specific_item']
            
            # Process each file in output_dir using compare_driver
            if os.path.isdir(output_dir):
                for root, subdirs, files in os.walk(output_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        print(chalk.blue(f"Processing file: {file_path}"))
                        compare_driver(file_path,spec_item)

            # Once processing is complete, send COMPARE_COMPLETE message
            COMPARE_publish_to_queue({'type': 'COMPARE_COMPLETE', 'output_dir': output_dir, 'query_hash': query_hash,'specific_item':spec_item})
            print(chalk.green("COMPARE_COMPLETE message sent."))

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