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

def main():

    
    def callback(ch, method, properties, body):
            
            try:
                msg = json.loads(body)
                print(chalk.yellow(f"Message received: {msg}"))

                output_dir = msg['output_dir']

                for _,subdirs,files in os.walk(output_dir):
                    
                    for file in files:
                        file_path = os.path.join(output_dir,file)
                        print(file_path)
                        compare_driver(file_path)
                        

            
            except Exception as e:
                print(chalk.red(f"Error processing message: {e}"))
            finally:
                ch.basic_ack(delivery_tag=method.delivery_tag)

       
        



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
