import sys, csv, json, os
import pika
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree

# Initialize paths
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

def exit_with_error(message):
    print(chalk.red(f"[CRITICAL ERROR] {message}"))
    sys.exit(1)

def main():
    def callback(ch, method, properties, body):  
        try:
            message = json.loads(body)
            print(f"\n{chalk.green('[NEW MESSAGE]')} Received: {message}")
            
            

            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(chalk.red(f"Error processing message: {e}"))
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # RabbitMQ setup
    try:
        connection_params = pika.ConnectionParameters( 
            host='localhost',
            port=5672,
            credentials=pika.PlainCredentials('guest', 'guest')  
        )

        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        channel.queue_declare(queue='user_search_requests', durable=True)  
        channel.basic_qos(prefetch_count=1) 
        channel.basic_consume(
            queue='user_search_requests', 
            on_message_callback=callback
        )
    
        print(chalk.green("Clearing queue"))
        channel.queue_purge(queue='user_search_requests')
        print(chalk.blue('[SYSTEM START] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()
    except Exception as e:
        exit_with_error(f"[ERROR] RabbitMQ setup or message consumption failed: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()