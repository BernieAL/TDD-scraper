import pika
import os,sys
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
import json

"""
Producer script 
    -for publishing products to queue - that have observed price changes
    -will be imported and used inside comparison script - comparing scraped price vs db price of product
    -publishes product_id, price, prev price, and url to queue as an object

"""

#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


def publish_to_queue(product_msg):
    # Set up the connection parameters (use correct RabbitMQ host and credentials)
    connection_params = pika.ConnectionParameters(
        host='localhost',  # Ensure this is the correct host, change if necessary
        port=5672,          # Default RabbitMQ port
        credentials=pika.PlainCredentials('guest', 'guest')
    )

    # Establish the connection using the connection_params object directly
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    
    channel.queue_declare(queue='price_change_queue',durable=True)
    

    #if not not end signal message - convert dates to strings to not throw serialization error cause of datetime format
    if product_msg.get('type') != 'BATCH_COMPLETE':
        product_msg['curr_scrape_date'] = (product_msg['curr_scrape_date']).strftime('%Y-%m-%d')
        product_msg['prev_scrape_date'] = (product_msg['prev_scrape_date']).strftime('%Y-%m-%d')

    #   'curr_scrape_date': scrape_date,
    #                 'prev_price': existing_product_id_prices_dict[row['product_id']]['prev_price'],
    #                 'prev_scrape_date': existing_product_id_prices_dict[row['product_id']]['prev_scrape_date']
    #                }
    
    # publish product to quueue
    channel.basic_publish(exchange='', 
                          routing_key='price_change_queue', 
                          body=json.dumps(product_msg),
                          properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
    print(f"Sent task: {product_msg}")
    connection.close()

if __name__ == "__main__":
    urls_to_scrape = ['https://example.com', 'https://anotherexample.com']
    # for url in urls_to_scrape:
    #    add_to_queue(url)