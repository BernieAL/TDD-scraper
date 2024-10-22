from datetime import datetime
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
    """
    publishes rec'd messages to queue

    :param: product obj as dictionary to be added to queue

        temp = {
            'product_id': row['product_id'],
            'curr_price': product_data['curr_price'],
            'curr_scrape_date': scrape_date,
            'prev_price': product_data['prev_price'],
            'prev_scrape_date': product_data['prev_scrape_date']
            'product_name': row['product_name']
            'listing_url': row['listing_url']
        }
        
    """
    try:
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

    except Exception as e:
        print(chalk.red(f"Error publishing message to queue: {e}"))

    try:
        
        # #if type is sold items, publish list of sold items to queue
        # if product_msg.get('type') == 'PROCESSING SOLD ITEMS COMPLETE':
        #     print(product_msg)
        #     # publish sold items list to queue
        #     channel.basic_publish(exchange='', 
        #                         routing_key='price_change_queue', 
        #                         body=json.dumps(product_msg),
        #                         properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))

        #if not end signal message - convert dates to strings to not throw serialization error cause of datetime format
        if product_msg.get('type') != 'PROCESSING SCRAPED FILE COMPLETE' and product_msg.get('type') != 'PROCESSED ALL SCRAPED FILES FOR QUERY' != 'PROCESSING SOLD ITEMS COMPLETE':
            product_msg['curr_scrape_date'] = (product_msg['curr_scrape_date']).strftime('%Y-%m-%d')
            product_msg['prev_scrape_date'] = (product_msg['prev_scrape_date']).strftime('%Y-%m-%d')


    
        # publish product to quueue
        channel.basic_publish(exchange='', 
                                routing_key='price_change_queue', 
                                body=json.dumps(product_msg),
                                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
        print(chalk.green(f"Successfully sent task to queue: {product_msg}"))

    except Exception as e:
        print(chalk.red(f"error publishing message to queue{e}"))
    
    finally:
        # Close the connection to RabbitMQ
        try:
            connection.close()
        except Exception as e:
            print(chalk.red(f"Error closing RabbitMQ connection: {e}"))

    
  
if __name__ == "__main__":
    
   
    # Prepare a sample product message for each URL (just as an example)
    product_message = {
        'product_id': '12345',
        'curr_price': 100.0,
        'prev_price': 95.0,
        'curr_scrape_date': datetime.now(),  # Example dates
        'prev_scrape_date': datetime.now(),
        'listing_url': "test.com",
        'type': 'PRODUCT_CHANGE'  # Example message type
    }
    publish_to_queue(product_message)
    publish_to_queue({'type': 'PROCESSING SCRAPED FILE COMPLETE'})