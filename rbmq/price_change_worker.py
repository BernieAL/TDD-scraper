import time
import pika
import os,sys,json
from simple_chalk import chalk

# from dotenv import load_dotenv,find_dotenv
# load_dotenv(find_dotenv())

# from bot_selenium_scraper import perform_url_scrape

#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from Analysis.percent_change_analysis import  calc_percentage_diff_driver
from email_sender import send_email_with_report

def main():

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
    
    #list to hold recieved messages
    recd_products = []

    def callback(ch,method,properties,body):

        msg = json.loads(body)
        ch.basic_ack(delivery_tag = method.delivery_tag)
        print(f"Message received: {msg}")

        if msg.get('type') != 'BATCH_COMPLETE':
            print('product added to queue')
            recd_products.append(msg) 

        elif msg.get('type') == 'BATCH_COMPLETE':   
           

            print('begin batch price analysis')

            try:
                report_file_path = calc_percentage_diff_driver(recd_products)
                print(chalk.green("report generated"))
            except Exception as e:
                print(e)

            
            try:
                send_email_with_report('balmanzar883@gmail.com',report_file_path,'Prada bags')
                print(chalk.green("email sent"))
            except Exception as e:
                print(e)

            #clear list after processing
            recd_products.clear()

        # res = perform_url_scrape(url)
        # if res == True:
        #     print("scrape successful")
        # if res == False:
        #     print("scrape unsuccessful")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='price_change_queue',on_message_callback=callback)


      #listen indefinitley for messages
    print(chalk.blue('[*] waiting for messages. To exit press CTRL+C'))
    channel.start_consuming()

if __name__ == "__main__":
    main()