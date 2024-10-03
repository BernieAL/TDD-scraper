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



def make_report_output_dir():

    """
    Creates the report output directory if it doesn't exist.
    """

    try:
        current_dir = os.path.abspath(os.path.dirname(__file__))
        root_dir = os.path.abspath(os.path.join(current_dir,".."))

        output_dir = os.path.join(root_dir,"OUTPUT_price_changes")

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        return output_dir
    
    except Exception as e:
        print(chalk.red(f"Error creating output directory: {e}"))
        
def main():

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
        print(chalk.red(f"Error connecting to RabbitMQ or declaring queue: {e}"))
        return  # Exit if we cannot connect to RabbitMQ
   
    #list to hold recieved messages
    recd_products = []

    #Create the output directory when the worker starts
    #dir will be used by percent_change_analysis and email script
    try:
        output_dir = make_report_output_dir()
    except Exception as e:
        print(chalk.red(f"Worker cannot proceed without output directory: {e}"))
        return  # Exit if directory creation fails

    def callback(ch,method,properties,body):

        try:
            msg = json.loads(body)
            print(f"Message received: {msg}")
        
            
            #check that msg is product to be added and not end signal
            if msg.get('type') != 'PROCESSING SCRAPED FILE COMPLETE' and msg.get('type') != 'PROCESSED ALL SCRAPED FILES FOR QUERY':
                print('product added to queue')
                recd_products.append(msg) 

            #check if msg is end signal for single file
            elif msg.get('type') == 'PROCESSING SCRAPED FILE COMPLETE': 

                source_file = msg.get('source_file')  # Get the source file name
                print(chalk.red(f"Processing source file: {source_file}"))
                print('begin batch price analysis')

                try:
                    calc_percentage_diff_driver(output_dir,recd_products,source_file)
                    print(chalk.green("report generated - stored in output dir"))
                except Exception as e:
                    print(e)

                # Clear product list after batch processing
                recd_products.clear()

            #check if msg is end signal for all files
            elif msg.get('type') == 'PROCESSED ALL SCRAPED FILES FOR QUERY':   
            
                
                try:
                    print(chalk.green("end signal rec'd - moving to craft email and attach reports"))
                    send_email_with_report('rosier101@gmail.com',output_dir,'Prada bags')
                    print(chalk.green("email sent"))
                except Exception as e:
                    print(e)
        except Exception as e:
             print(chalk.red(f"Error processing message: {e}"))
        finally:
            # Acknowledge the message only after processing it 
            ch.basic_ack(delivery_tag=method.delivery_tag)
     

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='price_change_queue',on_message_callback=callback)


    try:
        #listen indefinitley for messages
        print(chalk.blue('[*] waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()
    except Exception as e:
        print(chalk.red(f"Error during message consumption: {e}"))
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()