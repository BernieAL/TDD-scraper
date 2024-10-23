import sys,csv,json,os
import pika
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree  # For removing directories

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from Analysis.percent_change_analysis import calc_percentage_diff_driver
from email_sender import send_email_with_report

def make_price_report_root_dir():
    """
    Creates the report output directory if it doesn't exist.
    """
    try:
        current_dir = os.path.abspath(os.path.dirname(__file__))
        root_dir = os.path.abspath(os.path.join(current_dir, ".."))
        price_report_root_dir = os.path.join(root_dir, "price_report_output")

        if not os.path.exists(price_report_root_dir):
            os.makedirs(price_report_root_dir)

        return price_report_root_dir
    except Exception as e:
        print(chalk.red(f"Error creating output directory: {e}"))
        return None

def make_price_report_subdir(price_report_root_dir,brand,category,query_hash):
        """
        Creates a new subdir in price_report output root for this specific query.
        Will return subdir path

        :param brand: Brand name (e.g., Prada)
        :param category: Category of the product (e.g., Bags)
        :param query_hash: A hash for the query
        :return: The path of the created directory
        """
        current_date = datetime.now().strftime('%Y-%d-%m')
        try:
            # Build the directory name using the brand, query, and hash
            dir_name = f"PRICE_REPORT_{brand}_{current_date}_{category}_{query_hash}"
            new_sub_dir = os.path.join(price_report_root_dir, dir_name)
            
            print(f"Attempting to create directory: {new_sub_dir}")
            
            # Check if the directory already exists; if not, create it
            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
                print(f"Directory created: {new_sub_dir}")
            else:
                print(f"Directory already exists: {new_sub_dir}")
            
            return new_sub_dir
        
        except Exception as e:
            print(f"Error while creating sub-directory for raw scrape: {e}")
            return None

def make_sold_report_root_dir():
    """
    Creates the report output directory if it doesn't exist.
    """
    try:
        current_dir = os.path.abspath(os.path.dirname(__file__))
        root_dir = os.path.abspath(os.path.join(current_dir, ".."))
        sold_report_root_dir = os.path.join(root_dir, "sold_report_output")

        if not os.path.exists(sold_report_root_dir):
            os.makedirs(sold_report_root_dir)

        return sold_report_root_dir
    except Exception as e:
        print(chalk.red(f"Error creating output directory: {e}"))
        return None
    

def make_sold_report_subdir(sold_report_root_dir,brand,category,query_hash):
        """
        Creates a new subdir in sold_report output root for this specific query.
        Will return subdir path

        :param brand: Brand name (e.g., Prada)
        :param category: Category of the product (e.g., Bags)
        :param query_hash: A hash for the query
        :return: The path of the created directory
        """
        current_date = datetime.now().strftime('%Y-%d-%m')
        try:
            # Build the directory name using the brand, query, and hash
            dir_name = f"SOLD_REPORT_{brand}_{current_date}_{category}_{query_hash}"
            new_sub_dir = os.path.join(sold_report_root_dir, dir_name)
            
            print(f"Attempting to create directory: {new_sub_dir}")
            
            # Check if the directory already exists; if not, create it
            if not os.path.exists(new_sub_dir):
                os.makedirs(new_sub_dir)
                print(f"Directory created: {new_sub_dir}")
            else:
                print(f"Directory already exists: {new_sub_dir}")
            
            return new_sub_dir
        
        except Exception as e:
            print(f"Error while creating sub-directory for raw scrape: {e}")
            return None

def parse_file_name(file):
    """
    filenames recieved are in the same format.
    possible filenames recieved:
        FILTERED_italist_prada_2024-14-10_bags_c4672843.csv
        RAW_italist_prada_2024-14-10_bags_f3f28ac8.csv
    """
    file_path_tokens = file.split('/')[-1]
    file_name_tokens = file_path_tokens.split('_')
    source = file_name_tokens[1]
    brand = file_name_tokens[2]
    date = file_name_tokens[3]
    category = file_name_tokens[4]
    query_hash = file_name_tokens[-1].split('.')[0]

    print(chalk.blue(f"file_name_tokens {file_name_tokens}"))
    print(chalk.blue(f"source: {source}"))
    print(chalk.blue(f"brand {brand}"))
    print(chalk.blue(f"date: {date}"))
    print(chalk.blue(f"category {category}"))
    print(chalk.blue(f"query hash: {query_hash}"))

    return source, date, brand, category, query_hash

def main():

    recd_products = []  # Reset received products for each callback
    no_change_sources = []  # Track sources with no price changes

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            # print(f"Message received: {msg}")


            if 'source_file' in msg:
                source_file = msg['source_file']
                print(f"Received source file for processing: {source_file}")

                try:
                    source, date, brand, category, query_hash = parse_file_name(source_file)
                    print(chalk.green(f"Parsed values - Brand: {brand}, Category: {category}, Query Hash: {query_hash}"))
                except Exception as e:
                    print(chalk.red(f"Parse filename failed: {e}"))
                    return  # Exit if filename parsing fails

                price_report_root_dir = make_price_report_root_dir()
                if not price_report_root_dir:
                    print(chalk.red("Failed to create or access the price report root directory. Exiting."))
                    return

                # Create price_report_subdir early, even if no changes are found
                price_report_subdir = make_price_report_subdir(price_report_root_dir, brand, category, query_hash)

                sold_report_root_dir = make_sold_report_root_dir()
                if not sold_report_root_dir:
                    print(chalk.red("Failed to create or access the sold report root directory. Exiting."))
                    return

                # Create sold_report_subdir early, even if no changes are found
                sold_report_subdir = make_sold_report_subdir(sold_report_root_dir, brand, category, query_hash)
            
                if msg.get('type') == 'PROCESSING SOLD ITEMS COMPLETE':
                    sold_items = msg['sold_items']
                    sold_items_file_name = f"SOLD_{source}_{brand}_{date}_{query_hash}.csv"
                    sold_items_file_path = os.path.join(sold_report_subdir,sold_items_file_name)
                    
                    # print(chalk.red(f"SOLD ITEMS:{sold_items} "))

                    with open(sold_items_file_path,'w',newline='',encoding='utf-8') as file:
                        
                        fieldnames = ['product_id', 'curr_price', 'curr_scrape_date', 'prev_price', 'prev_scrape_date', 'sold_date', 'sold', 'url', 'source']
                        
                        #csv dictwriter
                        writer = csv.DictWriter(file,fieldnames=fieldnames)

                        #write headers
                        writer.writeheader()

                        for product_id,product_data in sold_items.items():
                            row = {'product_id': product_id}
                            row.update(product_data)  # Add the rest of the nested dictionary as columns
                            writer.writerow(row)


                
                    print(f"SOLD ITEMS added to queue.")

                # Now process the product messages or the end signal
                if msg.get('type') not in ['PROCESSING SCRAPED FILE COMPLETE', 'PROCESSED ALL SCRAPED FILES FOR QUERY', 'PROCESSING SOLD ITEMS COMPLETE']:
                    recd_products.append(msg)
                    print(f"Product added to queue. Current count: {len(recd_products)}")

                elif msg.get('type') == 'PROCESSING SCRAPED FILE COMPLETE':
                    print(chalk.green("SIGNAL RECD: PROCESSING SCRAPED FILE COMPLETE"))
                    if recd_products:
                        try:
                            # Process received products and generate the report
                            calc_percentage_diff_driver(price_report_subdir, recd_products, source_file)
                            print(chalk.green("Report generated and stored in output directory"))
                        except Exception as e:
                            print(chalk.red(f"Error in processing products: {e}"))
                    else:
                        print(chalk.red("No products received; adding to no_change_sources list"))
                        query = f"{brand}-{category}"
                        no_change_sources.append(f"{source}_{query}")
                        print(f"No change sources updated: {no_change_sources}")

                elif msg.get('type') == 'PROCESSED ALL SCRAPED FILES FOR QUERY':
                    print(chalk.green("SIGNAL RECD: PROCESSED ALL SCRAPED FILES FOR QUERY"))
                    if brand and category and query_hash:
                        try:
                            print(chalk.green(f"Sending email with price report from subdir: {price_report_subdir}"))
                            # print(chalk.green(f"And with sold report from subdir: {sold_report_subdir}"))
                            
                            send_email_with_report('balmanzar883@gmail.com', price_report_subdir,sold_report_subdir, f"{brand}_{category}", no_change_sources)
                            print(chalk.green("Email sent successfully"))
                            no_change_sources.clear()
                        except Exception as e:
                            print(chalk.red(f"Error during email sending: {e}"))
                    else:
                        print(chalk.red("Email not sent due to missing brand, category, or query_hash values"))

                    # Clear the queue **after** email is sent and processing is done
                    #MUST clear queue to remove prev query published messages
                    channel.queue_purge(queue='price_change_queue')
                    print(chalk.green("Queue cleared and ready for the next query"))

        except Exception as e:
            print(chalk.red(f"Error processing message: {e}"))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message

    # RabbitMQ setup
    try:
        connection_params = pika.ConnectionParameters(host='localhost', port=5672, credentials=pika.PlainCredentials('guest', 'guest'))
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        channel.queue_declare(queue='price_change_queue', durable=True)

        # Start consuming messages
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='price_change_queue', on_message_callback=callback)
        
        # print(chalk.green("Clearing queue"))
        # channel.queue_purge(queue='price_change_queue')

        print(chalk.blue('[*] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()

    except Exception as e:
        print(chalk.red(f"Error during RabbitMQ setup or message consumption: {e}"))
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()
