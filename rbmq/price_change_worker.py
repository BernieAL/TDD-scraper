import sys, csv, json, os
import pika
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree

# Initialize paths
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from analysis.percent_change_analysis import calc_percentage_diff_driver
from rbmq.compare_producer import COMPARE_publish_to_queue
from rbmq.process_producer import PROCESS_publish_to_queue
from email_sender import send_email_with_report

# Globals to hold current query data in mem
curr_query_info = {
    "brand": None,
    "category": None,
    "query_hash": None,
    "product_name": None,
    "date": None,
    "price_report_subdir": None,
    "sold_report_subdir": None,
    "price_report_root_dir": None,
    "sold_report_root_dir": None,
    "source": None,
    "source_file": None
}

# for tracking process status for current query
process_status = {
    'NEW_QUERY_MSG': False,
    'PROCESSING_SOLD_ITEMS_COMPLETE': False,
    'PROCESSING_SCRAPED_FILE_COMPLETE': False,
}

recd_products = []  # Global list to store products for each query

def reset_query_info():
    """Completely clear process_info and recd_products for a new query."""
    for key in curr_query_info:
        curr_query_info[key] = None
    global recd_products
    recd_products.clear()  # Clear recd_products to avoid data carryover
    print(chalk.blue("[INFO] Reset process_info and recd_products for a new query "))

def reset_process_status():
    for key in process_status:
        process_status[key] = False

def processes_up_to_end_signal():
    """Check if all required processes are complete before sending email"""
    required_statuses = {
        'NEW_QUERY_MSG': True,
        'PROCESSING_SCRAPED_FILE_COMPLETE': True,
        'PROCESSING_SOLD_ITEMS_COMPLETE': True
    }
    
    current_status = {k: process_status[k] for k in required_statuses.keys()}
    is_ready = all(current_status.values())
    
    if not is_ready:
        print(chalk.yellow(f"[INFO] Waiting for processes to complete. Current status:"))
        for k, v in current_status.items():
            print(chalk.yellow(f"  - {k}: {v}"))
    
    return is_ready
def exit_with_error(message):
    print(chalk.red(f"[CRITICAL ERROR] {message}"))
    sys.exit(1)

# Directory creation functions
def make_price_report_root_dir():
    try:
        root_dir = os.path.join(parent_dir, "price_report_output")
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
        return root_dir
    except Exception as e:
        exit_with_error(f"Error creating price report output directory: {e}")

def make_price_report_subdir(root_dir, brand, category, query_hash):
    try:
        subdir = os.path.join(root_dir, f"PRICE_REPORT_{brand}_{category}_{query_hash}")
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        return subdir
    except Exception as e:
        exit_with_error(f"Error creating price report subdirectory: {e}")

def make_sold_report_root_dir():
    try:
        root_dir = os.path.join(parent_dir, "sold_report_output")
        if not os.path.exists(root_dir):
            os.makedirs(root_dir)
        return root_dir
    except Exception as e:
        exit_with_error(f"Error creating sold report output directory: {e}")

def make_sold_report_subdir(root_dir, brand, category, query_hash):
    try:
        subdir = os.path.join(root_dir, f"SOLD_REPORT_{brand}_{category}_{query_hash}")
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        return subdir
    except Exception as e:
        exit_with_error(f"Error creating sold report subdirectory: {e}")

def parse_file_name(file):
    try:
        tokens = file.split('/')[-1].split('_')
        source, brand, date, category, query_hash = tokens[1], tokens[2], tokens[3], tokens[4], tokens[-1].split('.')[0]
        print(chalk.blue(f"[INFO] Parsed filename tokens: {tokens}"))
        return source, date, brand, category, query_hash
    except IndexError as e:
        exit_with_error(f"Filename parsing error: {e}")

def extract_query_hash(file_path):
    """Extract query hash from file path"""
    try:
        filename = file_path.split('/')[-1]
        return filename.split('_')[-1].split('.')[0]
    except Exception as e:
        print(chalk.red(f"Error extracting query hash: {e}"))
        return None

def main():
    no_change_sources = []

    # def callback(ch, method, properties, body):
    #     try:
    #         msg = json.loads(body)
    #         print(f"\n{chalk.green('[NEW MESSAGE]')} Received: {msg}")

    #         if msg.get('type') == "NEW_QUERY":
    #             try:
    #                 current_query_hash = extract_query_hash(msg['source_file'])
    #                 if current_query_hash != curr_query_info.get('query_hash'):
    #                     reset_query_info()
    #                     source_file = msg['source_file']
    #                     print(chalk.green(f"[PROCESSING] :::NEW QUERY::: \n New source file: {source_file}"))
                        
    #                     # Parse filename to populate curr_query_info for the new query
    #                     source, date, brand, category, query_hash = parse_file_name(source_file)
    #                     curr_query_info.update({
    #                         'source': source,
    #                         'date': date,
    #                         'category': category,
    #                         'brand': brand,
    #                         'query_hash': query_hash,
    #                         "price_report_root_dir": make_price_report_root_dir(),
    #                         "sold_report_root_dir": make_sold_report_root_dir(),
    #                         "source_file": source_file
    #                     })
    #                     curr_query_info['price_report_subdir'] = make_price_report_subdir(
    #                         curr_query_info['price_report_root_dir'], brand, category, query_hash
    #                     )
    #                     curr_query_info['sold_report_subdir'] = make_sold_report_subdir(
    #                         curr_query_info['sold_report_root_dir'], brand, category, query_hash
    #                     )
    #                     print(chalk.blue(f"[INFO] Process info updated for new file \n ------------- \n"))
    #                 else:
    #                     print(chalk.yellow("[INFO] Same query_hash - maintaining existing products"))

    #                 #update process status for this query
    #                 process_status['NEW_QUERY_MSG'] = True

    #             except Exception as e:
    #                 print(chalk.red(f"CALLBACK - CASE - NEW QUERY ERROR: {e}"))

    #         elif msg.get('type') == 'PRODUCT_PRICE_CHANGE':
    #             try:
    #                 recd_products.append(msg)
    #                 curr_query_info['product_name'] = msg.get('product_name')
    #                 print(f"[PROCESSING] Added product price change. Total items in queue: {len(recd_products)}")
    #                 print(chalk.yellow(recd_products))
    #             except Exception as e:
    #                 print(chalk.red(f"CALLBACK - CASE - PRODUCT PRICE CHANGE ERROR: {e}"))

    #         elif msg.get('type') == 'PROCESSING_SOLD_ITEMS_COMPLETE':
    #             try:
    #                 print(chalk.green(f"[PROCESSING] SOLD ITEMS"))
    #                 sold_items_dict = msg.get('sold_items_dict')
    #                 if sold_items_dict:
    #                     if not curr_query_info["sold_report_subdir"]:
    #                         curr_query_info["sold_report_subdir"] = make_sold_report_subdir(
    #                             curr_query_info['sold_report_root_dir'],
    #                             curr_query_info['brand'],
    #                             curr_query_info['category'],
    #                             curr_query_info['query_hash']
    #                         )
    #                     sold_items_file_path = os.path.join(
    #                         curr_query_info["sold_report_subdir"],
    #                         f"SOLD_{curr_query_info['source']}_{curr_query_info['brand']}_{curr_query_info['date']}_{curr_query_info['query_hash']}.csv"
    #                     )
    #                     with open(sold_items_file_path, 'w', newline='', encoding='utf-8') as file:
    #                         writer = csv.DictWriter(file, fieldnames=[
    #                             'product_id', 'product_name', 'curr_price', 'curr_scrape_date',
    #                             'prev_price', 'prev_scrape_date', 'sold_date', 'sold', 'url', 'source'
    #                         ])
    #                         writer.writeheader()
    #                         for product_id, product_data in sold_items_dict.items():
    #                             writer.writerow({'product_id': product_id, **product_data})
    #                     print(chalk.green(f"[SUCCESS] Sold items written to {sold_items_file_path}"))
    #                 else:
    #                     print(chalk.magenta(f"[INFO] No sold items for query {curr_query_info['query_hash']}"))

    #                 process_status['PROCESSING_SOLD_ITEMS_COMPLETE'] = True

    #             except Exception as e:
    #                 print(chalk.red(f"CALLBACK - CASE - PROCESS SOLD ITEMS ERROR: {e}"))

    #         elif msg.get('type') == 'PROCESSING_SCRAPED_FILE_COMPLETE':
    #             try:
    #                 print(chalk.green(f"[PROCESSING] SCRAPED FILE COMPLETE - GENERATING REPORTS"))
    #                 if recd_products and curr_query_info["price_report_subdir"]:
    #                     calc_percentage_diff_driver(
    #                         curr_query_info["price_report_subdir"], recd_products, curr_query_info['source_file']
    #                     )
    #                     print(chalk.green(f"[SUCCESS] Report generated and stored in {curr_query_info['price_report_subdir']}"))
    #                 else:
    #                     print(chalk.red("[INFO] No products received; added to no_change_sources list"))
    #                     no_change_sources.append(
    #                         f"{curr_query_info['brand']}_{curr_query_info['category']}_{curr_query_info['product_name']}"
    #                     )

    #                 process_status['PROCESSING_SCRAPED_FILE_COMPLETE'] = True
    #                 COMPARE_publish_to_queue({'type': 'PRICE_WORKER_COMPLETE', 'query_hash': curr_query_info['query_hash']})

    #             except Exception as e:
    #                 print(chalk.red(f"CALLBACK - CASE - PROCESS SCRAPED FILE ERROR: {e}"))

    #         elif msg.get('type') == 'PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY':
    #             try:
    #                 print(chalk.blue(f"[INFO] Preparing to send email report..."))
    #                 email_sent = send_email_with_report(
    #                     'balmanzar883@gmail.com',
    #                     curr_query_info['price_report_subdir'],
    #                     curr_query_info['sold_report_subdir'],
    #                     f"{curr_query_info['brand']}_{curr_query_info['category']}",
    #                     no_change_sources
    #                 )

    #                 if not email_sent:
    #                     exit_with_error("[ERROR] Email failed to send")
                    
    #                 print(chalk.green("[SUCCESS] Email sent successfully"))
    #                 PROCESS_publish_to_queue({
    #                     'type': "EMAIL_SENT",
    #                     'query_hash': curr_query_info['query_hash']
    #                 })
    #             except Exception as e:
    #                 exit_with_error(f"Error during email sending: {e}")
    #             finally:
    #                 no_change_sources.clear()
    #                 reset_query_info()
    #                 reset_process_status()

    #     except Exception as e:
    #         exit_with_error(f"[ERROR] Message processing failed: {e}")
    #     finally:
    #         ch.basic_ack(delivery_tag=method.delivery_tag)

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            print(f"\n{chalk.green('[NEW MESSAGE]')} Received: {msg}")

            current_query_hash = None

            if msg.get('type') == "NEW_QUERY":
                try:
                    source_file = msg['source_file']
                    current_query_hash = extract_query_hash(source_file)
                    
                    # Check if this is truly a new query
                    if current_query_hash != curr_query_info.get('query_hash'):
                        print(chalk.yellow("[INFO] New query detected - resetting state"))
                        reset_query_info()
                        reset_process_status()
                        
                        # Parse filename to populate curr_query_info for the new query
                        source, date, brand, category, query_hash = parse_file_name(source_file)
                        curr_query_info.update({
                            'source': source,
                            'date': date,
                            'category': category,
                            'brand': brand,
                            'query_hash': query_hash,
                            "price_report_root_dir": make_price_report_root_dir(),
                            "sold_report_root_dir": make_sold_report_root_dir(),
                            "source_file": source_file
                        })
                        curr_query_info['price_report_subdir'] = make_price_report_subdir(
                            curr_query_info['price_report_root_dir'], brand, category, query_hash
                        )
                        curr_query_info['sold_report_subdir'] = make_sold_report_subdir(
                            curr_query_info['sold_report_root_dir'], brand, category, query_hash
                        )
                    else:
                        print(chalk.yellow(f"[INFO] Continuing existing query {current_query_hash} - maintaining state"))
                        print(chalk.yellow(f"[INFO] Current received products count: {len(recd_products)}"))

                    # Always update process status
                    process_status['NEW_QUERY_MSG'] = True
                    print(chalk.blue(f"[INFO] Process info updated for file \n ------------- \n"))

                except Exception as e:
                    print(chalk.red(f"CALLBACK - CASE - NEW QUERY ERROR: {e}"))

            elif msg.get('type') == 'PRODUCT_PRICE_CHANGE':
                try:
                    recd_products.append(msg)
                    curr_query_info['product_name'] = msg.get('product_name')
                    print(f"[PROCESSING] Added product price change. Total items in queue: {len(recd_products)}")
                    print(chalk.yellow(f"Current products: {recd_products}"))
                except Exception as e:
                    print(chalk.red(f"CALLBACK - CASE - PRODUCT PRICE CHANGE ERROR: {e}"))

            elif msg.get('type') == 'PROCESSING_SCRAPED_FILE_COMPLETE':
                try:
                    print(chalk.green(f"[PROCESSING] SCRAPED FILE COMPLETE - GENERATING REPORTS"))
                    print(chalk.green(f"[INFO] Currently have {len(recd_products)} products"))
                    
                    if recd_products and curr_query_info["price_report_subdir"]:
                        calc_percentage_diff_driver(
                            curr_query_info["price_report_subdir"],
                            recd_products,
                            curr_query_info['source_file']
                        )
                        print(chalk.green(f"[SUCCESS] Report generated and stored in {curr_query_info['price_report_subdir']}"))
                    else:
                        print(chalk.yellow("[INFO] No price changes detected for this file"))
                        if curr_query_info.get('brand') and curr_query_info.get('category') and curr_query_info.get('product_name'):
                            no_change_sources.append(
                                f"{curr_query_info['brand']}_{curr_query_info['category']}_{curr_query_info['product_name']}"
                            )

                    # Update process status and notify compare queue
                    process_status['PROCESSING_SCRAPED_FILE_COMPLETE'] = True
                    COMPARE_publish_to_queue({
                        'type': 'PRICE_WORKER_COMPLETE',
                        'query_hash': curr_query_info['query_hash'],
                        'products_processed': len(recd_products)
                    })

                except Exception as e:
                    print(chalk.red(f"CALLBACK - CASE - PROCESS SCRAPED FILE ERROR: {e}"))

            elif msg.get('type') == 'PROCESSING_SOLD_ITEMS_COMPLETE':
                try:
                    print(chalk.green(f"[PROCESSING] SOLD ITEMS"))
                    sold_items_dict = msg.get('sold_items_dict')
                    
                    if sold_items_dict:
                        if not curr_query_info["sold_report_subdir"]:
                            curr_query_info["sold_report_subdir"] = make_sold_report_subdir(
                                curr_query_info['sold_report_root_dir'],
                                curr_query_info['brand'],
                                curr_query_info['category'],
                                curr_query_info['query_hash']
                            )
                        
                        sold_items_file_path = os.path.join(
                            curr_query_info["sold_report_subdir"],
                            f"SOLD_{curr_query_info['source']}_{curr_query_info['brand']}_{curr_query_info['date']}_{curr_query_info['query_hash']}.csv"
                        )
                        
                        with open(sold_items_file_path, 'w', newline='', encoding='utf-8') as file:
                            writer = csv.DictWriter(file, fieldnames=[
                                'product_id', 'product_name', 'curr_price', 'curr_scrape_date',
                                'prev_price', 'prev_scrape_date', 'sold_date', 'sold', 'url', 'source'
                            ])
                            writer.writeheader()
                            for product_id, product_data in sold_items_dict.items():
                                writer.writerow({'product_id': product_id, **product_data})
                        print(chalk.green(f"[SUCCESS] Sold items written to {sold_items_file_path}"))
                    else:
                        print(chalk.magenta(f"[INFO] No sold items for query {curr_query_info['query_hash']}"))

                    # Always set the flag to True, whether we had sold items or not
                    process_status['PROCESSING_SOLD_ITEMS_COMPLETE'] = True
                    print(chalk.green(f"[INFO] Updated process status: {process_status}"))

                except Exception as e:
                    print(chalk.red(f"CALLBACK - CASE - PROCESS SOLD ITEMS ERROR: {e}"))


                try:
                    print(chalk.green(f"[PROCESSING] SOLD ITEMS"))
                    sold_items_dict = msg.get('sold_items_dict')
                    
                    if sold_items_dict:
                        if not curr_query_info["sold_report_subdir"]:
                            curr_query_info["sold_report_subdir"] = make_sold_report_subdir(
                                curr_query_info['sold_report_root_dir'],
                                curr_query_info['brand'],
                                curr_query_info['category'],
                                curr_query_info['query_hash']
                            )
                        
                        sold_items_file_path = os.path.join(
                            curr_query_info["sold_report_subdir"],
                            f"SOLD_{curr_query_info['source']}_{curr_query_info['brand']}_{curr_query_info['date']}_{curr_query_info['query_hash']}.csv"
                        )
                        
                        with open(sold_items_file_path, 'w', newline='', encoding='utf-8') as file:
                            writer = csv.DictWriter(file, fieldnames=[
                                'product_id', 'product_name', 'curr_price', 'curr_scrape_date',
                                'prev_price', 'prev_scrape_date', 'sold_date', 'sold', 'url', 'source'
                            ])
                            writer.writeheader()
                            for product_id, product_data in sold_items_dict.items():
                                writer.writerow({'product_id': product_id, **product_data})
                        print(chalk.green(f"[SUCCESS] Sold items written to {sold_items_file_path}"))
                    else:
                        print(chalk.magenta(f"[INFO] No sold items for query {curr_query_info['query_hash']}"))

                    process_status['PROCESSING_SOLD_ITEMS_COMPLETE'] = True

                except Exception as e:
                    print(chalk.red(f"CALLBACK - CASE - PROCESS SOLD ITEMS ERROR: {e}"))

            
            elif msg.get('type') == 'PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY':
                if processes_up_to_end_signal():
                    try:
                        print(chalk.blue(f"[INFO] All processes complete, preparing email report..."))
                        print(chalk.blue(f"[INFO] Total products processed: {len(recd_products)}"))
                        
                        email_sent = send_email_with_report(
                            'balmanzar883@gmail.com',
                            curr_query_info['price_report_subdir'],
                            curr_query_info['sold_report_subdir'],
                            f"{curr_query_info['brand']}_{curr_query_info['category']}",
                            no_change_sources
                        )

                        if not email_sent:
                            exit_with_error("[ERROR] Email failed to send")
                            
                        print(chalk.green("[SUCCESS] Email sent successfully"))
                        PROCESS_publish_to_queue({
                            'type': "EMAIL_SENT",
                            'query_hash': curr_query_info['query_hash']
                        })
                    except Exception as e:
                        exit_with_error(f"Error during email sending: {e}")
                    finally:
                        no_change_sources.clear()
                        reset_query_info()
                        reset_process_status()
                else:
                    print(chalk.yellow("[WARNING] Received end signal but not all processes complete"))
                    print(chalk.yellow(f"[INFO] Current process status: {process_status}"))
                    # Store that we received the end signal
                    process_status['PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY'] = True


        except Exception as e:
            exit_with_error(f"[ERROR] Message processing failed: {e}")
        finally:
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
        channel.queue_declare(queue='price_change_queue', durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='price_change_queue', on_message_callback=callback)
        
        print(chalk.green("Clearing queue"))
        channel.queue_purge(queue='price_change_queue')
        print(chalk.blue('[SYSTEM START] Waiting for messages. To exit press CTRL+C'))
        channel.start_consuming()
    except Exception as e:
        exit_with_error(f"[ERROR] RabbitMQ setup or message consumption failed: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()