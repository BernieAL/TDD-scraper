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
    "source": None,
    "source_file": None,
    "paths": {}  # All shared paths will be stored here
}

process_status = {
    'NEW_QUERY_MSG': False,
    'PROCESSING_SOLD_ITEMS_COMPLETE': False,
    'PROCESSING_SCRAPED_FILE_COMPLETE': False,
}

recd_products = []  # Global list to store products for each query
empty_scrape_files = []  # to track sources with no results at all

def reset_query_info():
    """Reset all query info including paths"""
    curr_query_info.update({
        "brand": None,
        "category": None,
        "query_hash": None,
        "product_name": None,
        "date": None,
        "source": None,
        "source_file": None,
        "paths": {}
    })
    global recd_products, empty_scrape_files
    recd_products.clear()
    empty_scrape_files.clear()
    print(chalk.blue("[INFO] Reset query info and cleared product lists"))

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
        print(chalk.yellow("[INFO] Waiting for processes to complete. Current status:"))
        for k, v in current_status.items():
            print(chalk.yellow(f"  - {k}: {v}"))
    
    return is_ready

def parse_file_name(file):
    try:
        tokens = file.split('/')[-1].split('_')
        source, brand, date, category, query_hash = tokens[1], tokens[2], tokens[3], tokens[4], tokens[-1].split('.')[0]
        print(chalk.blue(f"[INFO] Parsed filename tokens: {tokens}"))
        return source, date, brand, category, query_hash
    except IndexError as e:
        print(chalk.red(f"Filename parsing error: {e}"))
        raise

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

    def callback(ch, method, properties, body):
        try:
            msg = json.loads(body)
            print(f"\n{chalk.green('[NEW MESSAGE]')} Received: {msg}")

            if msg.get('type') == "NEW_QUERY":
                try:
                    source_file = msg['source_file']
                    current_query_hash = extract_query_hash(source_file)
                    
                    if current_query_hash != curr_query_info.get('query_hash'):
                        print(chalk.yellow("[INFO] New query detected - resetting state"))
                        reset_query_info()
                        reset_process_status()
                        
                        source, date, brand, category, query_hash = parse_file_name(source_file)
                        curr_query_info.update({
                            'source': source,
                            'date': date,
                            'category': category,
                            'brand': brand,
                            'query_hash': query_hash,
                            'source_file': source_file,
                            'paths': msg.get('paths', {})  # Store shared paths from message
                        })
                        print(chalk.blue(f"[INFO] Process info updated for file"))
                    else:
                        print(chalk.yellow(f"[INFO] Continuing existing query {current_query_hash} - maintaining state"))
                        print(chalk.yellow(f"[INFO] Current received products count: {len(recd_products)}"))

                    process_status['NEW_QUERY_MSG'] = True

                except Exception as e:
                    print(chalk.red(f"Error processing NEW_QUERY message: {e}"))
                    raise

            elif msg.get('type') == 'PRODUCT_PRICE_CHANGE':
                try:
                    recd_products.append(msg)
                    curr_query_info['product_name'] = msg.get('product_name')
                    print(f"[PROCESSING] Added product price change. Total items: {len(recd_products)}")
                except Exception as e:
                    print(chalk.red(f"Error processing PRODUCT_PRICE_CHANGE: {e}"))
                    raise

            elif msg.get('type') == 'PROCESSING_SCRAPED_FILE_COMPLETE':
                try:
                    if msg.get('scrape_file_empty'):
                        print(chalk.yellow("[INFO] Scraped file was empty"))
                        empty_scrape_files.append(msg.get('source'))
                    else:
                        print(chalk.green(f"[PROCESSING] SCRAPED FILE COMPLETE - GENERATING REPORTS"))
                        curr_query_info['product_name'] = msg.get('product_name')
                        
                        if recd_products:
                            price_report_dir = curr_query_info['paths']['price_reports_dir']
                            calc_percentage_diff_driver(
                                price_report_dir,
                                recd_products,
                                curr_query_info['source_file']
                            )
                            print(chalk.green(f"[SUCCESS] Report generated in {price_report_dir}"))
                        else:
                            print(chalk.yellow("[INFO] No price changes detected"))
                            no_change_sources.append(curr_query_info['source'])

                    process_status['PROCESSING_SCRAPED_FILE_COMPLETE'] = True
                    COMPARE_publish_to_queue({
                        'type': 'PRICE_WORKER_COMPLETE',
                        'query_hash': curr_query_info['query_hash'],
                        'products_processed': len(recd_products)
                    })

                except Exception as e:
                    print(chalk.red(f"Error processing SCRAPED_FILE_COMPLETE: {e}"))
                    raise

            elif msg.get('type') == 'PROCESSING_SOLD_ITEMS_COMPLETE':
                try:
                    print(chalk.green(f"[PROCESSING] SOLD ITEMS"))
                    sold_items_dict = msg.get('sold_items_dict')
                    
                    if sold_items_dict:
                        sold_items_file_path = os.path.join(
                            curr_query_info['paths']['sold_reports_dir'],
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
                    print(chalk.red(f"Error processing SOLD_ITEMS_COMPLETE: {e}"))
                    raise

            elif msg.get('type') == 'PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY':
                if processes_up_to_end_signal():
                    try:
                        print(chalk.blue(f"[INFO] Preparing email report. Products processed: {len(recd_products)}"))
                        
                        email_sent = send_email_with_report(
                            {'email': msg.get('email')},
                            curr_query_info['paths']['price_reports_dir'],
                            curr_query_info['paths']['sold_reports_dir'],
                            f"{curr_query_info['brand']}_{curr_query_info['category']}_{curr_query_info['product_name']}",
                            no_change_sources,
                            empty_scrape_files
                        )
                        
                        PROCESS_publish_to_queue({
                            'type': 'EMAIL',
                            'status': 'PASS' if email_sent else 'FAIL',
                            'query_hash': curr_query_info['query_hash'],
                            'paths': curr_query_info['paths']
                        })

                        if not email_sent:
                            print(chalk.red("[ERROR] Email failed to send"))
                            
                    except Exception as e:
                        print(chalk.red(f"Error sending email: {e}"))
                        PROCESS_publish_to_queue({
                            'type': 'EMAIL',
                            'status': 'FAIL',
                            'query_hash': curr_query_info['query_hash'],
                            'paths': curr_query_info['paths']
                        })
                        raise
                    finally:
                        no_change_sources.clear()
                        reset_query_info()
                        reset_process_status()
                else:
                    print(chalk.yellow("[WARNING] Not all processes complete for end signal"))
                    process_status['PROCESSED_ALL_SCRAPED_FILES_FOR_QUERY'] = True

        except Exception as e:
            print(chalk.red(f"[ERROR] Message processing failed: {e}"))
            raise
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # RabbitMQ setup
    connection = None
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
        print(chalk.red(f"[ERROR] RabbitMQ setup failed: {e}"))
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()