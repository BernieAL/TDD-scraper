import sys, csv, json, os
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree
import boto3
from io import StringIO
import asyncio
from pathlib import Path

# Initialize paths
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from analysis.percent_change_analysis import calc_percentage_diff_driver
from email_sender import send_email_with_report
from aws.sns_config.sns_client import sns
from aws.sns_config.sns_topics import SNS_TOPICS, TOPIC_ARNS
from aws.db.dynamodb_config import dynamodb
from aws.db.table_schemas import TABLE_SCHEMAS

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

def parse_file_name(file_key):
    """Updated to handle S3 key paths"""
    try:
        # Get just the filename from the full S3 key
        filename = file_key.split('/')[-1]
        tokens = filename.split('_')
        source, brand, date, category, query_hash = tokens[1], tokens[2], tokens[3], tokens[4], tokens[-1].split('.')[0]
        print(chalk.blue(f"[INFO] Parsed filename tokens: {tokens}"))
        return source, date, brand, category, query_hash
    except IndexError as e:
        print(chalk.red(f"Filename parsing error: {e}"))
        raise

def extract_query_hash(file_key):
    """Extract query hash from S3 key"""
    try:
        filename = file_key.split('/')[-1]
        return filename.split('_')[-1].split('.')[0]
    except Exception as e:
        print(chalk.red(f"Error extracting query hash: {e}"))
        return None

async def store_price_history(product_data):
    """Store price history in DynamoDB"""
    try:
        # Create price history entry
        price_history_item = {
            'PK': f"PROD#{product_data['product_id']}",
            'SK': f"PRICE#{datetime.now().isoformat()}",
            'master_sku': product_data.get('master_sku', ''),
            'high_price': float(product_data.get('current_price', 0)),
            'low_price': float(product_data.get('current_price', 0)),
            'source': product_data.get('source', ''),
            'scrape_date': datetime.now().isoformat()
        }
        
        await dynamodb.put_item('price-history-table', price_history_item)
        print(chalk.green(f"[SUCCESS] Stored price history for product: {product_data['product_id']}"))
        
    except Exception as e:
        print(chalk.red(f"Error storing price history: {e}"))
        raise

async def handle_price_change(message):
    """Handle price change message from SNS"""
    try:
        recd_products.append(message)
        print(f"[PROCESSING] Added product price change. Total items: {len(recd_products)}")

        # Store price history in DynamoDB
        await store_price_history(message)

        # Write price changes to S3
        if recd_products:
            s3 = boto3.client('s3')
            filename = f"PRICE_CHANGES_{curr_query_info['source']}_{curr_query_info['brand']}_{curr_query_info['date']}_{curr_query_info['query_hash']}.json"
            try:
                s3.put_object(
                    Bucket=os.environ['S3_BUCKET'],
                    Key=f"{curr_query_info['paths']['reports_path']}/prices/{filename}",
                    Body=json.dumps(recd_products),
                    ContentType='application/json'
                )
            except Exception as e:
                print(chalk.red(f"Error uploading to S3: {e}"))
                raise

    except Exception as e:
        print(chalk.red(f"Error processing price change: {e}"))
        raise

async def handle_scraped_file_complete(message):
    """Handle scraped file complete message from SNS"""
    try:
        if message.get('scrape_file_empty') or message.get('filter_file_empty'):
            print(chalk.yellow("[INFO] Scraped file was empty"))
            empty_scrape_files.append(message.get('source'))
        else:
            print(chalk.green(f"[PROCESSING] SCRAPED FILE COMPLETE - GENERATING REPORTS"))
            
            if recd_products:
                # Use S3 paths for report generation
                s3 = boto3.client('s3')
                
                # Generate report data
                report_data = calc_percentage_diff_driver(
                    recd_products,
                    curr_query_info['source_file'],
                    curr_query_info['category']
                )
                
                # Save report directly to S3
                filename = f"PRICE_ANALYSIS_{curr_query_info['source']}_{curr_query_info['brand']}_{curr_query_info['date']}_{curr_query_info['query_hash']}.json"
                try:
                    s3.put_object(
                        Bucket=os.environ['S3_BUCKET'],
                        Key=f"{curr_query_info['paths']['analysis_path']}/{filename}",
                        Body=json.dumps(report_data),
                        ContentType='application/json'
                    )
                except Exception as e:
                    print(chalk.red(f"Error uploading to S3: {e}"))
                    raise
                print(chalk.green(f"[SUCCESS] Report generated at {curr_query_info['paths']['analysis_path']}/{filename}"))
            else:
                print(chalk.yellow("[INFO] No price changes detected"))

        process_status['PROCESSING_SCRAPED_FILE_COMPLETE'] = True
        
        # Publish completion message to SNS
        await sns.publish_message(
            TOPIC_ARNS['analysis_complete'],
            {
                'type': 'PRICE_WORKER_COMPLETE',
                'query_hash': curr_query_info['query_hash'],
                'products_processed': len(recd_products)
            }
        )

    except Exception as e:
        print(chalk.red(f"Error processing scraped file complete: {e}"))
        raise

async def handle_sold_items_complete(message):
    """Handle sold items complete message from SNS"""
    try:
        print(chalk.green(f"[PROCESSING] SOLD ITEMS"))
        sold_items_dict = message.get('sold_items_dict')
        
        if sold_items_dict:
            # Create CSV in memory
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=[
                'product_id', 'product_name', 'curr_price', 'curr_scrape_date',
                'prev_price', 'prev_scrape_date', 'sold_date', 'sold', 'url', 'source'
            ])
            writer.writeheader()
            for product_id, product_data in sold_items_dict.items():
                writer.writerow({'product_id': product_id, **product_data})

            # Upload to S3
            s3 = boto3.client('s3')
            filename = f"SOLD_ITEMS_{curr_query_info['source']}_{curr_query_info['brand']}_{curr_query_info['date']}_{curr_query_info['query_hash']}.csv"
            try:
                s3.put_object(
                    Bucket=os.environ['S3_BUCKET'],
                    Key=f"{curr_query_info['paths']['reports_path']}/sold/{filename}",
                    Body=output.getvalue(),
                    ContentType='text/csv'
                )
            except Exception as e:
                print(chalk.red(f"Error uploading to S3: {e}"))
                raise

        process_status['PROCESSING_SOLD_ITEMS_COMPLETE'] = True

    except Exception as e:
        print(chalk.red(f"Error processing sold items complete: {e}"))
        raise

async def handle_new_query(message):
    """Handle new query message from SNS"""
    try:
        source_file = message['source_file']
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
                'product_name': message.get('spec_item'),
                'paths': {
                    'raw_path': os.environ['RAW_PATH'],
                    'analysis_path': os.environ['ANALYSIS_PATH'],
                    'reports_path': os.environ['REPORTS_PATH'],
                    'price_reports_dir': f"{os.environ['REPORTS_PATH']}/prices",
                    'sold_reports_dir': f"{os.environ['REPORTS_PATH']}/sold"
                }
            })
            print(chalk.blue(f"[INFO] Process info updated for file"))
        else:
            print(chalk.yellow(f"[INFO] Continuing existing query {current_query_hash} - maintaining state"))
            print(chalk.yellow(f"[INFO] Current received products count: {len(recd_products)}"))

        process_status['NEW_QUERY_MSG'] = True

    except Exception as e:
        print(chalk.red(f"Error processing new query: {e}"))
        raise

async def handle_sns_message(message):
    """Handle incoming SNS message"""
    try:
        msg_type = message.get('type')
        
        if msg_type == 'NEW_QUERY':
            await handle_new_query(message)
        elif msg_type == 'PRODUCT_PRICE_CHANGE':
            await handle_price_change(message)
        elif msg_type == 'PROCESSING_SCRAPED_FILE_COMPLETE':
            await handle_scraped_file_complete(message)
        elif msg_type == 'PROCESSING_SOLD_ITEMS_COMPLETE':
            await handle_sold_items_complete(message)
        else:
            print(chalk.yellow(f"[INFO] Unknown message type: {msg_type}"))
            
        # Check if we should send email
        if processes_up_to_end_signal():
            # Send email request through SNS
            await sns.publish_message(
                TOPIC_ARNS['analysis_complete'],
                {
                    'type': 'SEND_EMAIL',
                    'query_hash': curr_query_info['query_hash'],
                    'query': curr_query_info['product_name'],
                    'email': curr_query_info.get('email'),
                    'price_reports_path': curr_query_info['paths'].get('reports_path', 'reports'),
                    'sold_reports_path': curr_query_info['paths'].get('sold_path', 'sold'),
                    'no_price_change_sources': empty_scrape_files
                }
            )
            
            # Reset state for next query
            reset_query_info()
            reset_process_status()
            
    except Exception as e:
        print(chalk.red(f"Error handling SNS message: {e}"))
        raise

async def main():
    """Main function to handle SNS messages"""
    try:
        # Subscribe to SNS topics
        for topic_name, topic in SNS_TOPICS.items():
            try:
                # Create topic if it doesn't exist
                topic_arn = await sns.create_topic(topic.name)
                TOPIC_ARNS[topic_name] = topic_arn
                print(chalk.green(f"[SUCCESS] Created/Found topic: {topic.name}"))
            except Exception as e:
                print(chalk.red(f"Error creating topic {topic.name}: {e}"))
                raise

        # Start processing messages
        while True:
            try:
                # Get messages from SNS
                response = await sns.client.receive_message(
                    QueueUrl=os.environ['SNS_QUEUE_URL']
                )
                
                for message in response.get('Messages', []):
                    try:
                        # Parse message body
                        body = json.loads(message['Body'])
                        await handle_sns_message(body)
                    except Exception as e:
                        print(chalk.red(f"Error processing message: {e}"))
                        continue
                
                # Wait before next poll
                await asyncio.sleep(1)
                
            except Exception as e:
                print(chalk.red(f"Error in message processing loop: {e}"))
                await asyncio.sleep(5)  # Wait longer on error

    except Exception as e:
        print(chalk.red(f"Fatal error in main: {e}"))
        raise

if __name__ == "__main__":
    asyncio.run(main())