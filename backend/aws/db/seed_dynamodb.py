import boto3
import json
import csv
from datetime import datetime
from dynamodb_config import dynamodb
from simple_chalk import chalk
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

# Import Config class
from backend.config.config import Config

# Debug paths
print(f"Project root: {project_root}")
print(f"Config BASE_DIR: {Config.BASE_DIR}")
print(f"Config DB_SEED_ROOT: {Config.DB_SEED_ROOT}")

# Use Config (BASE_DIR already includes 'backend')
seed_data_path = Path(Config.BASE_DIR) / Config.DB_SEED_ROOT
print(f"Final path: {seed_data_path.absolute()}")

# Check full seed path
print(f"Full seed path: {seed_data_path}")
print(f"Seed path exists: {seed_data_path.exists()}")


# Check directory structure
if seed_data_path.parent.exists():
    print("\nContents of parent directory:")
    for item in seed_data_path.parent.iterdir():
        print(f"- {item}")

#using pathlib instead of os.path.join
seed_data_path = Path(Config.BASE_DIR) / Config.DB_SEED_ROOT
print(seed_data_path.exists())


#need to read in scraped data into dynamodb from local

async def db_insert_to_products_table (item):
    await dynamodb.put_item('products_table',item)

async def db_insert_to_price_history_table(item):
    

    #get product id off item

    #find all rows with this product_id, get their price
    
    #encapsulate into obj, insert into db

    await dynamodb.put_item('price_history',item)
        # 'PK': f"PROD#{product_id}",
        # 'SK': f"PRICE#{datetime.now().isoformat()}",
        # 'price': float(price),
        # 'source': source,
        # 'scrape_date': datetime.now().isoformat()



def insert_raw_scraped_data():
    
    #path to raw_scraped_data
    raw_data_path = seed_data_path / 'scrape_data' / 'raw'
    print(raw_data_path.exists())


    #get all csv files in raw data dir
    csv_files = list(raw_data_path.rglob('*.csv'))
    print(f"found csv files {csv_files}")
    
    for file in csv_files:
        print(f"processing: {file.name}")

        try:
            #read csv file
            with file.open('r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    print(row)
                    db_insert_to_products_table(item)
                    db_insert_to_price_history_table(item)
                   
                   
        except Exception as e:
            print(chalk.red(f"Error processing {file.name}: {e}"))


    #for each file in csv_files, insert product into dynamodb
    


async def seed_from_s3():
    """Seed DynamoDB tables with data from S3"""
    try:
        s3 = boto3.client('s3')
        bucket = os.environ['S3_BUCKET']
        
        # List objects in raw data path
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix='raw/'  # Adjust path as needed
        )
        
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                print(chalk.blue(f"Processing file: {obj['Key']}"))
                
                # Get file from S3
                file_data = s3.get_object(
                    Bucket=bucket,
                    Key=obj['Key']
                )
                
                # Read CSV data
                csv_data = file_data['Body'].read().decode('utf-8').splitlines()
                reader = csv.DictReader(csv_data)
                
                # Process each row
                for row in reader:
                    product_id = row['product_id']
                    source = row['source']
                    
                    # Create product entry
                    await dynamodb.products_table.put_item(
                        Item={
                            'PK': f"PROD#{product_id}",
                            'SK': f"SOURCE#{source}",
                            'product_name': row['product_name'],
                            'current_price': float(row['price']),
                            'url': row['url'],
                            'source': source,
                            'last_updated': datetime.now().isoformat(),
                            'price_history': [{
                                'price': float(row['price']),
                                'timestamp': datetime.now().isoformat()
                            }]
                        }
                    )
                    print(chalk.green(f"Added product: {product_id} from {source}"))

                    # #add price to price history
                    
                    # await dynamodb.price_history_table.put_item(
                    #     Item={
                    #         'PK': f"PROD#{product_id}",
                    #         'SK': f"SOURCE#{source}",
                    #         'product_name': row['product_name'],
                    #         'current_price': float(row['price']),
                    #         'source': source,
                    #         'last_updated': datetime.now().isoformat(),
                    #         'timestamp': datetime.now().isoformat()
                    #     }
                    # )
                    # print(chalk.green(f"Added product price to price_history: {product_id} from {source}"))

                    

    except Exception as e:
        print(chalk.red(f"Error seeding database: {e}"))
        raise

if __name__ == "__main__":

    insert_raw_scraped_data()


    # # Create tables first
    # dynamodb.create_tables()
    
    # # Seed data
    # import asyncio
    # asyncio.run(seed_from_s3()) 