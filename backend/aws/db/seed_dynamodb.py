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

async def db_insert_to_products_table (row):
    await dynamodb.put_item('products_table',item)

async def db_insert_to_price_history_table(row):
    
    """
    Going on the assumption that each product has a group_id
    which is master id for that product.

    Ex. if product is LV NeverFull, then the master ID 
        for this product would be LV-B-NV-421k02
        LV for louis vuition
        B for bag
        NV for neverfull
        421k02 - randomnumber 

    Then the each listing itself would have its own indiv product id

        should product_listing_id be specific to source where they are listed?
        ITAL-19234-12123 etc?


    """
    
    print(row)
    # #recieves csv row
    # #get master_sku off row
    # master_sku = row['master_sku']

    # #build new obj to insert
    # #key is master_sku
    # entry = {
    #     'PK': f'{master_sku}',
    #     'price': float(row['price']),
    #     'source': row['source'],
    #     'scrape_date': datetime.now().isoformat()
    # }
    
    # print(entry)
    # # await dynamodb.put_item('price_history',entry)




def validate_row(row: dict) -> bool:
    """
    Validate CSV row data before insertion
    
    Required fields:
    - product_name: string
    - price: numeric
    - source: string
    - url: valid URL
    - master_sku: string (if we're using this)
    """
    try:
        # Check required fields exist
        required_fields = ['product_name', 'price', 'source', 'url']
        if not all(field in row for field in required_fields):
            print(chalk.yellow(f"Missing required fields in row: {row}"))
            return False
            
        # Validate price is numeric
        try:
            price = float(row['price'])
            if price <= 0:
                print(chalk.yellow(f"Invalid price: {price}"))
                return False
        except ValueError:
            print(chalk.yellow(f"Price not numeric: {row['price']}"))
            return False
            
        # Validate source is known/valid
        valid_sources = ['italist', 'farfetch', 'mytheresa']  # Add your sources
        if row['source'].lower() not in valid_sources:
            print(chalk.yellow(f"Unknown source: {row['source']}"))
            return False
            
        # Basic URL validation
        if not row['url'].startswith(('http://', 'https://')):
            print(chalk.yellow(f"Invalid URL: {row['url']}"))
            return False
            
        return True
        
    except Exception as e:
        print(chalk.red(f"Validation error: {e}"))
        return False

async def insert_raw_scraped_data_runner():
    """Reads CSV files and inserts valid data"""
    raw_data_path = seed_data_path / 'scrape_data' / 'raw'
    csv_files = list(raw_data_path.rglob('*.csv'))
    
    for file in csv_files:
        try:
            with file.open('r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if validate_row(row):
                        await db_insert_to_price_history_table(row)
                    else:
                        print(chalk.yellow(f"Skipping invalid row in {file.name}"))
                   
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

    import asyncio
    asyncio.run(insert_raw_scraped_data_runner())


    # # Create tables first
    # dynamodb.create_tables()
    
    # # Seed data
    # import asyncio
    # asyncio.run(seed_from_s3()) 