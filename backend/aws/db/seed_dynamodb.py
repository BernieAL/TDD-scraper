import boto3
import json
import csv
from datetime import datetime
from backend.aws.db.dynamodb_config import dynamodb
from simple_chalk import chalk

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
                    
    except Exception as e:
        print(chalk.red(f"Error seeding database: {e}"))
        raise

if __name__ == "__main__":
    # Create tables first
    dynamodb.create_tables()
    
    # Seed data
    import asyncio
    asyncio.run(seed_from_s3()) 