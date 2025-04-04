"""
Script to seed DynamoDB with initial product data from CSV files.
Reads raw scrape data and inserts into DynamoDB tables.
"""

import os
import sys
import csv 
from datetime import datetime
from pathlib import Path
from decimal import Decimal
from typing import Dict, Any
from boto3.dynamodb.types import TypeSerializer

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Import after adding to path
from backend.aws.db.dynamodb_config import DynamoDBClient
from backend.aws.db.table_schemas import PRODUCTS_TABLE, PRICE_HISTORY_TABLE
from simple_chalk import chalk

dynamodb = DynamoDBClient()
serializer = TypeSerializer()

def serialize_item(item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Convert a Python dictionary to DynamoDB format."""
    return {k: serializer.serialize(v) for k, v in item.items()}

def store_price_history(product_id: str, source: str, price: Decimal, scrape_date: str) -> None:
    """Store price history for a product."""
    try:
        price_history_item = {
            'PK': f'PROD#{product_id}',
            'SK': f'PRICE#{scrape_date}#{source}',
            'price': price,
            'source': source,
            'scrape_date': scrape_date
        }
        dynamodb.put_item(PRICE_HISTORY_TABLE['name'], serialize_item(price_history_item))
        print(f"Stored price history for product {product_id}")
    except Exception as e:
        print(f"Error storing price history for product {product_id}: {str(e)}")
        raise

def process_csv_file(file_path: str) -> None:
    """Process a single CSV file and store data in DynamoDB."""
    print(f"\nProcessing: {file_path}")
    try:
        # Extract scrape date from filename
        filename = os.path.basename(file_path)
        scrape_date = filename.split('_')[3]  # Format: RAW_ITALIST_PRADA_2024-24-11_BAGS_4fb6a214.csv
        print(f"Processing file with scrape date: {scrape_date}")

        with open(file_path, 'r', encoding='utf-8') as csvfile:
            # Skip the first 3 lines (scrape date, category, and separator)
            for _ in range(3):
                next(csvfile)
            
            # Create CSV reader
            reader = csv.reader(csvfile)
            next(reader)  # Skip separator line
            
            for row in reader:
                if not row:  # Skip empty rows
                    continue
                    
                product_id = row[0]  # Already in format "id-variant"
                
                # Create product item
                product_item = {
                    'PK': f'PROD#{product_id}',
                    'SK': f'SOURCE#{row[5]}',  # Source as sort key
                    'brand': row[1],
                    'product_name': row[2],
                    'current_price': Decimal(str(float(row[3]))),
                    'listing_url': row[4],
                    'source': row[5],
                    'last_updated': datetime.now().isoformat()
                }
                
                try:
                    # Store product data
                    dynamodb.put_item(PRODUCTS_TABLE['name'], serialize_item(product_item))
                    print(f"Stored product {product_id}")
                    
                    # Store price history
                    store_price_history(
                        product_id,
                        row[5],  # source
                        Decimal(str(float(row[3]))),  # price
                        scrape_date
                    )
                except Exception as e:
                    print(f"Error storing items for product {product_id}: {str(e)}")
                    raise
                
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        raise

def process_raw_directory(directory_path: str) -> None:
    """Process all CSV files in the raw scrape output directory."""
    print(f"Scanning directory: {directory_path}")
    try:
        # Walk through the directory
        for root, dirs, files in os.walk(directory_path):
            print(f"Found directory: {root}")
            print(f"Files in directory: {files}")
            
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    process_csv_file(file_path)
                    
    except Exception as e:
        print(f"Error processing directory {directory_path}: {str(e)}")
        raise

def main():
    """Main function to seed DynamoDB with data from CSV files."""
    try:
        # Get the project root directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        print(f"Current directory: {current_dir}")
        print(f"Project root: {project_root}")
        
        # Construct path to raw scrape output directory
        raw_dir_path = os.path.join(
            project_root,
            'backend',
            'OLD_local',
            'src',
            'scrape_file_output',
            'raw'
        )
        
        print(f"Starting to process files in: {raw_dir_path}")
        process_raw_directory(raw_dir_path)
        
    except Exception as e:
        print(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main() 