# src/utils/scraper_utils.py

import os, csv, sys
import hashlib
from datetime import datetime
import pandas as pd
from simple_chalk import chalk
import shutil
from pathlib import Path
from backend.utils.project_paths import SCRAPER_WORKER_ROOT

class ScraperUtils:
    """
    Utility class for managing scraper file operations and directory structure.

    Directory Structure:
        scraper_worker/
        ├── tmp/                    # Temporary storage for scraped files before S3 upload
        │   ├── italist/           # Italist.com scraped data
        │   ├── farfetch/          # Farfetch.com scraped data
        │   └── mytheresa/         # MyTheresa.com scraped data
        ├── utils/                  # Utility functions and helpers
        └── scrapers/              # Individual scraper implementations

    Attributes:
        scraper_worker_root (Path): Root directory of scraper_worker module
        temp_dir (Path): Temporary directory for staging files before S3 upload
        s3_client: Optional boto3 S3 client for uploads
        dynamodb_client: Optional boto3 DynamoDB client for status tracking
    """

    # Static class variable
    TEMP_DIR = SCRAPER_WORKER_ROOT / 'temp'

    def __init__(self, s3_client=None, dynamodb_client=None):
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        # Create temp directory if it doesn't exist
        self.TEMP_DIR.mkdir(exist_ok=True, parents=True)

    def generate_hash(self, query: str, specific_item: str, date: str) -> str:
        """Generate hash for query identification"""
        combined_str = f"{query}_{specific_item}_{date}"
        return hashlib.sha256(combined_str.encode()).hexdigest()[:8]

    def make_data_source_output_dir(self, source: str) -> Path:
        """Create source-specific directory in temp/"""
        data_src_dir = self.TEMP_DIR / source.lower()
        data_src_dir.mkdir(exist_ok=True)
        return data_src_dir

    def log_scraper_status(self, source, query_hash, status='SUCCESS', error=None):
        """Log scraper execution status to DynamoDB."""
        if not self.dynamodb_client:
            return False
            
        try:
            item = {
                'query_hash': {'S': query_hash},
                'timestamp': {'N': str(int(datetime.now().timestamp()))},
                'source': {'S': source},
                'status': {'S': status}
            }
            
            if error:
                item['error'] = {'S': str(error)}
                
            self.dynamodb_client.put_item(
                TableName=os.environ['DYNAMODB_TABLE'],
                Item=item
            )
            print(f"Logged {status} status for {source} to DynamoDB")
            return True
        except Exception as e:
            print(f"DynamoDB logging failed: {e}")
            return False

    def save_to_file(self, data, brand, category, source, output_dir, query_hash, data_type):
        """Save data to local temp file in source-specific directory"""
        try:
            current_date = datetime.now().strftime('%Y-%d-%m')
            
            prefix = "FILTERED_" if data_type == 1 else "RAW_"
            filename = f"{prefix}{source}_{brand}_{current_date}_{category}_{query_hash}.csv"
            
            source_dir = self.make_data_source_output_dir(source.lower())
            temp_file = source_dir / filename
            
            with open(temp_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([f"Scraped: {current_date}"])
                writer.writerow([f"category: {brand}-{category}"])
                writer.writerow(['product_id','brand','product_name','curr_price','listing_url','source'])
                writer.writerow(['----------------------'])

                for row in data:
                    if any(str(x).strip() for x in row):
                        processed_row = [
                            str(element).upper() if isinstance(element,str) else element
                            for element in row
                        ]
                        writer.writerow(processed_row)
                    
            print(f"Data successfully saved to {temp_file}")
            
            # Log success status
            self.log_scraper_status(source, query_hash, 'SUCCESS')
            
            return temp_file, filename
            
        except Exception as e:
            # Log failure status
            self.log_scraper_status(source, query_hash, 'FAILED', str(e))
            raise

    def upload_to_s3(self, file_path: str, s3_key: str) -> bool:
        """Upload file to S3"""
        if not self.s3_client:
            return False
            
        try:
            with open(file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=os.environ['S3_BUCKET'],
                    Key=s3_key,
                    Body=f
                )
            print(f"Uploaded to S3: {s3_key}")
            return True
        except Exception as e:
            print(f"S3 upload failed: {e}")
            return False

    def cleanup(self):
        """Remove temporary files"""
        if os.path.exists(self.TEMP_DIR):
            shutil.rmtree(self.TEMP_DIR)

    def parse_file_name(self, file: str) -> tuple:
        """Parse standardized filename into components"""
        file_path_tokens = file.split('/')[-1]
        file_name_tokens = file_path_tokens.split('_')
        
        source = file_name_tokens[1]
        brand = file_name_tokens[2]
        date = file_name_tokens[3]
        category = file_name_tokens[4]
        query_hash = file_name_tokens[5].split('.')[0]

        return source, date, brand, category, query_hash

    def filter_by_specific_item(self, scraped_data_file, specific_item, filtered_subdir, query_hash):
        """Filter scraped data by specific item"""
        try:
            df = pd.read_csv(scraped_data_file, skiprows=2)
            df = df.dropna()
            filtered_df = df[df['product_name'] == specific_item]

            source, date, brand, category, _ = self.parse_file_name(scraped_data_file)
            df_list = filtered_df.values.tolist()
            
            return self.save_to_file(df_list, brand, category, source, filtered_subdir, query_hash, 1)

        except Exception as e:
            print(chalk.red(f"Error in filter_by_specific_item: {e}"))
            raise

        
