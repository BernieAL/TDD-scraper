import os
import json
import logging
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class ScraperUtils:
    """Utility class for scraper operations."""

    def __init__(self, s3_client, dynamodb_client, s3_bucket: str, dynamodb_table: str):
        """Initialize the utility class with AWS clients and configuration."""
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        self.s3_bucket = s3_bucket
        self.dynamodb_table = dynamodb_table
        self.logger = logging.getLogger(__name__)

    def save_to_file(self, data: List[Dict], brand: str, query: str, source: str, output_dir: str, query_hash: str, retry_count: int = 0) -> Tuple[str, str]:
        """Save scraped data to a temporary file."""
        try:
            # Create output directory if it doesn't exist
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Generate filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{source.lower()}_{brand}_{query}_{timestamp}_{retry_count}.json"
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.tmp')
            
            # Write data to file
            with open(temp_file.name, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.info(f"Saved {len(data)} items to {temp_file.name}")
            return temp_file.name, filename

        except Exception as e:
            self.logger.error(f"Error saving data to file: {str(e)}")
            raise

    def upload_to_s3(self, local_file: str, s3_key: str) -> bool:
        """Upload a file to S3."""
        try:
            self.logger.info(f"Uploading {local_file} to s3://{self.s3_bucket}/{s3_key}")
            with open(local_file, 'rb') as f:
                self.s3_client.upload_fileobj(f, self.s3_bucket, s3_key)
            return True
        except Exception as e:
            self.logger.error(f"Error uploading to S3: {str(e)}")
            raise
        finally:
            try:
                os.unlink(local_file)
                self.logger.info(f"Cleaned up temporary file {local_file}")
            except Exception as e:
                self.logger.warning(f"Error cleaning up temporary file: {str(e)}")

    def get_driver_utils(self):
        """Get driver utilities for web scraping."""
        from .driver_utils import get_driver
        return get_driver 