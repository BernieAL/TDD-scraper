import boto3
import logging
from datetime import datetime

class StatusTracker:
    """Tracks scraper status in DynamoDB."""

    def __init__(self, table_name: str = 'scraper-status'):
        """
        Initialize the status tracker.
        
        Args:
            table_name (str): Name of the DynamoDB table to use
        """
        self.dynamodb = boto3.client('dynamodb', endpoint_url='http://localhost:4566')
        self.table_name = table_name
        self.logger = logging.getLogger(__name__)

    def log_status(self, source: str, query_hash: str, status: str, error_message: str = None):
        """
        Log the status of a scraping operation.
        
        Args:
            source (str): Source of the scrape
            query_hash (str): Hash of the query parameters
            status (str): Status of the operation (SUCCESS/FAILED)
            error_message (str, optional): Error message if status is FAILED
        """
        try:
            item = {
                'source': {'S': source},
                'query_hash': {'S': query_hash},
                'status': {'S': status},
                'timestamp': {'S': datetime.now().isoformat()},
            }
            
            if error_message:
                item['error_message'] = {'S': error_message}
                
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item=item
            )
        except Exception as e:
            self.logger.error(f"Failed to log status: {str(e)}")

    def get_status(self, source: str, query_hash: str):
        """
        Get the status of a scraping operation.
        
        Args:
            source (str): Source of the scrape
            query_hash (str): Hash of the query parameters
            
        Returns:
            dict: Status information
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.table_name,
                Key={
                    'source': {'S': source},
                    'query_hash': {'S': query_hash}
                }
            )
            return response.get('Item')
        except Exception as e:
            self.logger.error(f"Failed to get status: {str(e)}")
            return None 