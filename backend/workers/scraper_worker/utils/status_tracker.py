import time
import logging
from typing import Optional
import boto3

class StatusTracker:
    """Handles tracking and updating scraper status in DynamoDB."""

    def __init__(self, table_name: str = 'scraper_status'):
        """
        Initialize the status tracker.
        
        Args:
            table_name (str): Name of the DynamoDB table to use
        """
        self.table_name = table_name
        self.dynamodb = boto3.client('dynamodb')
        self.logger = logging.getLogger(__name__)

    def log_status(self, source: str, query_hash: str, status: str, error: Optional[str] = None) -> None:
        """
        Log scraper status to DynamoDB.
        
        Args:
            source (str): Source of the status update (e.g., scraper name)
            query_hash (str): Query hash for tracking
            status (str): Status of the operation (SUCCESS/FAILED)
            error (Optional[str]): Error message if any
        """
        item = {
            'query_hash': {'S': query_hash},
            'source': {'S': source},
            'status': {'S': status},
            'timestamp': {'N': str(int(time.time()))}
        }
        
        if error:
            item['error'] = {'S': error}
            
        try:
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item=item
            )
            self.logger.info(f"Logged {status} status for {source} to DynamoDB")
        except Exception as e:
            self.logger.error(f"Failed to log status to DynamoDB: {e}")

    def get_status(self, query_hash: str, source: str) -> list:
        """
        Get status history for a specific query and source.
        
        Args:
            query_hash (str): Query hash to look up
            source (str): Source to filter by
            
        Returns:
            list: List of status entries
        """
        try:
            response = self.dynamodb.query(
                TableName=self.table_name,
                KeyConditionExpression='query_hash = :hash',
                FilterExpression='source = :src',
                ExpressionAttributeValues={
                    ':hash': {'S': query_hash},
                    ':src': {'S': source}
                }
            )
            return response.get('Items', [])
        except Exception as e:
            self.logger.error(f"Failed to get status from DynamoDB: {e}")
            return [] 