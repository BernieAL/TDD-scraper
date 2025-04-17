"""
DynamoDB Configuration and Operations Manager

This module provides a centralized interface for DynamoDB interactions in the price comparison application.
It handles client configuration, table management, and database operations.

The DynamoDBClient class serves as a repository pattern implementation, encapsulating all DynamoDB-related
operations and configurations in one place.

Typical usage:
    db = DynamoDBClient()
    product = db.get_item('products-table', {'product_id': {'S': '123'}})

Environment Variables:
    AWS_REGION (str): AWS region for DynamoDB (default: 'us-east-1')
    AWS_SAM_LOCAL (bool): Flag for local SAM testing
    IS_LOCAL (bool): Flag for local development

Dependencies:
    - boto3: AWS SDK for Python
"""

import boto3
from typing import Dict, Any, Optional, List
from botocore.exceptions import ClientError
from functools import wraps
from simple_chalk import chalk
from backend.config.localstack import get_boto3_client

def handle_dynamo_error(func):
    """Decorator for handling DynamoDB errors"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(chalk.red(f"DynamoDB error in {func.__name__}: {error_code}"))
            raise
        except Exception as e:
            print(chalk.red(f"Unexpected error in {func.__name__}: {str(e)}"))
            raise
    return wrapper

class DynamoDBClient:
    """
    Manages DynamoDB configuration and operations.
    
    This class handles:
    1. Client setup for both local and production environments
    2. Basic CRUD operations for DynamoDB interactions using the low-level client interface
    
    Attributes:
        client: Boto3 DynamoDB client
        region (str): AWS region for DynamoDB
    """
    
    def __init__(self):
        """Initialize DynamoDB client"""
        self.client = get_boto3_client('dynamodb')

    @handle_dynamo_error
    def get_item(self, table_name: str, key: Dict[str, Dict[str, Any]]) -> Optional[Dict]:
        """
        Get item from specified table
        
        Args:
            table_name: Name of the table
            key: Primary key of the item to get in DynamoDB format
            
        Returns:
            Dict containing the item if found, None otherwise
        """
        response = self.client.get_item(TableName=table_name, Key=key)
        return response.get('Item')

    @handle_dynamo_error
    def put_item(self, table_name: str, item: Dict[str, Dict[str, Any]]) -> Dict:
        """
        Put item in specified table
        
        Args:
            table_name: Name of the table
            item: Item to put in the table in DynamoDB format
            
        Returns:
            Response from DynamoDB
        """
        return self.client.put_item(TableName=table_name, Item=item)

    @handle_dynamo_error
    def update_item(self, table_name: str, key: Dict[str, Dict[str, Any]], updates: Dict) -> Dict:
        """
        Update item in specified table
        
        Args:
            table_name: Name of the table
            key: Primary key of item to update in DynamoDB format
            updates: Dict containing UpdateExpression and ExpressionAttributeValues
            
        Returns:
            Response from DynamoDB
        """
        return self.client.update_item(
            TableName=table_name,
            Key=key,
            UpdateExpression=updates['UpdateExpression'],
            ExpressionAttributeValues=updates['ExpressionAttributeValues']
        )

    @handle_dynamo_error
    def delete_item(self, table_name: str, key: Dict[str, Dict[str, Any]]) -> Dict:
        """
        Delete item from specified table
        
        Args:
            table_name: Name of the table
            key: Primary key of item to delete in DynamoDB format
            
        Returns:
            Response from DynamoDB
        """
        return self.client.delete_item(TableName=table_name, Key=key)

    @handle_dynamo_error
    def query(self, table_name: str, key_condition: str, 
             values: Dict[str, Dict[str, Any]], index_name: Optional[str] = None) -> Dict:
        """
        Query items from specified table
        
        Args:
            table_name: Name of the table
            key_condition: KeyConditionExpression
            values: ExpressionAttributeValues in DynamoDB format
            index_name: Optional name of index to query
            
        Returns:
            Query results from DynamoDB
            
        Examples:
            # Query products from a specific source
            await dynamodb.query(
                table_name='products-table',
                key_condition='PK = :pk AND begins_with(SK, :sk)',
                values={
                    ':pk': {'S': 'PROD#123'},
                    ':sk': {'S': 'SOURCE#'}
                }
            )
            
            # Query price history with date range
            await dynamodb.query(
                table_name='price-history-table',
                key_condition='PK = :pk AND SK BETWEEN :start_date AND :end_date',
                values={
                    ':pk': {'S': 'PROD#123'},
                    ':start_date': {'S': 'PRICE#2023-01-01'},
                    ':end_date': {'S': 'PRICE#2023-12-31'}
                }
            )
        """
        params = {
            'TableName': table_name,
            'KeyConditionExpression': key_condition,
            'ExpressionAttributeValues': values
        }
        if index_name:
            params['IndexName'] = index_name
            
        return self.client.query(**params)

    def scan(self, table_name: str) -> List[Dict]:
        """Scan a DynamoDB table."""
        try:
            response = self.client.scan(
                TableName=table_name
            )
            return response.get('Items', [])
        except Exception as e:
            print(f"Error scanning table {table_name}: {e}")
            return []

# Create a singleton instance
dynamodb = DynamoDBClient()
    
    
    
   
   