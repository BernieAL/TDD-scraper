"""
DynamoDB Configuration and Operations Manager

This module provides a centralized interface for DynamoDB interactions in the price comparison application.
It handles client configuration, table management, and database operations.

The DynamoDBClient class serves as a repository pattern implementation, encapsulating all DynamoDB-related
operations and configurations in one place.

Typical usage:
    db = DynamoDBClient()
    product = await db.get_item('products-table', {'PK': {'S': 'PROD#123'}, 'SK': {'S': 'SOURCE#amazon'}})

Environment Variables:
    AWS_REGION (str): AWS region for DynamoDB (default: 'us-east-1')
    AWS_SAM_LOCAL (bool): Flag for local SAM testing
    IS_LOCAL (bool): Flag for local development

Dependencies:
    - boto3: AWS SDK for Python
    - .table_schemas: Local module defining DynamoDB table structures
"""

import boto3
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError
from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from backend.config.config import get_env_var
from simple_chalk import chalk
from functools import wraps
from backend.aws.db.table_schemas import PRODUCTS_TABLE, PRICE_HISTORY_TABLE




"""
without decorator:
    we would manually "wrap" the function to be handled
    by passing it as an arg to handle_dynamo_error 

    Ex. 
    get_item = handle_dynamo_error(get_item)

with decorator
    the wrapping function recieves the function as a param
    and returns a new wrapper function that wraps the original function in try/catch,
    calls the original function with await func(*args,**kwargs), and handles any errors

    @wraps(func) preserves the original functions metadata, so when you call get_item() it actually looks like:
    
     
        async def wrapper(**args, **kwargs):
            try:
                return await get_item(*args, **kwargs) #original function
            except ClientError as e:
                #handle dynamodb errors
            except Exception as e:
                #handle other errors

"""
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
        self.setup_dynamodb_client()

    @classmethod
    def setup_dynamodb_client(cls):
        """Set up DynamoDB client with LocalStack configuration."""
        try:
            cls.client = boto3.client(
                'dynamodb',
                endpoint_url='http://localhost:4567',  # Backend LocalStack endpoint
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1',
                verify=False  # Disable SSL verification for local development
            )
            print("DynamoDB client initialized successfully")
        except Exception as e:
            print(f"Error initializing DynamoDB client: {str(e)}")
            raise

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

# Create a singleton instance
dynamodb = DynamoDBClient()
    
    
    
   
   