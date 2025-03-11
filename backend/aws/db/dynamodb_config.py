"""
DynamoDB Configuration and Operations Manager

This module provides a centralized interface for DynamoDB interactions in the price comparison application.
It handles client configuration, table management, and database operations.

The DynamoDB class serves as a repository pattern implementation, encapsulating all DynamoDB-related
operations and configurations in one place.

Typical usage:
    db = DynamoDB()
    product = await db.get_item('products', {'PK': 'PROD#123', 'SK': 'SOURCE#amazon'})

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
from table_schemas import TABLE_SCHEMAS




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
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(chalk.red(f"DynamoDB error in {func.__name__}: {error_code}"))
            raise
        except Exception as e:
            print(chalk.red(f"Unexpected error in {func.__name__}: {str(e)}"))
            raise
    return wrapper

class DynamoDB:
    """
    Manages DynamoDB configuration and operations.
    
    This class handles:
    1. Client setup for both local and production environments
    2. Table reference management using predefined schemas
    3. Basic CRUD operations for DynamoDB interactions
    
    Attributes:
        client: Boto3 DynamoDB resource
        region (str): AWS region for DynamoDB
        tables (Dict): References to all DynamoDB tables
    """
    
    def __init__(self):
        """Initialize DynamoDB client and table references"""
        self.setup_dynamodb_client()

    def setup_dynamodb_client(self):
        """Initialize DynamoDB connection and table references"""
        try:
            self.region = get_env_var('AWS_REGION', 'us-east-1')
            self.is_local = get_env_var('AWS_SAM_LOCAL') or get_env_var('IS_LOCAL')
            
            # Configure client for local or production
            if self.is_local:
                self.client = boto3.resource(
                    'dynamodb',
                    endpoint_url='http://localhost:8000',
                    region_name=self.region,
                    aws_access_key_id='dummy',
                    aws_secret_access_key='dummy'
                )
            else:
                self.client = boto3.resource('dynamodb', region_name=self.region)
            
            # Initialize table references
            self._initialize_tables()
            
        except Exception as e:
            print(chalk.red(f"Error initializing DynamoDB: {e}"))
            raise

    
   
    #TABLE MGMT METHODS
    def _initialize_tables(self):
        """Initialize references to all tables, for each schema in TABLE_SCHEMAS list"""
        self._tables = {
            name: self.client.Table(schema.name)
            for name, schema in TABLE_SCHEMAS.items()
        }

    @handle_dynamo_error
    def create_tables(self):
        """
        Creates DynamoDB tables if they don't exist
        
        Examples:
            # Create all tables defined in TABLE_SCHEMAS
            dynamodb.create_tables()
            
            # Tables are created with schema from TABLE_SCHEMAS:
            # - products_table
            # - price_history_table
            # - users_table
            # - user_searches_table
        """
        for name, schema in TABLE_SCHEMAS.items():
            try:
                self.client.create_table(
                    TableName=schema.name,
                    KeySchema=[
                        {'AttributeName': schema.partition_key, 'KeyType': 'HASH'},
                        {'AttributeName': schema.sort_key, 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': name, 'AttributeType': type_}
                        for name, type_ in schema.attributes.items()
                        if name in [schema.partition_key, schema.sort_key]
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                print(chalk.green(f"Created table: {schema.name}"))
                
                # Wait for table to be created
                waiter = self.client.get_waiter('table_exists')
                waiter.wait(TableName=schema.name)
                
            except self.client.exceptions.ResourceInUseException:
                print(chalk.blue(f"Table already exists: {schema.name}"))
            except Exception as e:
                print(chalk.red(f"Error creating table {schema.name}: {e}"))
                raise

    @handle_dynamo_error
    def delete_tables(self):
        """
        Deletes all DynamoDB tables (use with caution!)
        Typically used in local development to reset the database
        """
        if not self.is_local:
            raise Exception("Cannot delete tables in production environment!")
            
        for name, schema in TABLE_SCHEMAS.items():
            try:
                table = self.client.Table(schema.name)
                table.delete()
                print(chalk.yellow(f"Deleted table: {schema.name}"))
                
                # Wait for table to be deleted
                waiter = self.client.get_waiter('table_not_exists')
                waiter.wait(TableName=schema.name)
                
            except Exception as e:
                print(chalk.red(f"Error deleting table {schema.name}: {e}"))
                raise

    #PROPERTIES
    @property
    def tables(self) -> Dict:
        """Get references to all tables"""
        return self._tables

    #CRUD OPS
    @handle_dynamo_error
    async def get_item(self, table_name: str, key: Dict) -> Optional[Dict]:
        """
        Get item from specified table
        
        Args:
            table_name: Name of the table
            key: Primary key of the item to get
            
        Returns:
            Dict containing the item if found, None otherwise
        """
        response = await self.tables[table_name].get_item(Key=key)
        return response.get('Item')

    @handle_dynamo_error
    async def put_item(self, table_name: str, item: Dict) -> Dict:
        """
        Put item in specified table
        
        Args:
            table_name: Name of the table
            item: Item to put in the table
            
        Returns:
            Response from DynamoDB
        """
        return await self.tables[table_name].put_item(Item=item)

    @handle_dynamo_error
    async def update_item(self, table_name: str, key: Dict, updates: Dict) -> Dict:
        """
        Update item in specified table
        
        Args:
            table_name: Name of the table
            key: Primary key of item to update
            updates: Dict containing UpdateExpression and ExpressionAttributeValues
            
        Returns:
            Response from DynamoDB
        """
        return await self.tables[table_name].update_item(
            Key=key,
            UpdateExpression=updates['expression'],
            ExpressionAttributeValues=updates['values']
        )

    @handle_dynamo_error
    async def delete_item(self, table_name: str, key: Dict) -> Dict:
        """
        Delete item from specified table
        
        Args:
            table_name: Name of the table
            key: Primary key of item to delete
            
        Returns:
            Response from DynamoDB
        """
        return await self.tables[table_name].delete_item(Key=key)

    @handle_dynamo_error
    async def query(self, table_name: str, key_condition: str, 
                   values: Dict, index_name: Optional[str] = None) -> Dict:
        """
        Query items from specified table
        
        Args:
            table_name: Name of the table
            key_condition: KeyConditionExpression
            values: ExpressionAttributeValues
            index_name: Optional name of index to query
            
        Returns:
            Query results from DynamoDB
            
        Examples:
            # Query products from a specific source
            await dynamodb.query(
                table_name='products',
                key_condition='PK = :pk AND begins_with(SK, :sk)',
                values={
                    ':pk': 'PRODUCT#123',
                    ':sk': 'SOURCE#'
                }
            )
            
            # Query price history with date range
            await dynamodb.query(
                table_name='price_history',
                key_condition='PK = :pk AND SK BETWEEN :start_date AND :end_date',
                values={
                    ':pk': 'PRODUCT#123',
                    ':start_date': 'PRICE#2023-01-01',
                    ':end_date': 'PRICE#2023-12-31'
                }
            )
        """
        params = {
            'KeyConditionExpression': key_condition,
            'ExpressionAttributeValues': values
        }
        if index_name:
            params['IndexName'] = index_name
            
        return await self.tables[table_name].query(**params)

# Singleton instance
dynamodb = DynamoDB()
    
    
    
   
   