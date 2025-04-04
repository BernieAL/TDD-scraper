from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from simple_chalk import chalk

@dataclass
class TableSchema:
    """Define table structure"""
    name: str
    partition_key: str
    sort_key: str
    attributes: Dict[str, str]  # name: type
    indexes: List[Dict] = None  # GSIs/LSIs

# Define table schemas
TABLE_SCHEMAS = {
    'users': TableSchema(
        name='users-table',
        partition_key='PK',      # USER#{user_id}
        sort_key='SK',          # META
        attributes={
            'PK': 'S',
            'SK': 'S',
            'email': 'S',
            'created_at': 'S',
            'last_login': 'S',
            'member_tier': 'N'  #0=free, 1 = lvl 1 paid, 2 = lvl 2 paid
        }
    ),
    'user_searches': TableSchema(
        name='user-searches-table',
        partition_key='PK',      # USER#{user_id}
        sort_key='SK',          # SEARCH#{timestamp}
        attributes={
            'PK': 'S',
            'SK': 'S',
            'query_hash': 'S',
            'brand': 'S',
            'category': 'S',
            'specific_item': 'S',
            'search_date': 'S', #date search was performed
            'timestamp': 'S',
            'status': 'S'  # Track search status
        }
    ),
    'products': TableSchema(
        name='products-table',
        partition_key='PK',      # PROD#{product_id}
        sort_key='SK',          # SOURCE#{source}
        attributes={
            'PK': 'S',
            'SK': 'S',
            'master_sku': 'S',
            'product_name': 'S',
            'current_price': 'N',
            'previous_price': 'N',
            'last_scrape_date': 'S',
            'url': 'S',
            'source': 'S'
        }
    ),
    'price_history': TableSchema(
        name='price-history-table',
        partition_key='PK',      # PROD#{product_id}
        sort_key='SK',          # PRICE#{timestamp}
        attributes={
            'PK': 'S',
            'SK': 'S',
            'master_sku': 'S',
            'high_price': 'N',
            'low_low': 'N',
            'source': 'S',
            'scrape_date': 'S'
        }
    )
}

# DynamoDB client configuration
dynamodb = boto3.client('dynamodb', endpoint_url='http://localhost:4567')

# Table schemas
PRODUCTS_TABLE = {
    'name': 'products-table',
    'schema': {
        'TableName': 'products-table',
        'KeySchema': [
            {'AttributeName': 'PK', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'SK', 'KeyType': 'RANGE'}   # Sort key
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'PK', 'AttributeType': 'S'},
            {'AttributeName': 'SK', 'AttributeType': 'S'}
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    }
}

PRICE_HISTORY_TABLE = {
    'name': 'price-history-table',
    'schema': {
        'TableName': 'price-history-table',
        'KeySchema': [
            {'AttributeName': 'PK', 'KeyType': 'HASH'},  # Partition key (Product ID)
            {'AttributeName': 'SK', 'KeyType': 'RANGE'}   # Sort key (Timestamp)
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'PK', 'AttributeType': 'S'},
            {'AttributeName': 'SK', 'AttributeType': 'S'}
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    }
}

def create_table(table_config):
    """
    Create a DynamoDB table if it doesn't exist.
    
    Args:
        table_config: Dictionary containing table name and schema
    """
    try:
        # Check if table exists
        dynamodb.describe_table(TableName=table_config['name'])
        print(chalk.green(f"Table {table_config['name']} already exists"))
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Table doesn't exist, create it
            try:
                dynamodb.create_table(**table_config['schema'])
                print(chalk.green(f"Created table {table_config['name']}"))
            except ClientError as e:
                print(chalk.red(f"Error creating table {table_config['name']}: {str(e)}"))
        else:
            print(chalk.red(f"Error checking table {table_config['name']}: {str(e)}"))

def create_all_tables():
    """Create all DynamoDB tables defined in the schemas"""
    tables = [PRODUCTS_TABLE, PRICE_HISTORY_TABLE]
    for table in tables:
        create_table(table)

if __name__ == "__main__":
    create_all_tables()
