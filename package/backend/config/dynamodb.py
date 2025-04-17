"""
DynamoDB configuration and table definitions.
"""
import boto3
from botocore.config import Config

# Table names
PRODUCTS_TABLE = 'products-table'
PRICE_HISTORY_TABLE = 'price-history-table'

def get_dynamodb():
    """Get a DynamoDB resource with retry configuration."""
    config = Config(
        retries = dict(
            max_attempts = 3
        )
    )
    return boto3.resource('dynamodb', config=config) 