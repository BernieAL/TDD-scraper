import pytest
import boto3
from moto import mock_dynamodb
from backend.config.dynamodb import get_dynamodb, PRODUCTS_TABLE
import os

@mock_dynamodb
def test_get_dynamodb_local():
    # Test local development connection
    os.environ['IS_LOCAL'] = 'true'
    db = get_dynamodb()
    assert db is not None
    assert isinstance(db, boto3.resources.base.ServiceResource)

@mock_dynamodb
def test_get_dynamodb_prod():
    # Test production connection
    os.environ.pop('IS_LOCAL', None)
    db = get_dynamodb()
    assert db is not None
    assert isinstance(db, boto3.resources.base.ServiceResource)

@mock_dynamodb
def test_table_creation():
    dynamodb = get_dynamodb()
    
    # Create test table
    table = dynamodb.create_table(
        TableName=PRODUCTS_TABLE,
        KeySchema=[
            {'AttributeName': 'PK', 'KeyType': 'HASH'},
            {'AttributeName': 'SK', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PK', 'AttributeType': 'S'},
            {'AttributeName': 'SK', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    assert table.table_status == 'ACTIVE' 