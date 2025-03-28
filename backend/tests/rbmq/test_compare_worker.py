import pytest
from moto import mock_dynamodb
from backend.workers.compare_worker import CompareWorker
from datetime import datetime

@pytest.fixture
def dynamodb_tables():
    """Create test tables"""
    with mock_dynamodb():
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Create products table
        dynamodb.create_table(
            TableName='products-table',
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
        
        # Create price history table
        dynamodb.create_table(
            TableName='price-history-table',
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
        
        yield dynamodb

@pytest.mark.asyncio
async def test_update_product_price(dynamodb_tables):
    worker = CompareWorker()
    
    # Test updating a product price
    product_id = "test123"
    source = "ITALIST"
    new_price = 199.99
    old_price = 149.99
    
    await worker.update_product_price(product_id, source, new_price, old_price)
    
    # Verify product update
    response = worker.products_table.get_item(
        Key={
            'PK': f'PROD#{product_id}',
            'SK': f'META#{source}'
        }
    )
    
    assert response['Item']['current_price'] == new_price
    assert response['Item']['previous_price'] == old_price 