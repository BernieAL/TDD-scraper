import pytest
from moto import mock_dynamodb
import boto3
from backend.aws.db.dynamodb_config import dynamodb
from backend.aws.db.table_schemas import TABLE_SCHEMAS
import os

@pytest.fixture
def mock_env():
    """Setup test environment variables"""
    os.environ['AWS_REGION'] = 'us-east-1'
    os.environ['IS_LOCAL'] = 'true'
    yield
    del os.environ['IS_LOCAL']

@pytest.fixture
def setup_tables():
    """Create test tables"""
    with mock_dynamodb():
        dynamodb.create_tables()
        yield dynamodb

class TestDynamoDBOperations:
    """Test DynamoDB CRUD operations"""



    """
    
    to test putting an item
    we would need to eval what is output of when we successfully put an item
    and if it fails

    
    
    
    """

    @pytest.mark.asyncio
    async def test_put_and_get_item(self, mock_env, setup_tables):
        """Test putting and getting an item"""
        # Arrange
        test_item = {
            'PK': 'PRODUCT#123',
            'SK': 'SOURCE#italist',
            'product_name': 'Test Product',
            'current_price': 199.99
        }

        # Act
        await dynamodb.put_item('products', test_item)
        result = await dynamodb.get_item('products', {
            'PK': 'PRODUCT#123',
            'SK': 'SOURCE#italist'
        })

        # Assert
        assert result is not None
        assert result['product_name'] == 'Test Product'
        assert result['current_price'] == 199.99

    @pytest.mark.asyncio
    async def test_update_item(self, mock_env, setup_tables):
        """Test updating an item"""
        # Arrange
        test_item = {
            'PK': 'PRODUCT#123',
            'SK': 'SOURCE#italist',
            'product_name': 'Test Product',
            'current_price': 199.99
        }
        await dynamodb.put_item('products', test_item)

        # Act
        update_expr = {
            'expression': 'SET current_price = :price',
            'values': {':price': 149.99}
        }
        await dynamodb.update_item('products', 
            {'PK': 'PRODUCT#123', 'SK': 'SOURCE#italist'},
            update_expr
        )

        # Assert
        updated_item = await dynamodb.get_item('products', {
            'PK': 'PRODUCT#123',
            'SK': 'SOURCE#italist'
        })
        assert updated_item['current_price'] == 149.99

    @pytest.mark.asyncio
    async def test_query_items(self, mock_env, setup_tables):
        """Test querying items"""
        # Arrange
        items = [
            {
                'PK': 'PRODUCT#123',
                'SK': 'SOURCE#italist',
                'product_name': 'Product 1',
                'current_price': 199.99
            },
            {
                'PK': 'PRODUCT#123',
                'SK': 'SOURCE#farfetch',
                'product_name': 'Product 1',
                'current_price': 189.99
            }
        ]
        for item in items:
            await dynamodb.put_item('products', item)

        # Act
        result = await dynamodb.query(
            'products',
            'PK = :pk AND begins_with(SK, :sk)',
            {':pk': 'PRODUCT#123', ':sk': 'SOURCE#'}
        )

        # Assert
        assert 'Items' in result
        assert len(result['Items']) == 2
        assert any(item['current_price'] == 199.99 for item in result['Items'])
        assert any(item['current_price'] == 189.99 for item in result['Items'])

    @pytest.mark.asyncio
    async def test_delete_item(self, mock_env, setup_tables):
        """Test deleting an item"""
        # Arrange
        test_item = {
            'PK': 'PRODUCT#123',
            'SK': 'SOURCE#italist',
            'product_name': 'Test Product',
            'current_price': 199.99
        }
        await dynamodb.put_item('products', test_item)

        # Act
        await dynamodb.delete_item('products', {
            'PK': 'PRODUCT#123',
            'SK': 'SOURCE#italist'
        })

        # Assert
        result = await dynamodb.get_item('products', {
            'PK': 'PRODUCT#123',
            'SK': 'SOURCE#italist'
        })
        assert result is None 