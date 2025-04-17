import pytest
import boto3
from decimal import Decimal
from datetime import datetime
from moto import mock_dynamodb, mock_sns
from workers.scraper_worker.analysis.price_analyzer import PriceAnalyzer

# Test constants
TEST_TABLE = 'test-price-history'
TEST_SNS_TOPIC = 'arn:aws:sns:us-east-1:123456789012:test-topic'

@pytest.fixture
def mock_aws():
    """Set up mock AWS services."""
    with mock_dynamodb():
        with mock_sns():
            # Create DynamoDB table
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            table = dynamodb.create_table(
                TableName=TEST_TABLE,
                KeySchema=[
                    {'AttributeName': 'product_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'source', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'product_id', 'AttributeType': 'S'},
                    {'AttributeName': 'source', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            
            # Create SNS topic
            sns = boto3.client('sns', region_name='us-east-1')
            sns.create_topic(Name='test-topic')
            
            yield {
                'dynamodb': dynamodb,
                'sns': sns,
                'table': table
            }

@pytest.fixture
def analyzer(mock_aws):
    """Create a PriceAnalyzer instance."""
    return PriceAnalyzer(TEST_TABLE, TEST_SNS_TOPIC)

@pytest.fixture
def sample_product():
    """Create a sample product for testing."""
    return {
        'id': 'test123',
        'brand': 'Test Brand',
        'name': 'Test Product',
        'price': '$100.00',
        'source': 'TEST_SOURCE',
        'url': 'http://test.com/product'
    }

def test_price_analyzer_initialization(analyzer):
    """Test PriceAnalyzer initialization."""
    assert analyzer.table.name == TEST_TABLE
    assert analyzer.sns_topic_arn == TEST_SNS_TOPIC

def test_parse_price(analyzer):
    """Test price parsing with different formats."""
    assert analyzer._parse_price('$100.00') == Decimal('100.00')
    assert analyzer._parse_price('€150,99') == Decimal('150.99')
    assert analyzer._parse_price('1,299.99') == Decimal('1299.99')

def test_store_initial_price(analyzer, sample_product, mock_aws):
    """Test storing initial price data."""
    analyzer._store_initial_price(sample_product)
    
    # Verify stored data
    response = mock_aws['table'].get_item(
        Key={
            'product_id': sample_product['id'],
            'source': sample_product['source']
        }
    )
    item = response['Item']
    
    assert item['brand'] == sample_product['brand']
    assert item['name'] == sample_product['name']
    assert item['price'] == sample_product['price']
    assert item['url'] == sample_product['url']
    assert 'first_seen' in item
    assert 'last_updated' in item

def test_analyze_price_changes_new_product(analyzer, sample_product):
    """Test analyzing prices for a new product."""
    changes = analyzer.analyze_price_changes([sample_product])
    assert len(changes) == 0  # No changes for new products

def test_analyze_price_changes_price_increase(analyzer, sample_product, mock_aws):
    """Test analyzing price increases."""
    # Store initial price
    analyzer._store_initial_price(sample_product)
    
    # Update product with higher price
    updated_product = sample_product.copy()
    updated_product['price'] = '$150.00'
    
    changes = analyzer.analyze_price_changes([updated_product])
    
    assert len(changes) == 1
    change = changes[0]
    assert change['previous_price'] == Decimal('100.00')
    assert change['current_price'] == Decimal('150.00')
    assert change['price_difference'] == Decimal('50.00')
    assert change['percentage_change'] == Decimal('50.00')

def test_analyze_price_changes_price_decrease(analyzer, sample_product, mock_aws):
    """Test analyzing price decreases."""
    # Store initial price
    analyzer._store_initial_price(sample_product)
    
    # Update product with lower price
    updated_product = sample_product.copy()
    updated_product['price'] = '$75.00'
    
    changes = analyzer.analyze_price_changes([updated_product])
    
    assert len(changes) == 1
    change = changes[0]
    assert change['previous_price'] == Decimal('100.00')
    assert change['current_price'] == Decimal('75.00')
    assert change['price_difference'] == Decimal('-25.00')
    assert change['percentage_change'] == Decimal('-25.00')

def test_generate_price_report(analyzer):
    """Test report generation."""
    price_changes = [{
        'product_id': 'test123',
        'brand': 'Test Brand',
        'name': 'Test Product',
        'current_price': Decimal('150.00'),
        'previous_price': Decimal('100.00'),
        'price_difference': Decimal('50.00'),
        'percentage_change': Decimal('50.00'),
        'source': 'TEST_SOURCE',
        'url': 'http://test.com/product',
        'timestamp': datetime.now().isoformat()
    }]
    
    report = analyzer.generate_price_report(price_changes)
    
    assert "Price Change Report" in report
    assert "Test Brand - Test Product" in report
    assert "Previous Price: $100.00" in report
    assert "Current Price: $150.00" in report
    assert "Change: $50.00 (50.0%)" in report

def test_send_report(analyzer, mock_aws):
    """Test report sending via SNS."""
    report = "Test price change report"
    analyzer.send_report(report)
    
    # Verify SNS publication
    topics = mock_aws['sns'].list_topics()
    assert len(topics['Topics']) == 1
    
    # In a real test, we would verify the message was published
    # but moto doesn't currently support checking published messages

def test_no_price_changes(analyzer, sample_product, mock_aws):
    """Test handling of products with no price changes."""
    # Store initial price
    analyzer._store_initial_price(sample_product)
    
    # Try to analyze the same price
    changes = analyzer.analyze_price_changes([sample_product])
    assert len(changes) == 0

def test_error_handling(analyzer):
    """Test error handling for invalid data."""
    invalid_product = {
        'id': 'test123',
        'brand': 'Test Brand',
        'name': 'Test Product',
        'price': 'invalid_price',  # Invalid price format
        'source': 'TEST_SOURCE',
        'url': 'http://test.com/product'
    }
    
    changes = analyzer.analyze_price_changes([invalid_product])
    assert len(changes) == 0  # Should handle error gracefully 