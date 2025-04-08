import pytest
import json
import os
from unittest.mock import patch, MagicMock
from moto import mock_s3, mock_dynamodb, mock_sns
import boto3
from decimal import Decimal
from workers.lambda_handlers.price_analysis_handler import handler, load_s3_data
from botocore.exceptions import ClientError

# Test constants
TEST_BUCKET = 'test-scraper-bucket'
TEST_TABLE = 'test-price-history'
TEST_SNS_TOPIC = 'arn:aws:sns:us-east-1:123456789012:test-topic'

@pytest.fixture
def mock_aws():
    """Set up mock AWS services."""
    with mock_s3():
        with mock_dynamodb():
            with mock_sns():
                # Create S3 bucket and upload test data
                s3 = boto3.client('s3', region_name='us-east-1')
                s3.create_bucket(Bucket=TEST_BUCKET)
                
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
                    's3': s3,
                    'dynamodb': dynamodb,
                    'sns': sns,
                    'table': table
                }

@pytest.fixture
def sample_scraped_data():
    """Create sample scraped data."""
    return [
        {
            'id': 'test123',
            'brand': 'Test Brand',
            'name': 'Test Product',
            'price': '$100.00',
            'source': 'TEST_SOURCE',
            'url': 'http://test.com/product'
        },
        {
            'id': 'test456',
            'brand': 'Another Brand',
            'name': 'Another Product',
            'price': '$200.00',
            'source': 'TEST_SOURCE',
            'url': 'http://test.com/another-product'
        }
    ]

@pytest.fixture
def s3_event():
    """Create a sample S3 event."""
    return {
        'Records': [
            {
                's3': {
                    'bucket': {
                        'name': TEST_BUCKET
                    },
                    'object': {
                        'key': 'test/scraped_data.json'
                    }
                }
            }
        ]
    }

def test_load_s3_data(mock_aws, sample_scraped_data):
    """Test loading data from S3."""
    # Upload test data to S3
    mock_aws['s3'].put_object(
        Bucket=TEST_BUCKET,
        Key='test/scraped_data.json',
        Body=json.dumps(sample_scraped_data)
    )
    
    # Load the data
    loaded_data = load_s3_data(TEST_BUCKET, 'test/scraped_data.json')
    assert loaded_data == sample_scraped_data

def test_handler_successful_run(mock_aws, sample_scraped_data, s3_event):
    """Test successful execution of the handler."""
    # Set up environment variables
    os.environ['PRICE_HISTORY_TABLE'] = TEST_TABLE
    os.environ['PRICE_ALERT_TOPIC'] = TEST_SNS_TOPIC
    
    # Upload test data to S3
    mock_aws['s3'].put_object(
        Bucket=TEST_BUCKET,
        Key='test/scraped_data.json',
        Body=json.dumps(sample_scraped_data)
    )
    
    # Run handler
    response = handler(s3_event, None)
    
    # Verify response
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['message'] == 'Price analysis completed successfully'
    assert body['products_processed'] == len(sample_scraped_data)

def test_handler_with_price_changes(mock_aws, sample_scraped_data, s3_event):
    """Test handler when price changes are detected."""
    # Set up environment variables
    os.environ['PRICE_HISTORY_TABLE'] = TEST_TABLE
    os.environ['PRICE_ALERT_TOPIC'] = TEST_SNS_TOPIC
    
    # Store initial prices in DynamoDB
    table = mock_aws['dynamodb'].Table(TEST_TABLE)
    for product in sample_scraped_data:
        table.put_item(Item={
            'product_id': product['id'],
            'source': product['source'],
            'price': '$90.00',  # Lower price to trigger change detection
            'brand': product['brand'],
            'name': product['name'],
            'url': product['url']
        })
    
    # Upload test data to S3
    mock_aws['s3'].put_object(
        Bucket=TEST_BUCKET,
        Key='test/scraped_data.json',
        Body=json.dumps(sample_scraped_data)
    )
    
    # Run handler
    response = handler(s3_event, None)
    
    # Verify response
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['price_changes_found'] > 0

def test_handler_no_price_changes(mock_aws, sample_scraped_data, s3_event):
    """Test handler when no price changes are detected."""
    # Set up environment variables
    os.environ['PRICE_HISTORY_TABLE'] = TEST_TABLE
    os.environ['PRICE_ALERT_TOPIC'] = TEST_SNS_TOPIC
    
    # Store same prices in DynamoDB
    table = mock_aws['dynamodb'].Table(TEST_TABLE)
    for product in sample_scraped_data:
        table.put_item(Item={
            'product_id': product['id'],
            'source': product['source'],
            'price': product['price'],  # Same price as scraped data
            'brand': product['brand'],
            'name': product['name'],
            'url': product['url']
        })
    
    # Upload test data to S3
    mock_aws['s3'].put_object(
        Bucket=TEST_BUCKET,
        Key='test/scraped_data.json',
        Body=json.dumps(sample_scraped_data)
    )
    
    # Run handler
    response = handler(s3_event, None)
    
    # Verify response
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['price_changes_found'] == 0

@pytest.mark.parametrize("error_service", ['s3', 'dynamodb', 'sns'])
def test_handler_error_handling(mock_aws, sample_scraped_data, s3_event, error_service):
    """Test handler error handling for different AWS service failures."""
    # Set up environment variables
    os.environ['PRICE_HISTORY_TABLE'] = TEST_TABLE
    os.environ['PRICE_ALERT_TOPIC'] = TEST_SNS_TOPIC

    # Mock service error
    error_msg = f"Simulated {error_service} error"
    
    if error_service == 's3':
        # Mock S3 client specifically
        with patch('boto3.client') as mock_boto3_client:
            mock_s3 = MagicMock()
            mock_s3.get_object.side_effect = ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': error_msg}},
                'GetObject'
            )
            mock_boto3_client.return_value = mock_s3
            with pytest.raises(ClientError) as exc_info:
                handler(s3_event, None)
            assert error_msg in str(exc_info.value)
    
    elif error_service == 'dynamodb':
        # Mock DynamoDB resource and table
        with patch('boto3.resource') as mock_boto3_resource:
            # First mock S3 to return valid data
            mock_s3 = MagicMock()
            mock_s3.get_object.return_value = {
                'Body': MagicMock(
                    read=lambda: json.dumps(sample_scraped_data).encode('utf-8')
                )
            }
            with patch('boto3.client') as mock_boto3_client:
                mock_boto3_client.return_value = mock_s3
                
                # Then mock DynamoDB to raise error
                mock_table = MagicMock()
                mock_table.get_item.side_effect = ClientError(
                    {'Error': {'Code': 'ResourceNotFoundException', 'Message': error_msg}},
                    'GetItem'
                )
                mock_dynamodb = MagicMock()
                mock_dynamodb.Table.return_value = mock_table
                mock_boto3_resource.return_value = mock_dynamodb
                
                with pytest.raises(ClientError) as exc_info:
                    handler(s3_event, None)
                assert error_msg in str(exc_info.value)
    
    else:  # sns
        # Mock SNS client
        with patch('boto3.client') as mock_boto3_client:
            # First mock S3 to return valid data
            mock_s3 = MagicMock()
            mock_s3.get_object.return_value = {
                'Body': MagicMock(
                    read=lambda: json.dumps(sample_scraped_data).encode('utf-8')
                )
            }

            # Then mock DynamoDB to return historical data with different price
            mock_dynamodb = MagicMock()
            mock_table = MagicMock()
            mock_table.get_item.return_value = {
                'Item': {
                    'product_id': 'test123',
                    'source': 'test_source',
                    'brand': 'Test Brand',
                    'name': 'Test Product',
                    'price': '$50.00',  # Different price to trigger change
                    'url': 'http://test.com'
                }
            }
            mock_dynamodb.Table.return_value = mock_table

            # Then mock SNS to raise error
            mock_sns = MagicMock()
            mock_sns.publish.side_effect = ClientError(
                {'Error': {'Code': 'InvalidParameter', 'Message': error_msg}},
                'Publish'
            )

            def get_mock_service(service_name):
                if service_name == 's3':
                    return mock_s3
                elif service_name == 'sns':
                    return mock_sns

            mock_boto3_client.side_effect = get_mock_service

            with patch('boto3.resource') as mock_boto3_resource:
                mock_boto3_resource.return_value = mock_dynamodb

                with pytest.raises(ClientError) as exc_info:
                    handler(s3_event, None)
                assert error_msg in str(exc_info.value)

def test_handler_invalid_event(mock_aws):
    """Test handler with invalid event structure."""
    invalid_event = {'Records': [{'invalid': 'structure'}]}
    
    with pytest.raises(KeyError):
        handler(invalid_event, None)

def test_handler_missing_env_vars(mock_aws, s3_event):
    """Test handler with missing environment variables."""
    # Clear environment variables
    if 'PRICE_HISTORY_TABLE' in os.environ:
        del os.environ['PRICE_HISTORY_TABLE']
    if 'PRICE_ALERT_TOPIC' in os.environ:
        del os.environ['PRICE_ALERT_TOPIC']
    
    with pytest.raises(KeyError):
        handler(s3_event, None) 