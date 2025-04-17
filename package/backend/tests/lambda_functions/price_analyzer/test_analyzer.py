"""
Tests for the price analyzer Lambda function.
Tests:
1. Event handling and validation
2. Data loading and processing
3. Price analysis
4. Database operations
5. Error handling
"""

import pytest
import json
import boto3
from datetime import datetime
from unittest.mock import patch, MagicMock
from moto import mock_s3, mock_dynamodb

@pytest.fixture
def sample_event():
    """Sample event that the analyzer would receive"""
    return {
        "query_hash": "test123",
        "paths": {
            "raw": "raw",
            "filtered": "filtered"
        }
    }

@pytest.fixture
def mock_s3_bucket():
    """Create and return a mock S3 bucket"""
    with mock_s3():
        s3 = boto3.client('s3')
        bucket_name = 'scraper-data-bucket'
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name

@pytest.fixture
def mock_dynamodb_table():
    """Create and return a mock DynamoDB table"""
    with mock_dynamodb():
        dynamodb = boto3.resource('dynamodb')
        table_name = 'products-table'
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'product_id', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'product_id', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        yield table_name

@pytest.fixture
def sample_price_data():
    """Sample price data for testing"""
    return [
        {
            "product_id": "1",
            "brand": "PRADA",
            "name": "Test Bag",
            "price": 1000.0,
            "currency": "USD",
            "url": "http://example.com/1",
            "source": "italist"
        },
        {
            "product_id": "2",
            "brand": "PRADA",
            "name": "Test Bag",
            "price": 1100.0,
            "currency": "USD",
            "url": "http://example.com/2",
            "source": "farfetch"
        }
    ]

def test_event_validation(sample_event):
    """Test event validation"""
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    # Test valid event
    response = analyze_prices(sample_event, None)
    assert response['statusCode'] == 200
    
    # Test missing required fields
    invalid_event = sample_event.copy()
    del invalid_event['query_hash']
    response = analyze_prices(invalid_event, None)
    assert response['statusCode'] == 400

def test_data_loading(sample_event, mock_s3_bucket, sample_price_data):
    """Test data loading from S3"""
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(sample_price_data).encode('utf-8')
            )
        }
        mock_boto3.return_value = mock_s3_client
        
        # Run analyzer
        response = analyze_prices(sample_event, None)
        assert response['statusCode'] == 200
        
        # Verify S3 data was loaded
        mock_s3_client.get_object.assert_called_once()
        call_args = mock_s3_client.get_object.call_args[1]
        assert call_args['Bucket'] == mock_s3_bucket
        assert 'filtered' in call_args['Key']

def test_price_analysis(sample_event, mock_s3_bucket, sample_price_data):
    """Test price analysis logic"""
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(sample_price_data).encode('utf-8')
            )
        }
        mock_boto3.return_value = mock_s3_client
        
        # Run analyzer
        response = analyze_prices(sample_event, None)
        assert response['statusCode'] == 200
        
        # Verify price analysis results
        body = json.loads(response['body'])
        assert 'analysis' in body
        analysis = body['analysis']
        assert 'min_price' in analysis
        assert 'max_price' in analysis
        assert 'avg_price' in analysis
        assert analysis['min_price'] == 1000.0
        assert analysis['max_price'] == 1100.0
        assert analysis['avg_price'] == 1050.0

def test_database_operations(sample_event, mock_s3_bucket, mock_dynamodb_table, sample_price_data):
    """Test database operations"""
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    with patch('boto3.client') as mock_boto3, \
         patch('boto3.resource') as mock_resource:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(sample_price_data).encode('utf-8')
            )
        }
        mock_boto3.return_value = mock_s3_client
        
        # Mock DynamoDB table
        mock_table = MagicMock()
        mock_resource.return_value.Table.return_value = mock_table
        
        # Run analyzer
        response = analyze_prices(sample_event, None)
        assert response['statusCode'] == 200
        
        # Verify database operations
        assert mock_table.put_item.call_count == len(sample_price_data)

def test_error_handling(sample_event):
    """Test error handling"""
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client to raise an error
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.side_effect = Exception("S3 Error")
        mock_boto3.return_value = mock_s3_client
        
        # Run analyzer
        response = analyze_prices(sample_event, None)
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body 