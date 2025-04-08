"""
Test configuration and fixtures for the application.
"""

import logging
import pytest
import os
import sys
import boto3
from moto import mock_s3, mock_dynamodb, mock_sns, mock_lambda

# Add the backend directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_config import setup_logging

# Set up logging for tests
setup_logging(
    log_level=logging.DEBUG,
    log_file="logs/test.log",
    log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

@pytest.fixture(autouse=True)
def setup_aws_credentials():
    """Automatically set up AWS credentials for all tests."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def mock_aws_services():
    """Mock AWS services for testing."""
    with mock_s3(), mock_dynamodb(), mock_sns(), mock_lambda():
        yield {
            's3': boto3.client('s3'),
            'dynamodb': boto3.resource('dynamodb'),
            'sns': boto3.client('sns'),
            'lambda': boto3.client('lambda')
        }

@pytest.fixture
def mock_s3_bucket(mock_aws_services):
    """Create a mock S3 bucket for testing."""
    s3 = mock_aws_services['s3']
    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)
    return bucket_name

@pytest.fixture
def mock_dynamodb_table(mock_aws_services):
    """Create a mock DynamoDB table for testing."""
    dynamodb = mock_aws_services['dynamodb']
    table = dynamodb.create_table(
        TableName='test-table',
        KeySchema=[
            {'AttributeName': 'query_hash', 'KeyType': 'HASH'},
            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'query_hash', 'AttributeType': 'S'},
            {'AttributeName': 'timestamp', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    return table

@pytest.fixture
def mock_sns_topic(mock_aws_services):
    """Create a mock SNS topic for testing."""
    sns = mock_aws_services['sns']
    topic = sns.create_topic(Name='test-topic')
    return topic['TopicArn']

@pytest.fixture
def sample_scraped_data():
    """Sample scraped data for testing."""
    return {
        "query_hash": "test_hash",
        "timestamp": "2024-01-01T00:00:00",
        "items": [
            {
                "url": "https://example.com/test1",
                "title": "Test Item 1",
                "price": 100.00
            },
            {
                "url": "https://example.com/test2",
                "title": "Test Item 2",
                "price": 200.00
            }
        ]
    }

@pytest.fixture
def sample_historical_data():
    """Sample historical data for testing."""
    return {
        "query_hash": "test_hash",
        "timestamp": "2023-12-31T00:00:00",
        "items": [
            {
                "url": "https://example.com/test1",
                "title": "Test Item 1",
                "price": 90.00
            },
            {
                "url": "https://example.com/test2",
                "title": "Test Item 2",
                "price": 190.00
            }
        ]
    }

@pytest.fixture
def sample_analysis_result():
    """Sample analysis result for testing."""
    return {
        "query_hash": "test_hash",
        "timestamp": "2024-01-01T00:00:00",
        "changes": [
            {
                "url": "https://example.com/test1",
                "title": "Test Item 1",
                "old_price": 90.00,
                "new_price": 100.00,
                "price_change": 10.00,
                "percentage_change": 11.11
            },
            {
                "url": "https://example.com/test2",
                "title": "Test Item 2",
                "old_price": 190.00,
                "new_price": 200.00,
                "price_change": 10.00,
                "percentage_change": 5.26
            }
        ],
        "metadata": {
            "total_items": 2,
            "items_with_changes": 2,
            "average_price_change": 10.00,
            "average_percentage_change": 8.19
        }
    } 