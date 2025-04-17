"""
Global test configuration and fixtures.
"""

import pytest
import os
import boto3
from moto import mock_ecs, mock_s3, mock_lambda, mock_dynamodb, mock_sns, mock_iam
from unittest.mock import Mock

@pytest.fixture(autouse=True)
def setup_aws_credentials():
    """Set up AWS credentials for all tests"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(autouse=True)
def mock_aws_services():
    """Mock all AWS services"""
    with mock_ecs(), mock_s3(), mock_lambda(), mock_dynamodb(), mock_sns(), mock_iam():
        yield

@pytest.fixture
def mock_s3_bucket():
    """Create mock S3 bucket"""
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='scraper-data-bucket')
    return 'scraper-data-bucket'

@pytest.fixture
def mock_dynamodb_table():
    """Create mock DynamoDB table"""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.create_table(
        TableName='scraper-results',
        KeySchema=[
            {'AttributeName': 'query_hash', 'KeyType': 'HASH'},
            {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'query_hash', 'AttributeType': 'S'},
            {'AttributeName': 'timestamp', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.wait_until_exists()
    return table

@pytest.fixture
def mock_sns_topic():
    """Create mock SNS topic"""
    sns = boto3.client('sns')
    topic = sns.create_topic(Name='scraper-notifications')
    return topic['TopicArn']

@pytest.fixture
def mock_lambda_function():
    """Create mock Lambda function"""
    lambda_client = boto3.client('lambda')
    lambda_client.create_function(
        FunctionName='scraper-worker',
        Runtime='python3.10',
        Handler='index.handler',
        Role='arn:aws:iam::123456789012:role/lambda-role',
        Code={'ZipFile': b''}
    )
    return 'scraper-worker'

@pytest.fixture
def sample_form_data():
    """Sample form submission data"""
    return {
        'brand': 'PRADA',
        'category': 'BAGS',
        'scraper_name': 'ITALIST',
        'query_hash': 'test123',
        'user_tier': 'free'
    }

@pytest.fixture
def sample_scrape_results():
    """Sample scraping results"""
    return [
        {
            'brand': 'PRADA',
            'product': 'Galleria Bag',
            'price': 2500,
            'url': 'http://test.com/product1',
            'date': '2024-01-15'
        }
    ] 