"""
Tests for the scrape orchestrator Lambda function.
Tests:
1. Event handling and validation
2. S3 path management
3. Task launching
4. Error handling
5. Local vs AWS execution
6. Invalid form data handling
"""

import pytest
import json
import boto3
import os
from botocore.config import Config

# Configure boto3 to use LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# Create boto3 client with LocalStack endpoint
s3_config = Config(
    region_name='us-east-1',
    retries={'max_attempts': 5, 'mode': 'standard'}
)

@pytest.fixture
def s3_client():
    """Create S3 client pointing to LocalStack"""
    return boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        config=s3_config
    )

@pytest.fixture
def sample_event():
    """Sample event that the orchestrator would receive"""
    return {
        "query_hash": "test123",
        "brand": "PRADA",
        "category": "BAGS",
        "local_test": True,
        "bucket_name": "scraper-data-bucket"
    }

@pytest.fixture
def mock_s3_bucket(s3_client):
    """Create and return a mock S3 bucket"""
    bucket_name = 'scraper-data-bucket'
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except Exception as e:
        if not "BucketAlreadyOwnedByYou" in str(e):
            raise
    return bucket_name

def test_event_validation(s3_client, mock_s3_bucket, sample_event):
    """Test event validation"""
    from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler
    
    # Test valid event
    response = lambda_handler(sample_event, None)
    assert response['statusCode'] == 200
    
    # Test missing required fields
    invalid_event = sample_event.copy()
    del invalid_event['query_hash']
    response = lambda_handler(invalid_event, None)
    assert response['statusCode'] == 500

def test_s3_path_management(s3_client, mock_s3_bucket, sample_event):
    """Test S3 path management"""
    from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler
    
    # Run orchestrator
    response = lambda_handler(sample_event, None)
    assert response['statusCode'] == 200
    
    # Verify S3 operations were performed
    objects = s3_client.list_objects_v2(Bucket=mock_s3_bucket)
    assert 'Contents' in objects

def test_error_handling(s3_client, sample_event):
    """Test error handling"""
    from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler
    
    # Run orchestrator with invalid bucket name to trigger error
    invalid_event = sample_event.copy()
    invalid_event['bucket_name'] = 'non-existent-bucket'
    response = lambda_handler(invalid_event, None)
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert 'error' in body

def test_local_execution(s3_client, mock_s3_bucket, sample_event):
    """Test local execution mode"""
    from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler
    
    # Event already has local_test=True
    response = lambda_handler(sample_event, None)
    assert response['statusCode'] == 200
    
    # Verify response contains expected fields
    body = json.loads(response['body'])
    assert 'message' in body
    assert 'analysis_triggered' in body
    assert 'scraped_files' in body
    assert 'search_type' in body

def test_invalid_form_data(s3_client, mock_s3_bucket, sample_event):
    """Test handling of invalid form data"""
    from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler
    
    # Test with invalid brand
    invalid_event = sample_event.copy()
    invalid_event['brand'] = 'INVALID_BRAND'
    
    response = lambda_handler(invalid_event, None)
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert 'error' in body 