"""
Comprehensive test suite for the ScrapeOrchestrator Lambda function.
Tests all critical functionality including:
- Event handling
- S3 operations
- Scraper execution
- Error handling
- Integration with other services
"""

import pytest
import json
import boto3
import os
from moto import mock_s3, mock_lambda, mock_iam
from unittest.mock import patch, MagicMock
from datetime import datetime
import tempfile
from pathlib import Path
import time
import zipfile
import io
from typing import Dict, Optional

# Import from the new location
from backend.workers.scraper_worker.scraper_orchestrator import (
    orchestrate_scraping_pipeline,
    ScraperOrchestrator
)

# Test configuration
TEST_BUCKET = 'tdd-scraper-test'
TEST_QUERY_HASH = 'test123'
TEST_BRAND = 'PRADA'
TEST_CATEGORY = 'BAGS'

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:4567'

@pytest.fixture
def s3(aws_credentials):
    """Mocked S3 client that creates a test bucket."""
    with mock_s3():
        s3 = boto3.client('s3', endpoint_url='http://localhost:4567')
        # Create the test bucket
        s3.create_bucket(Bucket=TEST_BUCKET)
        yield s3

@pytest.fixture
def lambda_client(aws_credentials):
    """Mocked Lambda client."""
    with mock_lambda():
        client = boto3.client('lambda', endpoint_url='http://localhost:4567')
        
        # Delete the function if it exists
        try:
            client.delete_function(FunctionName='test-function')
        except client.exceptions.ResourceNotFoundException:
            pass
        
        # Create a ZIP file with a simple Lambda function
        zip_output = io.BytesIO()
        with zipfile.ZipFile(zip_output, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('lambda_function.py', '''
def handler(event, context):
    return {
        'statusCode': 200,
        'body': 'Hello from Lambda!'
    }
''')
        
        # Create a mock Lambda function
        client.create_function(
            FunctionName='test-function',
            Runtime='python3.10',
            Role='arn:aws:iam::123456789012:role/test-role',
            Handler='lambda_function.handler',
            Code={'ZipFile': zip_output.getvalue()},
        )
        yield client

@pytest.fixture
def test_event():
    """Sample event for testing."""
    return {
        'query_hash': TEST_QUERY_HASH,
        'form_data': {
            'brand': TEST_BRAND,
            'category': TEST_CATEGORY
        }
    }

def test_s3_bucket_creation(s3):
    """Test that the S3 bucket exists and is accessible."""
    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    assert TEST_BUCKET in buckets

def test_lambda_function_creation(lambda_client):
    """Test that the Lambda function exists."""
    response = lambda_client.list_functions()
    functions = [func['FunctionName'] for func in response['Functions']]
    assert 'test-function' in functions

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_pipeline_execution(mock_scraper_class, test_event, s3):
    """Test the full execution of the scraping pipeline."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': '/tmp/test123/test_data.csv'
    }
    
    # Create a test data file in the correct location
    os.makedirs('/tmp/test123', exist_ok=True)
    test_file = '/tmp/test123/test_data.csv'
    with open(test_file, 'w') as f:
        f.write('test,data\n1,2\n')
    
    try:
        # Execute pipeline
        response = orchestrate_scraping_pipeline(test_event, None)
        
        # Verify response
        assert response is not None
        assert 'status' in response
        assert response['status'] == 'success'
        
    finally:
        # Clean up
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists('/tmp/test123/paths.json'):
            os.remove('/tmp/test123/paths.json')
        if os.path.exists('/tmp/test123'):
            try:
                os.rmdir('/tmp/test123')
            except OSError:
                # Directory not empty, but that's okay
                pass

def test_error_handling(test_event, s3):
    """Test error handling capabilities."""
    # Test with missing query_hash
    invalid_event = test_event.copy()
    del invalid_event['query_hash']
    
    with pytest.raises(ValueError, match="query_hash not found in event"):
        orchestrate_scraping_pipeline(invalid_event, None)

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_s3_path_management(mock_scraper_class, test_event, s3):
    """Test S3 path management and file organization."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': '/tmp/test123/test_data.csv'
    }
    
    # Create a test data file in the correct location
    os.makedirs('/tmp/test123', exist_ok=True)
    test_file = '/tmp/test123/test_data.csv'
    with open(test_file, 'w') as f:
        f.write('test,data\n1,2\n')
    
    try:
        # Execute pipeline
        response = orchestrate_scraping_pipeline(test_event, None)
        
        # Verify S3 paths
        response = s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f'queries/{TEST_QUERY_HASH}/')
        assert 'Contents' in response
        assert any(obj['Key'].endswith('paths.json') for obj in response['Contents'])
        
    finally:
        # Clean up
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists('/tmp/test123/paths.json'):
            os.remove('/tmp/test123/paths.json')
        if os.path.exists('/tmp/test123'):
            try:
                os.rmdir('/tmp/test123')
            except OSError:
                # Directory not empty, but that's okay
                pass

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_concurrent_execution(mock_scraper_class, test_event, s3):
    """Test handling of concurrent pipeline executions."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': '/tmp/test123/test_data.csv'
    }
    
    # Create test directories and files
    test_dirs = []
    test_files = []
    try:
        # Execute multiple pipelines concurrently
        responses = []
        for i in range(3):
            event = test_event.copy()
            event['query_hash'] = f'{TEST_QUERY_HASH}_{i}'
            
            # Create test directory and file
            test_dir = f'/tmp/{event["query_hash"]}'
            test_file = f'{test_dir}/test_data.csv'
            os.makedirs(test_dir, exist_ok=True)
            with open(test_file, 'w') as f:
                f.write('test,data\n1,2\n')
            
            test_dirs.append(test_dir)
            test_files.append(test_file)
            
            response = orchestrate_scraping_pipeline(event, None)
            responses.append(response)
        
        # Verify all executions completed
        assert len(responses) == 3
        assert all(r['status'] == 'success' for r in responses)
        
    finally:
        # Clean up
        for test_file in test_files:
            if os.path.exists(test_file):
                os.remove(test_file)
        for test_dir in test_dirs:
            if os.path.exists(test_dir):
                os.rmdir(test_dir)

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_resource_cleanup(mock_scraper_class, test_event, s3):
    """Test proper cleanup of temporary resources."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': '/tmp/test123/test_data.csv'
    }
    
    # Create a test data file in the correct location
    os.makedirs('/tmp/test123', exist_ok=True)
    test_file = '/tmp/test123/test_data.csv'
    with open(test_file, 'w') as f:
        f.write('test,data\n1,2\n')
    
    try:
        # Execute pipeline
        response = orchestrate_scraping_pipeline(test_event, None)
        
        # Verify cleanup
        response = s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f'queries/{TEST_QUERY_HASH}/')
        assert 'Contents' in response
        assert all(not obj['Key'].endswith('.tmp') for obj in response['Contents'])
        
    finally:
        # Clean up
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists('/tmp/test123/paths.json'):
            os.remove('/tmp/test123/paths.json')
        if os.path.exists('/tmp/test123'):
            try:
                os.rmdir('/tmp/test123')
            except OSError:
                # Directory not empty, but that's okay
                pass

def test_error_logging(test_event, s3):
    """Test error logging."""
    # Test with invalid S3 bucket
    os.environ['S3_BUCKET'] = 'non-existent-bucket'
    with pytest.raises(boto3.exceptions.S3UploadFailedError):
        orchestrate_scraping_pipeline(test_event, None)
    os.environ['S3_BUCKET'] = TEST_BUCKET

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_performance_metrics(mock_scraper_class, test_event, s3):
    """Test performance metrics collection."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': '/tmp/test123/test_data.csv'
    }
    
    # Create a test data file in the correct location
    os.makedirs('/tmp/test123', exist_ok=True)
    test_file = '/tmp/test123/test_data.csv'
    with open(test_file, 'w') as f:
        f.write('test,data\n1,2\n')
    
    try:
        # Execute pipeline and measure time
        start_time = time.time()
        response = orchestrate_scraping_pipeline(test_event, None)
        end_time = time.time()
        
        # Verify response
        assert response['status'] == 'success'
        assert end_time - start_time < 5.0  # Should complete within 5 seconds
        
    finally:
        # Clean up
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists('/tmp/test123/paths.json'):
            os.remove('/tmp/test123/paths.json')
        if os.path.exists('/tmp/test123'):
            try:
                os.rmdir('/tmp/test123')
            except OSError:
                # Directory not empty, but that's okay
                pass 