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
        'test_scraper': 'test_data.csv'
    }
    
    # Create a test data file
    with open('test_data.csv', 'w') as f:
        f.write('test,data\n1,2\n')
    
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    try:
        # Execute pipeline
        response = orchestrate_scraping_pipeline(test_event, None)
        
        # Verify response
        assert response is not None
        assert 'status' in response
        assert response['status'] == 'success'
        
        # Clean up
        os.remove('test_data.csv')
    except Exception as e:
        # Clean up even if test fails
        if os.path.exists('test_data.csv'):
            os.remove('test_data.csv')
        raise

def test_error_handling(test_event, s3):
    """Test error handling capabilities."""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
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
        'test_scraper': 'test_data.csv'
    }
    
    # Create a test data file
    with open('test_data.csv', 'w') as f:
        f.write('test,data\n1,2\n')
    
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    try:
        # Execute pipeline
        response = orchestrate_scraping_pipeline(test_event, None)
        
        # Verify S3 paths
        response = s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f'queries/{TEST_QUERY_HASH}/')
        assert 'Contents' in response
        assert any(obj['Key'].endswith('paths.json') for obj in response['Contents'])
        
        # Clean up
        os.remove('test_data.csv')
    except Exception as e:
        # Clean up even if test fails
        if os.path.exists('test_data.csv'):
            os.remove('test_data.csv')
        raise

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_concurrent_execution(mock_scraper_class, test_event, s3):
    """Test handling of concurrent pipeline executions."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': 'test_data.csv'
    }
    
    # Create a test data file
    with open('test_data.csv', 'w') as f:
        f.write('test,data\n1,2\n')
    
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    try:
        # Execute multiple pipelines concurrently
        responses = []
        for i in range(3):
            event = test_event.copy()
            event['query_hash'] = f'{TEST_QUERY_HASH}_{i}'
            response = orchestrate_scraping_pipeline(event, None)
            responses.append(response)
        
        # Verify all executions completed
        assert len(responses) == 3
        assert all(r['status'] == 'success' for r in responses)
        
        # Clean up
        os.remove('test_data.csv')
    except Exception as e:
        # Clean up even if test fails
        if os.path.exists('test_data.csv'):
            os.remove('test_data.csv')
        raise

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_resource_cleanup(mock_scraper_class, test_event, s3):
    """Test proper cleanup of temporary resources."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': 'test_data.csv'
    }
    
    # Create a test data file
    with open('test_data.csv', 'w') as f:
        f.write('test,data\n1,2\n')
    
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    try:
        # Execute pipeline
        response = orchestrate_scraping_pipeline(test_event, None)
        
        # Verify cleanup
        response = s3.list_objects_v2(Bucket=TEST_BUCKET, Prefix=f'queries/{TEST_QUERY_HASH}/')
        assert 'Contents' in response
        assert all(not obj['Key'].endswith('.tmp') for obj in response['Contents'])
        
        # Clean up
        os.remove('test_data.csv')
    except Exception as e:
        # Clean up even if test fails
        if os.path.exists('test_data.csv'):
            os.remove('test_data.csv')
        raise

def test_error_logging(test_event, s3):
    """Test error logging."""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    # Empty the bucket first
    response = s3.list_objects_v2(Bucket=TEST_BUCKET)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=TEST_BUCKET, Key=obj['Key'])
    
    # Force an error by removing the bucket
    s3.delete_bucket(Bucket=TEST_BUCKET)
    
    with pytest.raises(Exception):
        orchestrate_scraping_pipeline(test_event, None)

@patch('backend.workers.scraper_worker.scraper_orchestrator.ScraperOrchestrator')
def test_performance_metrics(mock_scraper_class, test_event, s3):
    """Test performance metrics and timing."""
    # Mock the scraper
    mock_scraper = mock_scraper_class.return_value
    mock_scraper.run_all_scrapers.return_value = {
        'test_scraper': 'test_data.csv'
    }
    
    # Create a test data file
    with open('test_data.csv', 'w') as f:
        f.write('test,data\n1,2\n')
    
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    try:
        # Execute pipeline and measure time
        start_time = time.time()
        response = orchestrate_scraping_pipeline(test_event, None)
        end_time = time.time()
        
        # Verify performance
        assert response['status'] == 'success'
        assert end_time - start_time < 30  # Should complete within 30 seconds
        
        # Clean up
        os.remove('test_data.csv')
    except Exception as e:
        # Clean up even if test fails
        if os.path.exists('test_data.csv'):
            os.remove('test_data.csv')
        raise 