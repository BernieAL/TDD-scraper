"""
Tests for the form submission handler lambda function.

Tests:
1. Successful form submission
2. Form data validation
3. Query hash generation
4. S3 storage
5. Pipeline orchestrator triggering
6. Error handling
"""

import pytest
from moto import mock_s3, mock_lambda
import json
import boto3
from datetime import datetime
import os

from backend.aws.lambda_functions.form_submission.form_handler import handle_form_submit

@pytest.fixture(autouse=True)
def setup_aws_credentials():
    """Set up AWS credentials for all tests"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def sample_form_data():
    """Sample form submission data"""
    return {
        'brand': 'PRADA',
        'category': 'BAGS',
        'specific_item': 'TOTE',
        'user_email': 'test@example.com'
    }

@mock_s3
@mock_lambda
def test_successful_form_submission(sample_form_data):
    """Test successful form submission"""
    # Create mock S3 bucket
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='scraper-data-bucket')
    
    # Create mock Lambda function
    lambda_client = boto3.client('lambda')
    lambda_client.create_function(
        FunctionName='scraper-worker',
        Runtime='python3.10',
        Handler='index.handler',
        Role='arn:aws:iam::123456789012:role/lambda-role',
        Code={'ZipFile': b''}
    )
    
    # Submit form
    response = handle_form_submit(sample_form_data, None)
    
    # Verify response
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert 'query_hash' in body
    assert 'message' in body
    assert body['message'] == 'Form submission processed'
    
    # Verify S3 storage
    stored_data = json.loads(s3.get_object(
        Bucket='scraper-data-bucket',
        Key=f"queries/{body['query_hash']}/form-params.json"
    )['Body'].read())
    
    assert stored_data['brand'] == 'PRADA'
    assert stored_data['category'] == 'BAGS'
    assert stored_data['specific_item'] == 'TOTE'
    assert stored_data['email'] == 'test@example.com'
    assert stored_data['query_hash'] == body['query_hash']

@mock_s3
@mock_lambda
def test_form_data_validation():
    """Test form data validation"""
    # Create mock S3 bucket
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='scraper-data-bucket')
    
    # Create mock Lambda function
    lambda_client = boto3.client('lambda')
    lambda_client.create_function(
        FunctionName='scraper-worker',
        Runtime='python3.10',
        Handler='index.handler',
        Role='arn:aws:iam::123456789012:role/lambda-role',
        Code={'ZipFile': b''}
    )
    
    # Test missing required fields
    invalid_data = {
        'brand': 'PRADA',
        'user_email': 'test@example.com'
    }
    response = handle_form_submit(invalid_data, None)
    assert response['statusCode'] == 500
    
    # Test invalid email
    invalid_email = {
        'brand': 'PRADA',
        'category': 'BAGS',
        'user_email': 'invalid-email'
    }
    response = handle_form_submit(invalid_email, None)
    assert response['statusCode'] == 500

@mock_s3
@mock_lambda
def test_query_hash_generation(sample_form_data):
    """Test query hash generation"""
    # Create mock S3 bucket
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='scraper-data-bucket')
    
    # Create mock Lambda function
    lambda_client = boto3.client('lambda')
    lambda_client.create_function(
        FunctionName='scraper-worker',
        Runtime='python3.10',
        Handler='index.handler',
        Role='arn:aws:iam::123456789012:role/lambda-role',
        Code={'ZipFile': b''}
    )
    
    # Submit form
    response = handle_form_submit(sample_form_data, None)
    body = json.loads(response['body'])
    query_hash = body['query_hash']
    
    # Verify hash format
    assert len(query_hash) == 12
    assert query_hash.isalnum()
    
    # Verify same query generates same hash
    response2 = handle_form_submit(sample_form_data, None)
    body2 = json.loads(response2['body'])
    assert body2['query_hash'] == query_hash
    
    # Verify different query generates different hash
    different_data = sample_form_data.copy()
    different_data['specific_item'] = 'DIFFERENT'
    response3 = handle_form_submit(different_data, None)
    body3 = json.loads(response3['body'])
    assert body3['query_hash'] != query_hash

@mock_s3
@mock_lambda
def test_pipeline_trigger(sample_form_data):
    """Test pipeline orchestrator trigger"""
    # Create mock S3 bucket
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='scraper-data-bucket')
    
    # Create mock Lambda function
    lambda_client = boto3.client('lambda')
    lambda_client.create_function(
        FunctionName='scraper-worker',
        Runtime='python3.10',
        Handler='index.handler',
        Role='arn:aws:iam::123456789012:role/lambda-role',
        Code={'ZipFile': b''}
    )
    
    # Submit form
    response = handle_form_submit(sample_form_data, None)
    body = json.loads(response['body'])
    query_hash = body['query_hash']
    
    # Verify Lambda invocation
    invocations = lambda_client.list_invocations(
        FunctionName='scraper-worker'
    )
    assert len(invocations['Invocations']) > 0
    
    # Verify invocation payload
    last_invocation = invocations['Invocations'][-1]
    payload = json.loads(last_invocation['Payload'])
    assert payload['query_hash'] == query_hash

@mock_s3
@mock_lambda
def test_error_handling():
    """Test error handling in form submission"""
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client to raise exception
        mock_s3 = mock_boto3.return_value
        mock_s3.put_object.side_effect = Exception("S3 error")
        
        # Execute handler with invalid data
        response = handle_form_submit({}, None)
        
        # Verify error response
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body
        assert 'S3 error' in body['error']

@mock_s3
@mock_lambda
def test_data_cleaning(sample_form_data):
    """Test form data cleaning"""
    with patch('boto3.client') as mock_boto3:
        # Mock clients
        mock_s3 = mock_boto3.return_value
        mock_s3.put_object.return_value = {}
        mock_lambda = mock_boto3.return_value
        mock_lambda.invoke.return_value = {'StatusCode': 200}
        
        # Execute handler
        handle_form_submit(sample_form_data, None)
        
        # Verify cleaned data in S3
        stored_data = json.loads(mock_s3.put_object.call_args[1]['Body'])
        assert stored_data['brand'] == 'PRADA'  # Uppercase and stripped
        assert stored_data['category'] == 'BAGS'  # Uppercase and stripped
        assert stored_data['specific_item'] == 'TOTE'  # Uppercase and stripped
