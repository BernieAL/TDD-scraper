"""
Tests for the form submission handler lambda function.
Tests:
1. Successful form submission
2. Form data validation
3. Query hash generation
4. S3 storage
5. Pipeline trigger
"""

import pytest
import json
import boto3
from datetime import datetime
from unittest.mock import patch

from backend.aws.lambda_functions.form_submission.form_handler import handle_form_submit, gen_query_hash

@pytest.fixture
def sample_form_data():
    """Sample form submission data"""
    return {
        'brand': 'PRADA',
        'category': 'BAGS',
        'specific_item': 'TOTE',
        'user_email': 'test@example.com'
    }

@patch('backend.aws.lambda_functions.form_submission.form_handler.datetime')
def test_query_hash_generation(mock_datetime, sample_form_data):
    """Test query hash generation"""
    # Mock datetime to return a fixed date
    mock_datetime.now.return_value = datetime(2024, 1, 15)
    
    # Test hash generation
    hash1 = gen_query_hash('PRADA', 'BAGS', 'TOTE')
    assert len(hash1) == 12
    assert hash1.isalnum()
    
    # Same inputs should generate same hash
    hash2 = gen_query_hash('PRADA', 'BAGS', 'TOTE')
    assert hash2 == hash1
    
    # Different inputs should generate different hash
    hash3 = gen_query_hash('PRADA', 'BAGS', 'DIFFERENT')
    assert hash3 != hash1
    
    # Different date should generate different hash
    mock_datetime.now.return_value = datetime(2024, 1, 16)
    hash4 = gen_query_hash('PRADA', 'BAGS', 'TOTE')
    assert hash4 != hash1

def test_successful_form_submission(sample_form_data, mock_s3_bucket, mock_lambda_function):
    """Test successful form submission"""
    with patch('backend.aws.lambda_functions.form_submission.form_handler.datetime') as mock_datetime:
        # Mock datetime to return a fixed date
        mock_datetime.now.return_value = datetime(2024, 1, 15)
        
        # Submit form
        response = handle_form_submit(sample_form_data, None)
        
        # Verify response
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert 'query_hash' in body
        assert 'message' in body
        assert body['message'] == 'Form submission processed'
        
        # Verify S3 storage
        s3 = boto3.client('s3')
        stored_data = json.loads(s3.get_object(
            Bucket=mock_s3_bucket,
            Key=f"queries/{body['query_hash']}/form-params.json"
        )['Body'].read())
        
        assert stored_data['brand'] == 'PRADA'
        assert stored_data['category'] == 'BAGS'
        assert stored_data['specific_item'] == 'TOTE'
        assert stored_data['email'] == 'test@example.com'
        assert stored_data['query_hash'] == body['query_hash']

def test_form_data_validation(mock_s3_bucket, mock_lambda_function):
    """Test form data validation"""
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

def test_pipeline_trigger(sample_form_data, mock_s3_bucket, mock_lambda_function):
    """Test pipeline orchestrator trigger"""
    with patch('backend.aws.lambda_functions.form_submission.form_handler.datetime') as mock_datetime:
        # Mock datetime to return a fixed date
        mock_datetime.now.return_value = datetime(2024, 1, 15)
        
        # Submit form
        response = handle_form_submit(sample_form_data, None)
        body = json.loads(response['body'])
        query_hash = body['query_hash']
        
        # Verify Lambda invocation
        lambda_client = boto3.client('lambda')
        invocations = lambda_client.list_invocations(
            FunctionName=mock_lambda_function
        )
        assert len(invocations['Invocations']) > 0
        
        # Verify invocation payload
        last_invocation = invocations['Invocations'][-1]
        payload = json.loads(last_invocation['Payload'])
        assert payload['query_hash'] == query_hash 