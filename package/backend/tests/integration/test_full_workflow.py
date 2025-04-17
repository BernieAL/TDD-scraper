"""
Integration tests for the complete workflow.
Tests:
1. Form submission to final analysis
2. Data flow through all stages
3. Error propagation
4. Local vs AWS execution
"""

import pytest
import json
import boto3
from datetime import datetime
from unittest.mock import patch, MagicMock
from moto import mock_s3, mock_lambda, mock_dynamodb, mock_sns

@pytest.fixture
def sample_form_data():
    """Sample form submission data"""
    return {
        "brand": "PRADA",
        "category": "BAGS",
        "min_price": 1000,
        "max_price": 2000
    }

@pytest.fixture
def mock_aws_services():
    """Set up mock AWS services"""
    with mock_s3(), mock_lambda(), mock_dynamodb(), mock_sns():
        # Create S3 bucket
        s3 = boto3.client('s3')
        bucket_name = 'scraper-data-bucket'
        s3.create_bucket(Bucket=bucket_name)
        
        # Create Lambda functions
        lambda_client = boto3.client('lambda')
        functions = {
            'form-submission': lambda_client.create_function(
                FunctionName='form-submission',
                Runtime='python3.10',
                Handler='form_handler.handle_form_submit',
                Role='arn:aws:iam::123456789012:role/lambda-role',
                Code={'ZipFile': b'empty'}
            ),
            'scrape-orchestrator': lambda_client.create_function(
                FunctionName='scrape-orchestrator',
                Runtime='python3.10',
                Handler='pipeline_orchestrator.orchestrate_scraping_pipeline',
                Role='arn:aws:iam::123456789012:role/lambda-role',
                Code={'ZipFile': b'empty'}
            ),
            'price-analyzer': lambda_client.create_function(
                FunctionName='price-analyzer',
                Runtime='python3.10',
                Handler='price_analyzer.analyze_prices',
                Role='arn:aws:iam::123456789012:role/lambda-role',
                Code={'ZipFile': b'empty'}
            ),
            'scraper-worker': lambda_client.create_function(
                FunctionName='scraper-worker',
                Runtime='python3.10',
                Handler='scraper_worker.scrape_products',
                Role='arn:aws:iam::123456789012:role/lambda-role',
                Code={'ZipFile': b'empty'}
            )
        }
        
        # Create DynamoDB table
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
        
        # Create SNS topic
        sns = boto3.client('sns')
        topic_arn = sns.create_topic(Name='scraper-notifications')['TopicArn']
        
        yield {
            'bucket_name': bucket_name,
            'functions': functions,
            'table_name': table_name,
            'topic_arn': topic_arn
        }

def test_complete_workflow(sample_form_data, mock_aws_services):
    """Test the complete workflow from form submission to analysis"""
    from backend.aws.lambda_functions.form_handler.form_handler import handle_form_submit
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    # Mock scraper data
    mock_scraper_data = [
        {
            "product_id": "1",
            "brand": "PRADA",
            "name": "Test Bag",
            "price": 1000.0,
            "currency": "USD",
            "url": "http://example.com/1",
            "source": "italist"
        }
    ]
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(mock_scraper_data).encode('utf-8')
            )
        }
        mock_boto3.return_value = mock_s3_client
        
        # Step 1: Form submission
        form_response = handle_form_submit(sample_form_data, None)
        assert form_response['statusCode'] == 200
        form_result = json.loads(form_response['body'])
        assert 'query_hash' in form_result
        
        # Step 2: Scrape orchestration
        orchestrator_event = {
            "query_hash": form_result['query_hash'],
            "brand": sample_form_data['brand'],
            "category": sample_form_data['category'],
            "paths": form_result['paths']
        }
        orchestrator_response = orchestrate_scraping_pipeline(orchestrator_event, None)
        assert orchestrator_response['statusCode'] == 200
        
        # Step 3: Price analysis
        analyzer_event = {
            "query_hash": form_result['query_hash'],
            "paths": form_result['paths']
        }
        analyzer_response = analyze_prices(analyzer_event, None)
        assert analyzer_response['statusCode'] == 200
        
        # Verify final results
        analyzer_result = json.loads(analyzer_response['body'])
        assert 'analysis' in analyzer_result
        assert 'products' in analyzer_result

def test_error_propagation(sample_form_data, mock_aws_services):
    """Test error propagation through the workflow"""
    from backend.aws.lambda_functions.form_handler.form_handler import handle_form_submit
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client to raise an error
        mock_s3_client = MagicMock()
        mock_s3_client.put_object.side_effect = Exception("S3 Error")
        mock_boto3.return_value = mock_s3_client
        
        # Step 1: Form submission should fail
        form_response = handle_form_submit(sample_form_data, None)
        assert form_response['statusCode'] == 500
        form_result = json.loads(form_response['body'])
        assert 'error' in form_result

def test_local_execution(sample_form_data, mock_aws_services):
    """Test local execution mode"""
    from backend.aws.lambda_functions.form_handler.form_handler import handle_form_submit
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    from backend.aws.lambda_functions.price_analyzer.price_analyzer import analyze_prices
    
    # Add local execution flag
    local_form_data = sample_form_data.copy()
    local_form_data['local'] = True
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps([]).encode('utf-8')
            )
        }
        mock_boto3.return_value = mock_s3_client
        
        # Step 1: Form submission
        form_response = handle_form_submit(local_form_data, None)
        assert form_response['statusCode'] == 200
        form_result = json.loads(form_response['body'])
        assert 'local_paths' in form_result
        
        # Step 2: Scrape orchestration
        orchestrator_event = {
            "query_hash": form_result['query_hash'],
            "brand": local_form_data['brand'],
            "category": local_form_data['category'],
            "paths": form_result['paths'],
            "local": True
        }
        orchestrator_response = orchestrate_scraping_pipeline(orchestrator_event, None)
        assert orchestrator_response['statusCode'] == 200
        
        # Step 3: Price analysis
        analyzer_event = {
            "query_hash": form_result['query_hash'],
            "paths": form_result['paths'],
            "local": True
        }
        analyzer_response = analyze_prices(analyzer_event, None)
        assert analyzer_response['statusCode'] == 200 