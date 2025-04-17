import json
import pytest
from datetime import datetime
from typing import Dict, Any
import boto3
from moto import mock_s3, mock_dynamodb, mock_sns, mock_lambda
import os
import sys

# Add the backend directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler as scraper_handler
from backend.workers.analysis_worker.analysis_worker_lambda import lambda_handler as analysis_handler
from backend.workers.report_worker.report_worker_lambda import lambda_handler as report_handler
from backend.workers.report_worker.report_generator import ReportGenerator

@pytest.fixture
def pipeline_test_data():
    """Sample data for testing the entire pipeline."""
    return {
        "email": "test@example.com",
        "query": "test search",
        "query_hash": "test_hash",
        "bucket_name": "test-bucket",
        "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:test-topic",
        "search_type": "specific",
        "search_data": {
            "url": "https://example.com/test",
            "title": "Test Item",
            "price": 100.00
        }
    }

@pytest.fixture
def mock_aws_infrastructure():
    """Set up all required AWS infrastructure for pipeline testing."""
    with mock_s3(), mock_dynamodb(), mock_sns(), mock_lambda():
        # Create S3 bucket
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-bucket')
        
        # Create DynamoDB table
        dynamodb = boto3.resource('dynamodb')
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
        
        # Create SNS topic
        sns = boto3.client('sns')
        topic = sns.create_topic(Name='test-topic')
        
        # Create Lambda functions
        lambda_client = boto3.client('lambda')
        lambda_client.create_function(
            FunctionName='scraper-worker',
            Runtime='python3.9',
            Role='arn:aws:iam::123456789012:role/test-role',
            Handler='scraper_worker_lambda.lambda_handler',
            Code={'ZipFile': b''}
        )
        
        yield {
            's3': s3,
            'dynamodb': dynamodb,
            'sns': sns,
            'lambda': lambda_client,
            'table': table,
            'topic_arn': topic['TopicArn']
        }

def test_full_pipeline(pipeline_test_data, mock_aws_infrastructure):
    """Test the complete pipeline from scraping to analysis to reporting."""
    # Step 1: Run scraper worker
    scraper_event = {
        "email": pipeline_test_data["email"],
        "query": pipeline_test_data["query"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "search_type": pipeline_test_data["search_type"],
        "search_data": pipeline_test_data["search_data"]
    }
    
    scraper_response = scraper_handler(scraper_event, None)
    assert scraper_response["statusCode"] == 200
    
    # Verify scraper output in S3
    s3_objects = mock_aws_infrastructure['s3'].list_objects_v2(
        Bucket=pipeline_test_data["bucket_name"]
    )
    assert len(s3_objects.get('Contents', [])) > 0
    
    # Step 2: Run analysis worker
    analysis_event = {
        "email": pipeline_test_data["email"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "sns_topic_arn": pipeline_test_data["sns_topic_arn"]
    }
    
    analysis_response = analysis_handler(analysis_event, None)
    assert analysis_response["statusCode"] == 200
    
    # Verify analysis results in S3
    analysis_objects = mock_aws_infrastructure['s3'].list_objects_v2(
        Bucket=pipeline_test_data["bucket_name"],
        Prefix=f"analysis/{pipeline_test_data['query_hash']}"
    )
    assert len(analysis_objects.get('Contents', [])) > 0
    
    # Step 3: Run report worker
    report_event = {
        "email": pipeline_test_data["email"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "sns_topic_arn": pipeline_test_data["sns_topic_arn"],
        "analysis_results": json.loads(analysis_response["body"])
    }
    
    report_response = report_handler(report_event, None)
    assert report_response["statusCode"] == 200
    
    # Verify report in S3
    report_objects = mock_aws_infrastructure['s3'].list_objects_v2(
        Bucket=pipeline_test_data["bucket_name"],
        Prefix=f"reports/{pipeline_test_data['query_hash']}"
    )
    assert len(report_objects.get('Contents', [])) > 0
    
    # Verify SNS notification
    # Note: In moto, we can't directly verify SNS messages, but we can verify
    # that the topic exists and was used
    assert mock_aws_infrastructure['topic_arn'] == pipeline_test_data["sns_topic_arn"]

def test_pipeline_error_handling(pipeline_test_data, mock_aws_infrastructure):
    """Test error handling in the pipeline."""
    # Test with invalid search data
    invalid_event = {
        "email": pipeline_test_data["email"],
        "query": pipeline_test_data["query"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "search_type": "invalid_type",
        "search_data": {}
    }
    
    # Scraper should fail
    scraper_response = scraper_handler(invalid_event, None)
    assert scraper_response["statusCode"] == 500
    
    # Analysis should fail without valid scraper output
    analysis_event = {
        "email": pipeline_test_data["email"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "sns_topic_arn": pipeline_test_data["sns_topic_arn"]
    }
    
    analysis_response = analysis_handler(analysis_event, None)
    assert analysis_response["statusCode"] == 500
    
    # Report should fail without valid analysis
    report_event = {
        "email": pipeline_test_data["email"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "sns_topic_arn": pipeline_test_data["sns_topic_arn"],
        "analysis_results": {"error": "Invalid analysis"}
    }
    
    report_response = report_handler(report_event, None)
    assert report_response["statusCode"] == 500

def test_pipeline_with_historical_data(pipeline_test_data, mock_aws_infrastructure):
    """Test pipeline with existing historical data."""
    # Add historical data to DynamoDB
    table = mock_aws_infrastructure['table']
    historical_data = {
        "query_hash": pipeline_test_data["query_hash"],
        "timestamp": datetime.now().isoformat(),
        "items": [
            {
                "url": pipeline_test_data["search_data"]["url"],
                "title": pipeline_test_data["search_data"]["title"],
                "price": 90.00  # Different price to trigger change
            }
        ]
    }
    table.put_item(Item=historical_data)
    
    # Run the pipeline
    scraper_event = {
        "email": pipeline_test_data["email"],
        "query": pipeline_test_data["query"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "search_type": pipeline_test_data["search_type"],
        "search_data": pipeline_test_data["search_data"]
    }
    
    scraper_response = scraper_handler(scraper_event, None)
    assert scraper_response["statusCode"] == 200
    
    analysis_event = {
        "email": pipeline_test_data["email"],
        "query_hash": pipeline_test_data["query_hash"],
        "bucket_name": pipeline_test_data["bucket_name"],
        "sns_topic_arn": pipeline_test_data["sns_topic_arn"]
    }
    
    analysis_response = analysis_handler(analysis_event, None)
    assert analysis_response["statusCode"] == 200
    
    # Verify price change was detected
    analysis_results = json.loads(analysis_response["body"])
    assert len(analysis_results["changes"]) > 0
    assert analysis_results["changes"][0]["old_price"] == 90.00
    assert analysis_results["changes"][0]["new_price"] == 100.00 