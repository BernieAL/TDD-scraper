"""
Tests for the analysis worker Lambda function.
"""

import json
import pytest
from datetime import datetime
from typing import Dict, Any
import boto3
from moto import mock_s3, mock_dynamodb
import os
import sys

from backend.workers.analysis_worker.analysis_worker_lambda import lambda_handler, analyze_price_changes

@pytest.fixture
def sample_scraped_data():
    """Sample scraped data for testing."""
    return {
        "items": [
            {
                "url": "https://example.com/test1",
                "title": "Test Item 1",
                "price": 100.00,
                "brand": "Test Brand",
                "category": "Test Category"
            },
            {
                "url": "https://example.com/test2",
                "title": "Test Item 2",
                "price": 200.00,
                "brand": "Test Brand",
                "category": "Test Category"
            }
        ],
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "source": "test_source",
            "query_hash": "test_hash"
        }
    }

@pytest.fixture
def sample_historical_data():
    """Sample historical data for testing."""
    return {
        "test_hash": {
            "items": [
                {
                    "url": "https://example.com/test1",
                    "title": "Test Item 1",
                    "price": 110.00,
                    "brand": "Test Brand",
                    "category": "Test Category"
                },
                {
                    "url": "https://example.com/test2",
                    "title": "Test Item 2",
                    "price": 190.00,
                    "brand": "Test Brand",
                    "category": "Test Category"
                }
            ],
            "timestamp": (datetime.now() - datetime.timedelta(days=1)).isoformat()
        }
    }

def test_analyze_price_changes(sample_scraped_data, sample_historical_data):
    """Test price change analysis."""
    analysis_result = analyze_price_changes(
        sample_scraped_data["items"],
        sample_historical_data["test_hash"]["items"]
    )
    
    assert len(analysis_result["changes"]) == 2
    assert analysis_result["metadata"]["total_items"] == 2
    assert analysis_result["metadata"]["items_with_changes"] == 2
    
    # Verify price changes
    changes = {item["url"]: item for item in analysis_result["changes"]}
    assert changes["https://example.com/test1"]["price_change"] == -10.00
    assert changes["https://example.com/test2"]["price_change"] == 10.00

def test_lambda_handler(sample_scraped_data, sample_historical_data):
    """Test the Lambda handler function."""
    with mock_s3(), mock_dynamodb():
        # Create S3 bucket and upload scraped data
        s3 = boto3.client('s3')
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        
        s3.put_object(
            Bucket=bucket_name,
            Key="scraped/test_hash.json",
            Body=json.dumps(sample_scraped_data)
        )
        
        # Create DynamoDB table and insert historical data
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.create_table(
            TableName="test-table",
            KeySchema=[{"AttributeName": "query_hash", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "query_hash", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST"
        )
        
        table.put_item(Item=sample_historical_data)
        
        # Test handler
        event = {
            "bucket_name": bucket_name,
            "query_hash": "test_hash",
            "table_name": "test-table"
        }
        
        response = lambda_handler(event, None)
        assert response["statusCode"] == 200
        response_body = json.loads(response["body"])
        assert "analysis_results" in response_body
        assert "report_path" in response_body

def test_lambda_handler_error_handling():
    """Test error handling in the Lambda handler."""
    with mock_s3(), mock_dynamodb():
        # Test with invalid bucket name
        event = {
            "bucket_name": "non-existent-bucket",
            "query_hash": "test_hash",
            "table_name": "test-table"
        }
        
        response = lambda_handler(event, None)
        assert response["statusCode"] == 500
        response_body = json.loads(response["body"])
        assert "error" in response_body

def test_lambda_handler_specific_search(
    mock_s3_bucket: str,
    mock_dynamodb_table: Any,
    mock_sns_topic: str,
    sample_scraped_data: Dict[str, Any]
):
    """Test the Lambda handler with specific search data."""
    # Set up filtered data in S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=mock_s3_bucket,
        Key='queries/test123/filtered/filtered_data.json',
        Body=json.dumps(sample_scraped_data['data'])
    )
    
    # Create test event for specific search
    event = {
        'query_hash': 'test123',
        'paths': {
            'raw': 'queries/test123/raw',
            'filtered': 'queries/test123/filtered',
            'analysis': 'queries/test123/analysis'
        },
        'is_specific_search': True,
        'email': 'test@example.com'
    }
    
    # Run Lambda handler
    response = lambda_handler(event, None)
    
    # Verify response
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['message'] == 'Analysis completed successfully'
    assert body['search_type'] == 'specific' 