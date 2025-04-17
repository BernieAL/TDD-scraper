"""
Tests for the report worker Lambda function.
"""

import json
import pytest
from datetime import datetime
from typing import Dict, Any
import boto3
from moto import mock_s3, mock_dynamodb, mock_sns
import os
import sys

from backend.workers.report_worker.report_worker_lambda import lambda_handler, validate_event
from backend.workers.report_worker.report_generator import ReportGenerator

@pytest.fixture
def sample_report_data():
    """Sample data for testing report generation."""
    return {
        "email": "test@example.com",
        "query_hash": "test_hash",
        "bucket_name": "test-bucket",
        "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:test-topic",
        "analysis_results": {
            "changes": [
                {
                    "url": "https://example.com/test1",
                    "title": "Test Item 1",
                    "old_price": 100.00,
                    "new_price": 90.00,
                    "price_change": -10.00,
                    "percentage_change": -10.0
                }
            ],
            "metadata": {
                "total_items": 1,
                "items_with_changes": 1,
                "average_price_change": -10.00,
                "average_percentage_change": -10.0
            }
        }
    }

def test_validate_event(sample_report_data):
    """Test event validation."""
    assert validate_event(sample_report_data) is True
    
    # Test missing required fields
    invalid_data = sample_report_data.copy()
    del invalid_data["email"]
    assert validate_event(invalid_data) is False

def test_lambda_handler(sample_report_data):
    """Test the Lambda handler function."""
    with mock_s3(), mock_dynamodb(), mock_sns():
        # Create S3 bucket
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=sample_report_data["bucket_name"])
        
        # Create SNS topic
        sns = boto3.client('sns')
        topic = sns.create_topic(Name='test-topic')
        sample_report_data["sns_topic_arn"] = topic["TopicArn"]
        
        # Test handler
        response = lambda_handler(sample_report_data, None)
        assert response["statusCode"] == 200
        response_body = json.loads(response["body"])
        assert "message" in response_body
        assert "report_path" in response_body
        
        # Verify report was saved to S3
        s3_objects = s3.list_objects_v2(
            Bucket=sample_report_data["bucket_name"],
            Prefix=f"reports/{sample_report_data['query_hash']}"
        )
        assert len(s3_objects.get('Contents', [])) > 0

def test_lambda_handler_error_handling(sample_report_data):
    """Test error handling in the Lambda handler."""
    # Test with invalid bucket name
    invalid_data = sample_report_data.copy()
    invalid_data["bucket_name"] = "non-existent-bucket"
    
    with mock_s3(), mock_dynamodb(), mock_sns():
        response = lambda_handler(invalid_data, None)
        assert response["statusCode"] == 500
        response_body = json.loads(response["body"])
        assert "error" in response_body

def test_lambda_handler_empty_analysis(sample_report_data):
    """Test the Lambda handler with empty analysis results."""
    empty_data = sample_report_data.copy()
    empty_data["analysis_results"] = {
        "changes": [],
        "metadata": {
            "total_items": 0,
            "items_with_changes": 0,
            "average_price_change": 0.0,
            "average_percentage_change": 0.0
        }
    }
    
    with mock_s3(), mock_dynamodb(), mock_sns():
        # Create S3 bucket
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=empty_data["bucket_name"])
        
        # Create SNS topic
        sns = boto3.client('sns')
        topic = sns.create_topic(Name='test-topic')
        empty_data["sns_topic_arn"] = topic["TopicArn"]
        
        # Test handler
        response = lambda_handler(empty_data, None)
        assert response["statusCode"] == 200
        response_body = json.loads(response["body"])
        assert "message" in response_body
        assert "report_path" in response_body 