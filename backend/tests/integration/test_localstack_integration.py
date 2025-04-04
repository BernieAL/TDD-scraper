import os
import json
import boto3
import pytest
import time
from moto import mock_s3, mock_lambda
from botocore.exceptions import ClientError

# Test configuration
TEST_BUCKET = "tdd-scraper-test"
TEST_FUNCTION_NAME = "test-function"
TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role"
LOCALSTACK_ENDPOINT = "http://localhost:4567"  # Updated to use backend LocalStack

@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="module")
def s3_client():
    """Create a boto3 S3 client with LocalStack endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

@pytest.fixture(scope="module")
def lambda_client():
    """Create a boto3 Lambda client with LocalStack endpoint."""
    return boto3.client(
        "lambda",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

@pytest.fixture(scope="module")
def test_bucket(s3_client):
    """Create and return a test S3 bucket."""
    try:
        s3_client.create_bucket(Bucket=TEST_BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
    return TEST_BUCKET

@pytest.fixture(scope="module")
def test_lambda_function(lambda_client, test_bucket):
    """Create and return a test Lambda function."""
    # Create a basic Lambda function
    function_code = """
def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
"""
    
    try:
        response = lambda_client.create_function(
            FunctionName=TEST_FUNCTION_NAME,
            Runtime="python3.10",
            Role=TEST_ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": function_code.encode()},
            Environment={
                "Variables": {
                    "S3_BUCKET": test_bucket
                }
            }
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        # Function already exists, get its configuration
        response = lambda_client.get_function(FunctionName=TEST_FUNCTION_NAME)
    
    return response["Configuration"]

def test_s3_bucket_operations(s3_client, test_bucket):
    """Test basic S3 bucket operations."""
    # Test bucket exists
    response = s3_client.list_buckets()
    assert test_bucket in [bucket["Name"] for bucket in response["Buckets"]]
    
    # Test file upload
    test_key = "test.txt"
    test_content = "Hello, LocalStack!"
    s3_client.put_object(
        Bucket=test_bucket,
        Key=test_key,
        Body=test_content
    )
    
    # Test file download
    response = s3_client.get_object(Bucket=test_bucket, Key=test_key)
    assert response["Body"].read().decode() == test_content

def test_lambda_invocation(lambda_client, test_lambda_function):
    """Test Lambda function invocation."""
    # Test successful invocation
    response = lambda_client.invoke(
        FunctionName=TEST_FUNCTION_NAME,
        Payload=json.dumps({"test": "data"})
    )
    assert response["StatusCode"] == 200
    
    # Test error handling
    with pytest.raises(ClientError) as exc_info:
        lambda_client.invoke(
            FunctionName="non-existent-function",
            Payload=json.dumps({"test": "data"})
        )
    assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_lambda_s3_integration(s3_client, lambda_client, test_bucket, test_lambda_function):
    """Test integration between Lambda and S3."""
    # Upload a test file
    test_key = "lambda-test.txt"
    test_content = "Lambda test content"
    s3_client.put_object(
        Bucket=test_bucket,
        Key=test_key,
        Body=test_content
    )
    
    # Invoke Lambda with S3 event
    s3_event = {
        "Records": [{
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2024-01-01T00:00:00.000Z",
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": test_bucket},
                "object": {"key": test_key}
            }
        }]
    }
    
    response = lambda_client.invoke(
        FunctionName=TEST_FUNCTION_NAME,
        Payload=json.dumps(s3_event)
    )
    assert response["StatusCode"] == 200

def test_error_handling(s3_client, lambda_client, test_bucket):
    """Test error handling scenarios."""
    # Test invalid S3 operation
    with pytest.raises(ClientError) as exc_info:
        s3_client.get_object(Bucket=test_bucket, Key="non-existent-key")
    assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"
    
    # Test invalid Lambda configuration
    with pytest.raises(ClientError) as exc_info:
        lambda_client.create_function(
            FunctionName="invalid-function",
            Runtime="invalid-runtime",
            Role=TEST_ROLE_ARN,
            Handler="invalid.handler",
            Code={"ZipFile": b"invalid"}
        )
    assert exc_info.value.response["Error"]["Code"] == "InvalidParameterValueException" 