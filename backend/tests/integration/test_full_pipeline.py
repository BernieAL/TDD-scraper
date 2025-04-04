import os
import json
import boto3
import pytest
from moto import mock_s3, mock_dynamodb
from botocore.exceptions import ClientError
from datetime import datetime
import time

# Test configuration
TEST_BUCKET = "tdd-scraper-test"
TEST_TABLE = "scraper-results"
LOCALSTACK_ENDPOINT = "http://localhost:4567"

# Mock scraper configurations
MOCK_SCRAPERS = {
    "italist": {
        "name": "italist",
        "endpoint": "https://www.italist.com",
        "timeout": 30,
        "retries": 3
    },
    "mock_scraper1": {
        "name": "mock_scraper1",
        "endpoint": "https://www.mock1.com",
        "timeout": 30,
        "retries": 3
    },
    "mock_scraper2": {
        "name": "mock_scraper2",
        "endpoint": "https://www.mock2.com",
        "timeout": 30,
        "retries": 3
    }
}

@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials."""
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
def dynamodb_client():
    """Create a boto3 DynamoDB client with LocalStack endpoint."""
    return boto3.client(
        "dynamodb",
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
def test_table(dynamodb_client):
    """Create and return a test DynamoDB table."""
    try:
        dynamodb_client.create_table(
            TableName=TEST_TABLE,
            KeySchema=[
                {"AttributeName": "scraper_name", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "scraper_name", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"}
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    return TEST_TABLE

@pytest.fixture(autouse=True)
def cleanup_dynamodb(dynamodb_client, test_table):
    """Clean up DynamoDB table before and after each test."""
    # Clean up before test
    try:
        response = dynamodb_client.scan(TableName=test_table)
        for item in response["Items"]:
            dynamodb_client.delete_item(
                TableName=test_table,
                Key={
                    "scraper_name": item["scraper_name"],
                    "timestamp": item["timestamp"]
                }
            )
    except ClientError:
        pass
    
    yield
    
    # Clean up after test
    try:
        response = dynamodb_client.scan(TableName=test_table)
        for item in response["Items"]:
            dynamodb_client.delete_item(
                TableName=test_table,
                Key={
                    "scraper_name": item["scraper_name"],
                    "timestamp": item["timestamp"]
                }
            )
    except ClientError:
        pass

class MockScraper:
    """Base class for mock scrapers."""
    def __init__(self, config):
        self.config = config
        self.name = config["name"]
        self.endpoint = config["endpoint"]
        self.timeout = config["timeout"]
        self.retries = config["retries"]

    def scrape(self, query):
        """Mock scraping method to be implemented by specific scrapers."""
        raise NotImplementedError

class MockScraper1(MockScraper):
    """First mock scraper implementation."""
    def scrape(self, query):
        """Mock successful scraping."""
        return {
            "status": "success",
            "data": {
                "query": query,
                "results": [
                    {"id": "1", "name": "Product 1", "price": 100},
                    {"id": "2", "name": "Product 2", "price": 200}
                ]
            },
            "metadata": {
                "scraper": self.name,
                "timestamp": datetime.now().isoformat(),
                "duration": 1.5
            }
        }

class MockScraper2(MockScraper):
    """Second mock scraper implementation."""
    def scrape(self, query):
        """Mock failed scraping."""
        raise Exception("Mock scraper 2 failed intentionally")

def test_full_pipeline(s3_client, dynamodb_client, test_bucket, test_table):
    """Test the full scraping pipeline with multiple scrapers."""
    # Test data
    test_query = {
        "brand": "test_brand",
        "category": "test_category",
        "query_hash": "test_hash"
    }
    
    # Initialize scrapers
    scrapers = [
        MockScraper1(MOCK_SCRAPERS["mock_scraper1"]),
        MockScraper2(MOCK_SCRAPERS["mock_scraper2"])
    ]
    
    results = []
    failures = []
    
    # Run scrapers
    for scraper in scrapers:
        try:
            result = scraper.scrape(test_query)
            results.append(result)
            
            # Save to S3
            s3_key = f"results/{scraper.name}/{test_query['query_hash']}.json"
            s3_client.put_object(
                Bucket=test_bucket,
                Key=s3_key,
                Body=json.dumps(result)
            )
            
            # Save to DynamoDB
            dynamodb_client.put_item(
                TableName=test_table,
                Item={
                    "scraper_name": {"S": scraper.name},
                    "timestamp": {"S": datetime.now().isoformat()},
                    "query_hash": {"S": test_query["query_hash"]},
                    "status": {"S": "success"},
                    "data": {"S": json.dumps(result["data"])}
                }
            )
            
        except Exception as e:
            failures.append({
                "scraper": scraper.name,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            
            # Log failure to DynamoDB
            dynamodb_client.put_item(
                TableName=test_table,
                Item={
                    "scraper_name": {"S": scraper.name},
                    "timestamp": {"S": datetime.now().isoformat()},
                    "query_hash": {"S": test_query["query_hash"]},
                    "status": {"S": "failed"},
                    "error": {"S": str(e)}
                }
            )
    
    # Verify results
    assert len(results) == 1  # One successful scraper
    assert len(failures) == 1  # One failed scraper
    
    # Verify S3 data
    s3_response = s3_client.get_object(
        Bucket=test_bucket,
        Key=f"results/{scrapers[0].name}/{test_query['query_hash']}.json"
    )
    s3_data = json.loads(s3_response["Body"].read().decode())
    assert s3_data["status"] == "success"
    assert len(s3_data["data"]["results"]) == 2
    
    # Verify DynamoDB data
    response = dynamodb_client.scan(TableName=test_table)
    items = response["Items"]
    assert len(items) == 2  # One success, one failure
    
    success_items = [item for item in items if item["status"]["S"] == "success"]
    failure_items = [item for item in items if item["status"]["S"] == "failed"]
    
    assert len(success_items) == 1
    assert len(failure_items) == 1
    assert failure_items[0]["error"]["S"] == "Mock scraper 2 failed intentionally"

def test_pipeline_error_handling(s3_client, dynamodb_client, test_bucket, test_table):
    """Test error handling in the pipeline."""
    # Test data with invalid query
    invalid_query = {
        "brand": None,
        "category": None,
        "query_hash": None
    }
    
    scraper = MockScraper1(MOCK_SCRAPERS["mock_scraper1"])
    
    try:
        scraper.scrape(invalid_query)
    except Exception as e:
        # Verify error logging
        response = dynamodb_client.scan(TableName=test_table)
        items = response["Items"]
        error_items = [item for item in items if item["status"]["S"] == "failed"]
        assert len(error_items) > 0
        assert "error" in error_items[0]

def test_pipeline_retry_mechanism(s3_client, dynamodb_client, test_bucket, test_table):
    """Test the retry mechanism in the pipeline."""
    class RetryScraper(MockScraper):
        def __init__(self, config):
            super().__init__(config)
            self.attempts = 0
        
        def scrape(self, query):
            self.attempts += 1
            if self.attempts < self.retries:
                raise Exception("Temporary failure")
            return {
                "status": "success",
                "data": {"query": query, "results": []},
                "metadata": {
                    "scraper": self.name,
                    "timestamp": datetime.now().isoformat(),
                    "attempts": self.attempts
                }
            }
    
    scraper = RetryScraper(MOCK_SCRAPERS["mock_scraper1"])
    result = None
    last_error = None
    
    # Implement retry logic
    for attempt in range(scraper.retries):
        try:
            result = scraper.scrape({"query": "test"})
            break
        except Exception as e:
            last_error = e
            if attempt == scraper.retries - 1:
                raise
            time.sleep(1)  # Wait before retry
    
    assert result is not None
    assert result["status"] == "success"
    assert result["metadata"]["attempts"] == scraper.retries 