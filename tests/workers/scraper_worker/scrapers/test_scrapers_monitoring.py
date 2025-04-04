import os
import pytest
import boto3
import time
from moto import mock_s3, mock_cloudwatch, mock_dynamodb
from workers.scraper_worker.scrapers.vestiaire_scraper import VestiaireScraper
from workers.scraper_worker.scrapers.rebag_scraper import RebagScraper

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        # Create test bucket
        s3.create_bucket(Bucket="test-scraper-bucket")
        yield s3

@pytest.fixture(scope="function")
def cloudwatch(aws_credentials):
    with mock_cloudwatch():
        yield boto3.client("cloudwatch", region_name="us-east-1")

@pytest.fixture(scope="function")
def dynamodb(aws_credentials):
    with mock_dynamodb():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        # Create test table
        dynamodb.create_table(
            TableName="scraper-status",
            KeySchema=[
                {"AttributeName": "source", "KeyType": "HASH"},
                {"AttributeName": "query_hash", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "source", "AttributeType": "S"},
                {"AttributeName": "query_hash", "AttributeType": "S"}
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        )
        yield dynamodb

def test_vestiaire_scraper_with_monitoring(s3, cloudwatch, dynamodb):
    """Test VestiaireScraper with monitoring enabled."""
    # Initialize scraper with test parameters
    scraper = VestiaireScraper(
        brand="gucci",
        query="bag",
        output_dir="test_output",
        query_hash="test123",
        local=True  # Use local test HTML
    )
    
    # Run the scraper
    try:
        results = scraper.run()
        
        # Verify results
        assert results is not None
        assert len(results) > 0
        assert all(key in results[0] for key in ["brand", "name", "price", "url", "source"])
        
        # Verify CloudWatch metrics
        metrics = cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "duration",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "ScraperMetrics",
                            "MetricName": "ScrapeDuration",
                            "Dimensions": [{"Name": "Source", "Value": "VESTIAIRE"}]
                        },
                        "Period": 60,
                        "Stat": "Sum"
                    }
                }
            ],
            StartTime=time.time() - 300,  # Last 5 minutes
            EndTime=time.time()
        )
        
        assert len(metrics["MetricDataResults"]) > 0
        
    except Exception as e:
        pytest.fail(f"Scraper test failed: {str(e)}")

def test_rebag_scraper_with_monitoring(s3, cloudwatch, dynamodb):
    """Test RebagScraper with monitoring enabled."""
    # Initialize scraper with test parameters
    scraper = RebagScraper(
        brand="gucci",
        query="bag",
        output_dir="test_output",
        query_hash="test456",
        local=True  # Use local test HTML
    )
    
    # Run the scraper
    try:
        results = scraper.run()
        
        # Verify results
        assert results is not None
        assert len(results) > 0
        assert all(key in results[0] for key in ["brand", "name", "price", "url", "source"])
        
        # Verify CloudWatch metrics
        metrics = cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "duration",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "ScraperMetrics",
                            "MetricName": "ScrapeDuration",
                            "Dimensions": [{"Name": "Source", "Value": "REBAG"}]
                        },
                        "Period": 60,
                        "Stat": "Sum"
                    }
                }
            ],
            StartTime=time.time() - 300,  # Last 5 minutes
            EndTime=time.time()
        )
        
        assert len(metrics["MetricDataResults"]) > 0
        
    except Exception as e:
        pytest.fail(f"Scraper test failed: {str(e)}")

def test_error_handling_and_monitoring(s3, cloudwatch, dynamodb):
    """Test error handling and monitoring for invalid scenarios."""
    # Initialize scraper with invalid parameters to trigger errors
    scraper = VestiaireScraper(
        brand="",  # Invalid brand
        query="",  # Invalid query
        output_dir="test_output",
        query_hash="test789",
        local=True
    )
    
    # Run the scraper and expect it to handle errors
    try:
        scraper.run()
        pytest.fail("Expected an error but none was raised")
    except Exception:
        # Verify error metrics in CloudWatch
        metrics = cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "errors",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "ScraperMetrics",
                            "MetricName": "Errors",
                            "Dimensions": [{"Name": "Source", "Value": "VESTIAIRE"}]
                        },
                        "Period": 60,
                        "Stat": "Sum"
                    }
                }
            ],
            StartTime=time.time() - 300,
            EndTime=time.time()
        )
        
        assert len(metrics["MetricDataResults"]) > 0 