import os
import pytest
import boto3
import time
from workers.scraper_worker.scrapers.vestiaire_scraper import VestiaireScraper
from workers.scraper_worker.scrapers.rebag_scraper import RebagScraper

# Configure boto3 to use LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
LOCALSTACK_ENDPOINT = 'http://localhost:4566'

@pytest.fixture(scope='module')
def aws_resources():
    """Set up AWS resources in LocalStack."""
    # Create boto3 clients using LocalStack endpoint
    s3 = boto3.client('s3', endpoint_url=LOCALSTACK_ENDPOINT)
    cloudwatch = boto3.client('cloudwatch', endpoint_url=LOCALSTACK_ENDPOINT)
    dynamodb = boto3.client('dynamodb', endpoint_url=LOCALSTACK_ENDPOINT)
    
    # Create test bucket
    bucket_name = 'test-scraper-bucket'
    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Bucket creation error (might already exist): {e}")
    
    # Create DynamoDB table for status tracking
    table_name = 'scraper-status'
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'source', 'KeyType': 'HASH'},
                {'AttributeName': 'query_hash', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'source', 'AttributeType': 'S'},
                {'AttributeName': 'query_hash', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except Exception as e:
        print(f"Table creation error (might already exist): {e}")
    
    # Wait for table to be created
    time.sleep(2)
    
    return {
        's3': s3,
        'cloudwatch': cloudwatch,
        'dynamodb': dynamodb,
        'bucket_name': bucket_name,
        'table_name': table_name
    }

def test_vestiaire_scraper_with_monitoring(aws_resources):
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
        assert all(key in results[0] for key in ['brand', 'name', 'price', 'url', 'source'])
        
        # Verify CloudWatch metrics
        metrics = aws_resources['cloudwatch'].get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'duration',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'ScraperMetrics',
                            'MetricName': 'ScrapeDuration',
                            'Dimensions': [{'Name': 'Source', 'Value': 'VESTIAIRE'}]
                        },
                        'Period': 60,
                        'Stat': 'Sum'
                    }
                }
            ],
            StartTime=time.time() - 300,  # Last 5 minutes
            EndTime=time.time()
        )
        
        assert len(metrics['MetricDataResults']) > 0
        
    except Exception as e:
        pytest.fail(f"Scraper test failed: {str(e)}")

def test_rebag_scraper_with_monitoring(aws_resources):
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
        assert all(key in results[0] for key in ['brand', 'name', 'price', 'url', 'source'])
        
        # Verify CloudWatch metrics
        metrics = aws_resources['cloudwatch'].get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'duration',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'ScraperMetrics',
                            'MetricName': 'ScrapeDuration',
                            'Dimensions': [{'Name': 'Source', 'Value': 'REBAG'}]
                        },
                        'Period': 60,
                        'Stat': 'Sum'
                    }
                }
            ],
            StartTime=time.time() - 300,  # Last 5 minutes
            EndTime=time.time()
        )
        
        assert len(metrics['MetricDataResults']) > 0
        
    except Exception as e:
        pytest.fail(f"Scraper test failed: {str(e)}")

def test_error_handling_and_monitoring(aws_resources):
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
        metrics = aws_resources['cloudwatch'].get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'errors',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'ScraperMetrics',
                            'MetricName': 'Errors',
                            'Dimensions': [{'Name': 'Source', 'Value': 'VESTIAIRE'}]
                        },
                        'Period': 60,
                        'Stat': 'Sum'
                    }
                }
            ],
            StartTime=time.time() - 300,
            EndTime=time.time()
        )
        
        assert len(metrics['MetricDataResults']) > 0 