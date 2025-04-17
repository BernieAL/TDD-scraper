import pytest
import os
import boto3
import json
from datetime import datetime
from unittest.mock import Mock, patch
from moto import mock_s3, mock_dynamodb

from backend.workers.scraper_worker.scrapers.rebag_scraper import RebagScraper
from backend.workers.scraper_worker.scrapers.vestiaire_scraper import VestiaireScraper
from backend.workers.scraper_worker.utils.scraper_utils import ScraperUtils

# Test configuration
TEST_BRAND = "prada"
TEST_QUERY = "bags"
TEST_OUTPUT_DIR = "test_output"
TEST_QUERY_HASH = "test_hash_123"
TEST_BUCKET = "test-bucket"
TEST_TABLE = "scraper_status"

# Set environment variables for tests
os.environ['DYNAMODB_TABLE'] = TEST_TABLE
os.environ['S3_BUCKET'] = TEST_BUCKET

@pytest.fixture
def s3_client():
    """Create a mock S3 client."""
    with mock_s3():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=TEST_BUCKET)
        yield s3

@pytest.fixture
def dynamodb_client():
    """Create a mock DynamoDB client."""
    with mock_dynamodb():
        dynamodb = boto3.client('dynamodb')
        
        # Create test table with composite key
        dynamodb.create_table(
            TableName=TEST_TABLE,
            KeySchema=[
                {'AttributeName': 'query_hash', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'query_hash', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'N'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        yield dynamodb

@pytest.fixture
def mock_driver():
    """Create a mock Selenium WebDriver."""
    driver = Mock()
    driver.quit = Mock()
    return driver

@pytest.fixture
def mock_successful_listing():
    """Create a mock listing that returns valid data."""
    listing = Mock()
    link = Mock()
    link.get_attribute.return_value = "http://test.com/product/123"
    
    listing.find_element.side_effect = lambda by, value: {
        "a.product-card__link": link,
        "div.product-card__brand": Mock(text="Test Brand"),
        "div.product-card__name": Mock(text="Test Product"),
        "div.product-card__price": Mock(text="$100")
    }[value]
    
    return listing

@pytest.fixture
def mock_failing_listing():
    """Create a mock listing that raises an exception."""
    listing = Mock()
    listing.find_element.side_effect = Exception("Test error")
    return listing

def test_multiple_scrapers_sequence(s3_client, dynamodb_client, mock_driver, mock_successful_listing, mock_failing_listing):
    """Test running multiple scrapers in sequence with success and failure scenarios."""

    # Create scrapers
    rebag_scraper = RebagScraper(TEST_BRAND, TEST_QUERY, TEST_OUTPUT_DIR, TEST_QUERY_HASH)
    vestiaire_scraper = VestiaireScraper(TEST_BRAND, TEST_QUERY, TEST_OUTPUT_DIR, TEST_QUERY_HASH)

    # Mock the driver creation for both scrapers
    with patch('backend.workers.scraper_worker.scrapers.rebag_scraper.RebagScraper.get_driver') as mock_rebag_driver, \
         patch('backend.workers.scraper_worker.scrapers.vestiaire_scraper.VestiaireScraper.get_driver') as mock_vestiaire_driver, \
         patch('boto3.client') as mock_boto3_client:

        mock_rebag_driver.return_value = mock_driver
        mock_vestiaire_driver.return_value = mock_driver
        mock_boto3_client.side_effect = lambda service: s3_client if service == 's3' else dynamodb_client

        # Set up mock listings for Rebag (success scenario)
        mock_driver.find_elements.return_value = [mock_successful_listing]

        # Run Rebag scraper
        rebag_result = rebag_scraper.run()

        # Verify S3 upload for Rebag
        objects = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
        assert objects['KeyCount'] == 1

        # Verify DynamoDB entry for Rebag
        response = dynamodb_client.query(
            TableName=TEST_TABLE,
            KeyConditionExpression='query_hash = :hash',
            ExpressionAttributeValues={':hash': {'S': TEST_QUERY_HASH}}
        )
        assert len(response['Items']) == 1
        assert response['Items'][0]['source']['S'] == 'REBAG'

        # Set up mock listings for Vestiaire (failure scenario)
        mock_driver.find_elements.return_value = [mock_failing_listing]

        # Run Vestiaire scraper (should handle failure gracefully)
        try:
            vestiaire_result = vestiaire_scraper.run()
        except Exception as e:
            # Verify the error is logged in DynamoDB
            response = dynamodb_client.query(
                TableName=TEST_TABLE,
                KeyConditionExpression='query_hash = :hash',
                ExpressionAttributeValues={':hash': {'S': TEST_QUERY_HASH}}
            )
            assert len(response['Items']) == 2  # Both success and failure entries
            failed_item = [item for item in response['Items'] if item['status']['S'] == 'FAILED'][0]
            assert failed_item['source']['S'] == 'VESTIAIRE'
            assert 'error' in failed_item

def test_retry_after_failure(s3_client, dynamodb_client, mock_driver, mock_successful_listing, mock_failing_listing):
    """Test that a failed scraper can be retried successfully."""

    vestiaire_scraper = VestiaireScraper(TEST_BRAND, TEST_QUERY, TEST_OUTPUT_DIR, TEST_QUERY_HASH)

    with patch('backend.workers.scraper_worker.scrapers.vestiaire_scraper.VestiaireScraper.get_driver') as mock_get_driver, \
         patch('boto3.client') as mock_boto3_client:
        mock_get_driver.return_value = mock_driver
        mock_boto3_client.side_effect = lambda service: s3_client if service == 's3' else dynamodb_client

        # First attempt - failure
        mock_driver.find_elements.return_value = [mock_failing_listing]

        try:
            vestiaire_scraper.run()
        except Exception:
            pass

        # Verify failure is logged
        response = dynamodb_client.query(
            TableName=TEST_TABLE,
            KeyConditionExpression='query_hash = :hash',
            ExpressionAttributeValues={':hash': {'S': TEST_QUERY_HASH}}
        )
        assert len(response['Items']) == 1
        assert response['Items'][0]['status']['S'] == 'FAILED'

        # Second attempt - success
        mock_driver.find_elements.return_value = [mock_successful_listing]
        vestiaire_scraper.run()

        # Verify success is logged
        response = dynamodb_client.query(
            TableName=TEST_TABLE,
            KeyConditionExpression='query_hash = :hash',
            ExpressionAttributeValues={':hash': {'S': TEST_QUERY_HASH}}
        )
        assert len(response['Items']) == 2
        success_item = [item for item in response['Items'] if item['status']['S'] == 'SUCCESS'][0]
        assert success_item['source']['S'] == 'VESTIAIRE'

def test_concurrent_updates(s3_client, dynamodb_client, mock_driver, mock_successful_listing):
    """Test that multiple scrapers can update S3 and DynamoDB concurrently."""

    # Create scrapers with different query hashes
    rebag_scraper = RebagScraper(TEST_BRAND, TEST_QUERY, TEST_OUTPUT_DIR, "hash1")
    vestiaire_scraper = VestiaireScraper(TEST_BRAND, TEST_QUERY, TEST_OUTPUT_DIR, "hash2")

    with patch('backend.workers.scraper_worker.scrapers.rebag_scraper.RebagScraper.get_driver') as mock_rebag_driver, \
         patch('backend.workers.scraper_worker.scrapers.vestiaire_scraper.VestiaireScraper.get_driver') as mock_vestiaire_driver, \
         patch('boto3.client') as mock_boto3_client:

        mock_rebag_driver.return_value = mock_driver
        mock_vestiaire_driver.return_value = mock_driver
        mock_boto3_client.side_effect = lambda service: s3_client if service == 's3' else dynamodb_client

        # Set up mock listings for both scrapers
        mock_driver.find_elements.return_value = [mock_successful_listing]

        # Run both scrapers
        rebag_result = rebag_scraper.run()
        vestiaire_result = vestiaire_scraper.run()

        # Verify S3 uploads
        objects = s3_client.list_objects_v2(Bucket=TEST_BUCKET)
        assert objects['KeyCount'] == 2

        # Verify DynamoDB entries
        response = dynamodb_client.scan(TableName=TEST_TABLE)
        assert len(response['Items']) == 2

        # Verify each scraper's entry
        sources = [item['source']['S'] for item in response['Items']]
        assert 'REBAG' in sources
        assert 'VESTIAIRE' in sources 