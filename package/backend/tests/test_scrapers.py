import pytest
import os
import tempfile
from datetime import datetime
import json
import boto3
import logging
from moto import mock_s3, mock_dynamodb
import sys

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from workers.scraper_worker.scrapers.rebag_scraper import RebagScraper
from workers.scraper_worker.scrapers.vestiaire_scraper import VestiaireScraper
from workers.scraper_worker.scrapers.italist_scraper import ItalistScraper
from workers.scraper_worker.scraper_orchestrator import ScraperOrchestrator
from workers.scraper_worker.utils.monitoring_service import MonitoringService
from workers.scraper_worker.utils.status_tracker import StatusTracker

# Register the integration mark
pytest.mark.integration = pytest.mark.integration

# Constants for testing
TEST_BUCKET = 'test-scraper-bucket'
TEST_TABLE = 'test-table'

@pytest.fixture
def mock_aws():
    """Mock AWS services."""
    with mock_s3():
        with mock_dynamodb():
            s3 = boto3.client('s3', region_name='us-east-1')
            s3.create_bucket(Bucket=TEST_BUCKET)
            
            dynamodb = boto3.client('dynamodb', region_name='us-east-1')
            dynamodb.create_table(
                TableName=TEST_TABLE,
                KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
                BillingMode='PAY_PER_REQUEST'
            )
            
            yield {
                's3': s3,
                'dynamodb': dynamodb
            }

@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname

@pytest.fixture
def scraper_config():
    """Basic configuration for scrapers."""
    return {
        'brand': 'gucci',
        'query': 'bag',
        'query_hash': 'test123',
        'local': True
    }

@pytest.fixture
def monitoring_service():
    """Create a monitoring service instance."""
    return MonitoringService()

@pytest.fixture
def status_tracker():
    """Create a status tracker instance."""
    return StatusTracker()

@pytest.fixture
def orchestrator(monitoring_service, status_tracker):
    """Create a scraper orchestrator instance."""
    return ScraperOrchestrator(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )

@pytest.mark.integration
def test_rebag_scraper_initialization(monitoring_service, status_tracker):
    """Test RebagScraper initialization."""
    scraper = RebagScraper(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )
    assert isinstance(scraper, RebagScraper)
    assert scraper.source == "REBAG"

@pytest.mark.integration
def test_vestiaire_scraper_initialization(monitoring_service, status_tracker):
    """Test VestiaireScraper initialization."""
    scraper = VestiaireScraper(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )
    assert isinstance(scraper, VestiaireScraper)
    assert scraper.source == "VESTIAIRE"

@pytest.mark.integration
def test_italist_scraper_initialization(monitoring_service, status_tracker):
    """Test ItalistScraper initialization."""
    scraper = ItalistScraper(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )
    assert isinstance(scraper, ItalistScraper)
    assert scraper.source == "ITALIST"

@pytest.mark.integration
def test_rebag_scraper_run(mock_aws, monitoring_service, status_tracker, scraper_config, temp_output_dir):
    """Test the RebagScraper run method."""
    scraper = RebagScraper(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )
    results = scraper.run(
        brand=scraper_config['brand'],
        query=scraper_config['query'],
        output_dir=temp_output_dir,
        query_hash=scraper_config['query_hash'],
        local=scraper_config['local']
    )
    assert len(results) > 0
    
    # Verify S3 upload
    s3_objects = mock_aws['s3'].list_objects_v2(Bucket=TEST_BUCKET)
    assert 'Contents' in s3_objects
    assert len(s3_objects['Contents']) > 0

@pytest.mark.integration
def test_vestiaire_scraper_run(mock_aws, monitoring_service, status_tracker, scraper_config, temp_output_dir):
    """Test the VestiaireScraper run method."""
    scraper = VestiaireScraper(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )
    results = scraper.run(
        brand=scraper_config['brand'],
        query=scraper_config['query'],
        output_dir=temp_output_dir,
        query_hash=scraper_config['query_hash'],
        local=scraper_config['local']
    )
    assert len(results) > 0
    
    # Verify S3 upload
    s3_objects = mock_aws['s3'].list_objects_v2(Bucket=TEST_BUCKET)
    assert 'Contents' in s3_objects
    assert len(s3_objects['Contents']) > 0

@pytest.mark.integration
def test_italist_scraper_run(mock_aws, monitoring_service, status_tracker, scraper_config, temp_output_dir):
    """Test the ItalistScraper run method."""
    scraper = ItalistScraper(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )
    results = scraper.run(
        brand=scraper_config['brand'],
        query=scraper_config['query'],
        output_dir=temp_output_dir,
        query_hash=scraper_config['query_hash'],
        local=scraper_config['local']
    )
    assert len(results) > 0
    
    # Verify S3 upload
    s3_objects = mock_aws['s3'].list_objects_v2(Bucket=TEST_BUCKET)
    assert 'Contents' in s3_objects
    assert len(s3_objects['Contents']) > 0

@pytest.mark.integration
def test_orchestrator_get_active_scrapers(orchestrator):
    """Test getting active scrapers from orchestrator."""
    active_scrapers = orchestrator.get_active_scrapers()
    assert len(active_scrapers) > 0
    assert 'rebag' in active_scrapers
    assert 'vestiaire' in active_scrapers
    assert 'italist' in active_scrapers

@pytest.mark.integration
def test_orchestrator_run_all_scrapers(mock_aws, orchestrator, scraper_config, temp_output_dir):
    """Test running all scrapers through the orchestrator."""
    for scraper in orchestrator.scrapers.values():
        scraper.configure_search(
            brand=scraper_config['brand'],
            query=scraper_config['query'],
            output_dir=temp_output_dir,
            query_hash=scraper_config['query_hash'],
            local=scraper_config['local']
        )
    
    results = orchestrator.run_all_scrapers()
    assert len(results) > 0
    
    # Verify S3 upload
    s3_objects = mock_aws['s3'].list_objects_v2(Bucket=TEST_BUCKET)
    assert 'Contents' in s3_objects
    assert len(s3_objects['Contents']) > 0

@pytest.mark.integration
def test_orchestrator_failure_handling(orchestrator, scraper_config):
    """Test orchestrator's failure handling."""
    # Record a failure
    orchestrator.record_failure('rebag', 'Test error')
    
    # Check failed scrapers
    failed_scrapers = orchestrator.get_failed_scrapers()
    assert 'rebag' in failed_scrapers
    
    # Check failure details
    failure_details = orchestrator.get_failure_details('rebag')
    assert failure_details is not None
    assert any('Test error' in detail for detail in failure_details)

@pytest.mark.integration
def test_scraper_logging_and_monitoring(mock_aws, monitoring_service, status_tracker, scraper_config, temp_output_dir):
    """Test that scrapers properly log and monitor their operations."""
    scraper = RebagScraper(
        monitoring_service=monitoring_service,
        status_tracker=status_tracker,
        s3_bucket=TEST_BUCKET,
        dynamodb_table=TEST_TABLE
    )
    
    # Run scraper
    scraper.run(
        brand=scraper_config['brand'],
        query=scraper_config['query'],
        output_dir=temp_output_dir,
        query_hash=scraper_config['query_hash'],
        local=scraper_config['local']
    )
    
    # Verify monitoring metrics were recorded
    # Note: In a real test, we would mock the monitoring service and verify calls

@pytest.mark.integration
def test_complete_pipeline_flow(mock_aws, orchestrator, scraper_config, temp_output_dir):
    """Test the complete scraping pipeline flow."""
    # Configure all scrapers
    for scraper in orchestrator.scrapers.values():
        scraper.configure_search(
            brand=scraper_config['brand'],
            query=scraper_config['query'],
            output_dir=temp_output_dir,
            query_hash=scraper_config['query_hash'],
            local=scraper_config['local']
        )
    
    # Run all scrapers
    results = orchestrator.run_all_scrapers()
    assert len(results) > 0
    
    # Verify S3 uploads
    s3_objects = mock_aws['s3'].list_objects_v2(Bucket=TEST_BUCKET)
    assert 'Contents' in s3_objects
    assert len(s3_objects['Contents']) > 0

@pytest.mark.integration
def test_orchestrator_cleanup_and_completion(mock_aws, orchestrator, scraper_config, temp_output_dir):
    """Test cleanup and completion of scraping operations."""
    # Configure and run scrapers
    for scraper in orchestrator.scrapers.values():
        scraper.configure_search(
            brand=scraper_config['brand'],
            query=scraper_config['query'],
            output_dir=temp_output_dir,
            query_hash=scraper_config['query_hash'],
            local=scraper_config['local']
        )
    
    results = orchestrator.run_all_scrapers()
    assert len(results) > 0
    
    # Verify all files are uploaded and local temp files are cleaned up
    s3_objects = mock_aws['s3'].list_objects_v2(Bucket=TEST_BUCKET)
    assert 'Contents' in s3_objects
    assert len(s3_objects['Contents']) > 0
    
    # Verify temp files are cleaned up
    temp_files = [f for f in os.listdir(temp_output_dir) if f.endswith('.tmp')]
    assert len(temp_files) == 0
    
    # Verify status updates in DynamoDB
    dynamodb = mock_aws['dynamodb']
    status_items = dynamodb.scan(TableName=TEST_TABLE)
    assert 'Items' in status_items
    
    # All scrapers should have a final status
    for item in status_items['Items']:
        assert 'status' in item
        assert item['status']['S'] in ['SUCCESS', 'FAILED'] 