import json
import pytest
from datetime import datetime
from typing import Dict, Any
import boto3
from moto import mock_s3, mock_lambda
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch
from backend.workers.scraper_worker.scraper_orchestrator import ScraperOrchestrator
from backend.workers.scraper_worker.scrapers.italist_scraper import ItalistScraper
from backend.workers.scraper_worker.scrapers.rebag_scraper import RebagScraper





"""
Key points about this test file:
    Tests the orchestrator's responsibilities, not individual scrapers
    Uses mocks for S3 and scrapers
    Tests success and failure cases
    Tests exception handling
    Uses parametrize for multiple configurations
The main test categories:
    Initialization tests
    Success path tests
    Failure handling tests
    Invalid input tests
    Exception handling tests


"""

class TestScraperOrchestrator:
    """Test suite for ScraperOrchestrator class"""

    @pytest.fixture
    def mock_s3_client(self):
        """Mock AWS S3 client"""
        return Mock()

    @pytest.fixture
    def orchestrator(self, mock_s3_client):
        """Create ScraperOrchestrator instance with mocked S3"""
        return ScraperOrchestrator(s3_client=mock_s3_client)

    @pytest.fixture
    def mock_scraper(self):
        """Create a mock scraper"""
        scraper = Mock()
        # Mock the run method to return a fake file path
        scraper.run.return_value = Path("/temp/test/RAW_TEST_BRAND_DATE_CATEGORY.csv")
        return scraper

    def test_initialize_scrapers(self, orchestrator):
        """Test that scrapers are correctly initialized"""
        orchestrator.initialize_scrapers()
        
        assert 'ITALIST' in orchestrator.scrapers
        assert 'REBAG' in orchestrator.scrapers
        assert isinstance(orchestrator.scrapers['ITALIST'], ItalistScraper)
        assert isinstance(orchestrator.scrapers['REBAG'], RebagScraper)

    def test_run_scraper_success(self, orchestrator, mock_scraper):
        """Test successful scraper execution"""
        # Setup
        orchestrator.scrapers['ITALIST'] = mock_scraper
        
        # Execute
        result = orchestrator.run_scraper(
            scraper_name="ITALIST",
            brand="PRADA",
            category="BAGS",
            output_dir="raw",
            query_hash="test123",
            local=True
        )

        # Verify
        assert result is not None
        assert isinstance(result, (str, Path))
        mock_scraper.run.assert_called_once()

    def test_run_scraper_failure(self, orchestrator, mock_scraper):
        """Test scraper failure handling"""
        # Setup scraper to return None (failure case)
        mock_scraper.run.return_value = None
        orchestrator.scrapers['ITALIST'] = mock_scraper
        
        # Execute
        result = orchestrator.run_scraper(
            scraper_name="ITALIST",
            brand="PRADA",
            category="BAGS",
            output_dir="raw",
            query_hash="test123",
            local=True
        )

        # Verify
        assert result is None
        assert orchestrator.record_failure.called

    def test_run_scraper_invalid_scraper(self, orchestrator):
        """Test handling of invalid scraper name"""
        result = orchestrator.run_scraper(
            scraper_name="INVALID_SCRAPER",
            brand="PRADA",
            category="BAGS",
            output_dir="raw",
            query_hash="test123",
            local=True
        )

        assert result is None
        assert orchestrator.record_failure.called

    def test_run_scraper_exception(self, orchestrator, mock_scraper):
        """Test handling of scraper exceptions"""
        # Setup scraper to raise an exception
        mock_scraper.run.side_effect = Exception("Scraper error")
        orchestrator.scrapers['ITALIST'] = mock_scraper

        result = orchestrator.run_scraper(
            scraper_name="ITALIST",
            brand="PRADA",
            category="BAGS",
            output_dir="raw",
            query_hash="test123",
            local=True
        )

        assert result is None
        assert orchestrator.record_failure.called

    @pytest.mark.parametrize("scraper_name,brand,category", [
        ("ITALIST", "PRADA", "BAGS"),
        ("REBAG", "GUCCI", "SHOES"),
    ])
    def test_multiple_scrapers(self, orchestrator, mock_scraper, scraper_name, brand, category):
        """Test multiple scraper configurations"""
        orchestrator.scrapers[scraper_name] = mock_scraper

        result = orchestrator.run_scraper(
            scraper_name=scraper_name,
            brand=brand,
            category=category,
            output_dir="raw",
            query_hash="test123",
            local=True
        )

        assert result is not None
        mock_scraper.run.assert_called_once()

@pytest.fixture
def sample_search_data():
    """Sample search data for testing."""
    return {
        "query": "test query",
        "email": "test@example.com",
        "is_specific_search": False,
        "query_hash": "test_hash"
    }

def test_scraper_orchestrator(sample_search_data):
    """Test the scraper orchestrator."""
    with mock_s3(), mock_lambda():
        # Create S3 bucket
        s3 = boto3.client('s3')
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        
        # Create Lambda function
        lambda_client = boto3.client('lambda')
        lambda_client.create_function(
            FunctionName="test-scraper",
            Runtime="python3.10",
            Role="arn:aws:iam::123456789012:role/test-role",
            Handler="scraper_worker_lambda.lambda_handler",
            Code={"ZipFile": b""}
        )
        
        # Initialize orchestrator
        orchestrator = ScraperOrchestrator(
            bucket_name=bucket_name,
            lambda_function_name="test-scraper"
        )
        
        # Test orchestration
        response = orchestrator.orchestrate_scraping(sample_search_data)
        
        assert response["statusCode"] == 200
        response_body = json.loads(response["body"])
        assert "message" in response_body
        assert "query_hash" in response_body
        assert response_body["query_hash"] == sample_search_data["query_hash"]

def test_scraper_orchestrator_error_handling(sample_search_data):
    """Test error handling in the scraper orchestrator."""
    with mock_s3(), mock_lambda():
        # Initialize orchestrator with invalid bucket
        orchestrator = ScraperOrchestrator(
            bucket_name="non-existent-bucket",
            lambda_function_name="test-scraper"
        )
        
        # Test orchestration (should fail)
        response = orchestrator.orchestrate_scraping(sample_search_data)
        
        assert response["statusCode"] == 500
        response_body = json.loads(response["body"])
        assert "error" in response_body