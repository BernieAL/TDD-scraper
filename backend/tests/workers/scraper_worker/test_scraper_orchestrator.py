import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from backend.workers.scraper_worker.scraper_orchestrator import ScraperOrchestrator
from backend.workers.scraper_worker.scrapers import ItalistScraper, RebagScraper





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