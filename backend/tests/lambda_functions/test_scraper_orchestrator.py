"""




"""



from backend.workers.scraper_worker import ScraperOrchestrator
from backend.utils.project_paths import SCRAPER_WORKER_ROOT


import shutil
import pytest
from dataclasses import dataclass
import os

class TestScraperOrchestrator:
    def test_run_scraper_success(self, orchestrator, mock_scraper):
        """Test that run_scraper returns a valid file path and handles success"""
        # Setup mock scraper to return a file
        mock_scraper.run.return_value = "/temp/italist/some_file.csv"
        orchestrator.scrapers["ITALIST"] = mock_scraper

        result = orchestrator.run_scraper(
            scraper_name="ITALIST",
            brand="PRADA",
            category="BAGS",
            output_dir="raw",
            query_hash="test123",
            local=True
        )

        # High-level checks
        assert result is not None
        assert os.path.exists(result)
        assert result.endswith(".csv")

    def test_run_scraper_failure(self, orchestrator, mock_scraper):
        """Test that run_scraper handles failures correctly"""
        mock_scraper.run.return_value = None
        orchestrator.scrapers["ITALIST"] = mock_scraper

        result = orchestrator.run_scraper(
            scraper_name="ITALIST",
            brand="PRADA",
            category="BAGS",
            output_dir="raw",
            query_hash="test123",
            local=True
        )

        assert result is None
        # Verify failure was recorded
        assert orchestrator.record_failure.called




