"""
Local development tests that can run without AWS services.
Tests:
1. Local scraper execution
2. Local data processing
3. Local storage operations
4. Local notifications
"""

import pytest
import json
import os
import tempfile
from datetime import datetime
from unittest.mock import patch, MagicMock
from backend.workers.scraper_worker.scraper_orchestrator import ScraperOrchestrator
from backend.workers.scraper_worker.scrapers.italist_scraper import ItalistScraper
from backend.workers.scraper_worker.scrapers.rebag_scraper import RebagScraper

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir

@pytest.fixture
def sample_scraping_config():
    """Sample scraping configuration"""
    return {
        "brand": "PRADA",
        "category": "BAGS",
        "min_price": 1000,
        "max_price": 2000
    }

@pytest.fixture
def mock_scraper_data():
    """Mock data returned by scrapers"""
    return [
        {
            "product_id": "1",
            "brand": "PRADA",
            "name": "Test Bag",
            "price": 1000.0,
            "currency": "USD",
            "url": "http://example.com/1",
            "source": "italist"
        },
        {
            "product_id": "2",
            "brand": "PRADA",
            "name": "Test Bag",
            "price": 1100.0,
            "currency": "USD",
            "url": "http://example.com/2",
            "source": "rebag"
        }
    ]

def test_local_scraper_execution(temp_dir, sample_scraping_config, mock_scraper_data):
    """Test local scraper execution"""
    # Mock scraper classes
    with patch('backend.workers.scraper_worker.scrapers.italist_scraper.ItalistScraper.scrape') as mock_italist, \
         patch('backend.workers.scraper_worker.scrapers.rebag_scraper.RebagScraper.scrape') as mock_rebag:
        
        # Configure mock scrapers
        mock_italist.return_value = [mock_scraper_data[0]]
        mock_rebag.return_value = [mock_scraper_data[1]]
        
        # Initialize orchestrator
        orchestrator = ScraperOrchestrator(
            bucket_name=None,  # Local mode
            query_hash="test123",
            raw_path=os.path.join(temp_dir, "raw"),
            filtered_path=os.path.join(temp_dir, "filtered")
        )
        
        # Run scraping
        results = orchestrator.run_scrapers(
            sample_scraping_config["brand"],
            sample_scraping_config["category"]
        )
        
        # Verify results
        assert len(results) == 2
        assert all(item in results for item in mock_scraper_data)
        
        # Verify files were created
        assert os.path.exists(os.path.join(temp_dir, "raw", "italist.json"))
        assert os.path.exists(os.path.join(temp_dir, "raw", "rebag.json"))
        assert os.path.exists(os.path.join(temp_dir, "filtered", "combined.json"))

def test_local_data_processing(temp_dir, mock_scraper_data):
    """Test local data processing"""
    from backend.workers.scraper_worker.data_processor import DataProcessor
    
    # Create test files
    raw_dir = os.path.join(temp_dir, "raw")
    filtered_dir = os.path.join(temp_dir, "filtered")
    os.makedirs(raw_dir)
    os.makedirs(filtered_dir)
    
    # Write test data
    with open(os.path.join(raw_dir, "italist.json"), "w") as f:
        json.dump([mock_scraper_data[0]], f)
    with open(os.path.join(raw_dir, "rebag.json"), "w") as f:
        json.dump([mock_scraper_data[1]], f)
    
    # Process data
    processor = DataProcessor(raw_dir, filtered_dir)
    processor.process_data()
    
    # Verify processed data
    with open(os.path.join(filtered_dir, "combined.json"), "r") as f:
        processed_data = json.load(f)
    
    assert len(processed_data) == 2
    assert all(item in processed_data for item in mock_scraper_data)

def test_local_storage_operations(temp_dir, mock_scraper_data):
    """Test local storage operations"""
    from backend.workers.scraper_worker.storage import LocalStorage
    
    # Initialize storage
    storage = LocalStorage(temp_dir)
    
    # Test saving data
    storage.save_data("test.json", mock_scraper_data)
    assert os.path.exists(os.path.join(temp_dir, "test.json"))
    
    # Test loading data
    loaded_data = storage.load_data("test.json")
    assert loaded_data == mock_scraper_data

def test_local_notifications(temp_dir, sample_scraping_config):
    """Test local notifications"""
    from backend.workers.scraper_worker.notifications import LocalNotifier
    
    # Initialize notifier
    notifier = LocalNotifier(temp_dir)
    
    # Test success notification
    success_message = "Scraping completed successfully"
    notifier.send_success(sample_scraping_config, success_message)
    
    # Verify notification file
    notification_file = os.path.join(temp_dir, "notifications.json")
    assert os.path.exists(notification_file)
    
    with open(notification_file, "r") as f:
        notifications = json.load(f)
    
    assert len(notifications) == 1
    assert notifications[0]["status"] == "success"
    assert notifications[0]["message"] == success_message
    
    # Test error notification
    error_message = "Scraping failed"
    notifier.send_error(sample_scraping_config, error_message)
    
    with open(notification_file, "r") as f:
        notifications = json.load(f)
    
    assert len(notifications) == 2
    assert notifications[1]["status"] == "error"
    assert notifications[1]["message"] == error_message 