import pytest
import os
import json
from datetime import datetime
from unittest.mock import Mock, patch
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from backend.workers.scraper_worker.scrapers.rebag_scraper import RebagScraper

# Test configuration
TEST_BRAND = "prada"
TEST_QUERY = "bags"
TEST_OUTPUT_DIR = "test_output"
TEST_QUERY_HASH = "test_hash_123"

@pytest.fixture
def scraper():
    """Create a RebagScraper instance for testing."""
    return RebagScraper(TEST_BRAND, TEST_QUERY, TEST_OUTPUT_DIR, TEST_QUERY_HASH)

@pytest.fixture
def mock_driver():
    """Create a mock Selenium WebDriver."""
    driver = Mock()
    return driver

@pytest.fixture
def mock_listing():
    """Create a mock product listing element."""
    listing = Mock(spec=WebElement)
    
    # Mock the find_element method to return elements with text
    def mock_find_element(by, value):
        element = Mock(spec=WebElement)
        if value == "a.product-link":
            element.get_attribute.return_value = "https://shop.rebag.com/products/prada-bag-12345"
        elif value == "span.product-brand":
            element.text = "Prada"
        elif value == "h3.product-title":
            element.text = "Prada Bag"
        elif value == "span.product-price":
            element.text = "$1,234.56"
        return element
    
    listing.find_element.side_effect = mock_find_element
    return listing

def test_initialization(scraper):
    """Test scraper initialization."""
    assert scraper.brand == TEST_BRAND
    assert scraper.query == TEST_QUERY
    assert scraper.output_dir == TEST_OUTPUT_DIR
    assert scraper.query_hash == TEST_QUERY_HASH
    assert scraper.source == "REBAG"
    assert scraper.base_url == "https://shop.rebag.com"

def test_get_listings_success(scraper, mock_driver):
    """Test successful retrieval of product listings."""
    # Mock the WebDriverWait and find_elements
    mock_driver.find_elements.return_value = [Mock(), Mock()]
    
    listings = scraper.get_listings(mock_driver)
    assert len(listings) == 2

def test_get_listings_failure(scraper, mock_driver):
    """Test handling of failed listing retrieval."""
    mock_driver.find_elements.side_effect = NoSuchElementException()
    
    listings = scraper.get_listings(mock_driver)
    assert len(listings) == 0

def test_extract_listing_data_success(scraper, mock_listing):
    """Test successful extraction of listing data."""
    product_id, brand, product_name, price, url, source = scraper.extract_listing_data(mock_listing)
    
    assert product_id == "REBAG-prada-bag-12345"
    assert brand == "Prada"
    assert product_name == "Prada Bag"
    assert price == 1234.56
    assert url == "https://shop.rebag.com/products/prada-bag-12345"
    assert source == "REBAG"

def test_extract_listing_data_failure(scraper):
    """Test handling of failed data extraction."""
    mock_listing = Mock(spec=WebElement)
    mock_listing.find_element.side_effect = NoSuchElementException()
    
    result = scraper.extract_listing_data(mock_listing)
    assert all(item is None for item in result)

def test_generate_id_from_url(scraper):
    """Test product ID generation from URL."""
    test_url = "https://shop.rebag.com/products/prada-bag-12345"
    product_id = scraper.generate_id_from_url(test_url)
    assert product_id == "REBAG-prada-bag-12345"

def test_get_numeric_only(scraper):
    """Test price string to numeric conversion."""
    # Test various price formats
    assert scraper.get_numeric_only("$1,234.56") == 1234.56
    assert scraper.get_numeric_only("1,234.56 USD") == 1234.56
    assert scraper.get_numeric_only("invalid") == 0.0

@patch('backend.workers.scraper_worker.scrapers.rebag_scraper.boto3.client')
@patch('backend.workers.scraper_worker.scrapers.rebag_scraper.ScraperUtils')
def test_run_success(mock_utils, mock_boto3, scraper, mock_driver):
    """Test successful execution of the scraping process."""
    # Mock the driver creation
    with patch('backend.workers.scraper_worker.scrapers.rebag_scraper.RebagScraper.get_driver') as mock_get_driver:
        mock_get_driver.return_value = mock_driver
        
        # Mock the listing retrieval
        mock_listing = Mock(spec=WebElement)
        mock_driver.find_elements.return_value = [mock_listing]
        
        # Mock the data extraction
        mock_listing.find_element.side_effect = lambda by, value: Mock(text="test", get_attribute=lambda x: "test")
        
        # Mock the utils
        mock_utils_instance = Mock()
        mock_utils.return_value = mock_utils_instance
        mock_utils_instance.save_to_file.return_value = ("temp_file.csv", "test.csv")
        
        # Run the scraper
        result = scraper.run()
        
        # Verify the results
        assert result == "temp_file.csv"
        mock_utils_instance.save_to_file.assert_called_once()
        mock_utils_instance.upload_to_s3.assert_called_once()
        mock_utils_instance.cleanup.assert_called_once()

@patch('backend.workers.scraper_worker.scrapers.rebag_scraper.boto3.client')
def test_run_failure(mock_boto3, scraper):
    """Test handling of errors during scraping process."""
    # Mock the driver creation to raise an exception
    with patch('backend.workers.scraper_worker.scrapers.rebag_scraper.RebagScraper.get_driver') as mock_get_driver:
        mock_get_driver.side_effect = Exception("Test error")
        
        # Run the scraper and expect an exception
        with pytest.raises(Exception) as exc_info:
            scraper.run()
        assert str(exc_info.value) == "Test error"

@patch('backend.workers.scraper_worker.scrapers.rebag_scraper.boto3.client')
@patch('backend.workers.scraper_worker.scrapers.rebag_scraper.ScraperUtils')
def test_local_mode(mock_utils, mock_boto3, scraper, mock_driver):
    """Test scraper in local mode."""
    # Set local mode
    scraper.local = True
    
    # Mock the driver creation
    with patch('backend.workers.scraper_worker.scrapers.rebag_scraper.RebagScraper.get_driver') as mock_get_driver:
        mock_get_driver.return_value = mock_driver
        
        # Mock the listing retrieval
        mock_listing = Mock(spec=WebElement)
        mock_driver.find_elements.return_value = [mock_listing]
        
        # Mock the data extraction
        mock_listing.find_element.side_effect = lambda by, value: Mock(text="test", get_attribute=lambda x: "test")
        
        # Mock the utils
        mock_utils_instance = Mock()
        mock_utils.return_value = mock_utils_instance
        mock_utils_instance.save_to_file.return_value = ("temp_file.csv", "test.csv")
        
        # Run the scraper
        result = scraper.run()
        
        # Verify the results
        assert result == "temp_file.csv"
        mock_utils_instance.save_to_file.assert_called_once()
        mock_utils_instance.upload_to_s3.assert_called_once()
        mock_utils_instance.cleanup.assert_called_once() 