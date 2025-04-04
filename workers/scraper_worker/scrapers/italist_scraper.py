import os
import time
import logging
from selenium.webdriver.common.by import By
import boto3
from ..utils.scraper_utils import ScraperUtils
from ..utils.driver_utils import get_driver
from ..utils.error_handler import ErrorHandler
from ..utils.status_tracker import StatusTracker
from ..utils.monitoring_service import MonitoringService

class ItalistScraper:
    """Scraper for Italist website."""

    def __init__(self, monitoring_service: MonitoringService, status_tracker: StatusTracker, s3_bucket: str, dynamodb_table: str):
        """Initialize the scraper with required services and configuration."""
        self.monitoring = monitoring_service
        self.status_tracker = status_tracker
        self.s3_bucket = s3_bucket
        self.dynamodb_table = dynamodb_table
        self.source = "ITALIST"
        
        # Set up services
        self.logger = logging.getLogger(__name__)
        self.error_handler = ErrorHandler(max_retries=3, retry_delay=5)

    def configure_search(self, brand: str, query: str, output_dir: str, query_hash: str, local: bool = False):
        """Configure search parameters for the scraper."""
        self.brand = brand
        self.query = query
        self.output_dir = output_dir
        self.query_hash = query_hash
        self.local = local
        self.search_url = "https://www.italist.com/search"

    def get_driver(self):
        """Get a configured Selenium WebDriver."""
        return get_driver()

    def get_listings(self, driver):
        """Get all product listings from the page."""
        @self.error_handler.retry_on_failure
        def _get_listings():
            return driver.find_elements(By.CSS_SELECTOR, "div.product-card")
        return _get_listings()

    def extract_listing_data(self, listing):
        """Extract data from a single product listing."""
        try:
            product_link = listing.find_element(By.CSS_SELECTOR, "a.product-card__link")
            product_url = product_link.get_attribute("href")
            
            product_brand = listing.find_element(By.CSS_SELECTOR, "div.product-card__brand").text
            product_name = listing.find_element(By.CSS_SELECTOR, "div.product-card__name").text
            product_price = listing.find_element(By.CSS_SELECTOR, "div.product-card__price").text
            
            return {
                'brand': product_brand,
                'name': product_name,
                'price': product_price,
                'url': product_url,
                'source': self.source
            }
        except Exception as e:
            self.error_handler.handle_error(e, "extract_listing_data", self.source)
            self.monitoring.record_error(self.source, "ExtractListingError")
            return None

    def run(self, brand: str = None, query: str = None, output_dir: str = None, query_hash: str = None, local: bool = False):
        """Execute the scraping process."""
        if any(param is not None for param in [brand, query, output_dir, query_hash]):
            self.configure_search(brand, query, output_dir, query_hash, local)
            
        if not hasattr(self, 'brand') or not hasattr(self, 'query'):
            raise ValueError("Search parameters not configured. Call configure_search() first or provide parameters to run()")

        start_time = time.time()
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Initialize S3 client and utils
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb')
        utils = ScraperUtils(s3_client=s3, dynamodb_client=dynamodb, s3_bucket=self.s3_bucket, dynamodb_table=self.dynamodb_table)

        if self.local:
            # For local testing with saved HTML
            url = f"file:///{os.path.abspath('local_websites/italist_test.html')}"
        else:
            # Construct search URL
            url = f"{self.search_url}?q={self.brand}+{self.query}"

        driver = self.get_driver()
        try:
            # Navigate to the search page
            driver.get(url)
            time.sleep(2)  # Wait for page load

            # Get all product listings
            listings = self.get_listings(driver)
            data = []

            # Extract data from each listing
            for listing in listings:
                product_data = self.extract_listing_data(listing)
                if product_data:  # Only add if data was extracted successfully
                    data.append(product_data)

            # Validate data
            if not self.error_handler.validate_data(data, ['brand', 'name', 'price', 'url', 'source']):
                self.error_handler.handle_empty_data(self.source)
                self.status_tracker.log_status(self.source, self.query_hash, "FAILED", "Invalid or empty data extracted")
                self.monitoring.record_error(self.source, "EmptyDataError")
                raise Exception("Invalid or empty data extracted")

            # Save results locally
            temp_file, filename = utils.save_to_file(data, self.brand, self.query, self.source, self.output_dir, self.query_hash, 0)

            # Upload to S3
            s3_key = f"{self.output_dir}/{filename}"
            utils.upload_to_s3(temp_file, s3_key)

            # Log success
            self.status_tracker.log_status(self.source, self.query_hash, "SUCCESS")

            # Record metrics
            duration = time.time() - start_time
            self.monitoring.record_scrape_duration(self.source, duration)
            self.monitoring.record_items_scraped(self.source, len(data))

            return data

        except Exception as e:
            self.error_handler.handle_error(e, "run", self.source)
            self.status_tracker.log_status(self.source, self.query_hash, "FAILED", str(e))
            self.monitoring.record_error(self.source, "RunError")
            raise
        finally:
            driver.quit() 