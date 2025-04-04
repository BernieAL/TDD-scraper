import requests,os,csv,sys
from datetime import datetime
import pytest
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import seleniumwire.undetected_chromedriver as uc


from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementNotVisibleException, StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException,TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC

import time
import random
import boto3
import logging
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, List, Optional

#target purse
target = "locky bb"
brand = "prada"
italist_general_param_url = f"https://www.italist.com/us/brands/{brand}/110/women/"
# next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=1&categories%5B%5D=437&skip=60

italist_branded_bags_url = f"https://www.italist.com/us/brands/{brand}/110/women/?categories%5B%5D=76"
#next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=76&skip=60


# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


#for testing using locally saved copy of website
""" Reminder - this is a single saved web page that will be specific to specific bag or search
"""
local_saved_file_path = os.path.abspath('src/local_websites/Prada Bags for Women ALWAYS LIKE A SALE.html')
local_url = 'file:///' + local_saved_file_path.replace('\\','/')


# from src.scrapers.base_scraper import BaseScraper
from ..utils.scraper_utils import ScraperUtils


from scrapers.base_scraper import BaseScraper

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ItalistScraper(BaseScraper):


    def __init__(self, brand, query,output_dir,query_hash,local):
        super().__init__(brand, query)
        self.local = local
        self.output_dir = output_dir
        self.source = "ITALIST"
        self.query_hash = query_hash
        self.base_url = "https://www.italist.com"
        self.search_url = f"{self.base_url}/search"
        logger.info(f"Configuration:")
        logger.info(f"- Brand: {self.brand}")
        logger.info(f"- Query: {self.query}")
        logger.info(f"- Output Directory: {self.output_dir}")
        logger.info(f"- Query Hash: {self.query_hash}")
        logger.info(f"- Local Mode: {self.local}")
        

    def get_listings(self, driver):
        """Find all listings inside the product grid container."""
        try:
            return driver.find_elements(By.XPATH, "//div[contains(@class, 'product-grid-container')]//a")
        except NoSuchElementException:
            print("Could not find listings.")
            return []

    def extract_listing_data(self, listing):
        """Extracts the brand, product name, and price from a single listing."""
        try:
            listing_url = listing.get_attribute('href')
            product_id = self.generate_id_from_url(listing_url)
            brand = listing.find_element(By.CSS_SELECTOR, "div.brand").text or "No brand"
            product_name = listing.find_element(By.XPATH, ".//div[contains(@class, 'productName')]").text or "No product name"
            price = listing.find_element(By.XPATH, ".//span[contains(@class, 'price')]").text or "No price"
            price = self.get_numeric_only(price)
            source = self.source
            return product_id, brand, product_name, price, listing_url, source
        except NoSuchElementException:
            print("Error extracting data.")
            return None, None, None, None, None
        

    def generate_id_from_url(self, url):
        """Generate a unique ID based on the product URL."""
        tokens = url.split('/')
        return f"{tokens[-3]}-{tokens[-4]}"

    def get_numeric_only(self, price_str):
        """Strip characters and return only the numeric part."""
        return int(price_str.split("USD")[1])

    def _extract_listing_data(self, listing: BeautifulSoup) -> Dict:
        """Extract data from a single product listing"""
        try:
            logger.debug("Extracting data from product listing...")
            product_data = {
                'title': self._get_text(listing, 'h3.product-title'),
                'brand': self._get_text(listing, 'span.product-brand'),
                'price': self._get_text(listing, 'span.product-price'),
                'original_price': self._get_text(listing, 'span.product-original-price'),
                'discount': self._get_text(listing, 'span.product-discount'),
                'url': self._get_attribute(listing, 'a.product-link', 'href'),
                'image_url': self._get_attribute(listing, 'img.product-image', 'src')
            }
            logger.debug(f"Extracted product data: {product_data}")
            return product_data
        except Exception as e:
            logger.error(f"Error extracting listing data: {str(e)}", exc_info=True)
            return {}
            
    def _get_text(self, element: BeautifulSoup, selector: str) -> str:
        """Helper to safely extract text from an element"""
        try:
            result = element.select_one(selector)
            return result.text.strip() if result else ""
        except Exception as e:
            logger.warning(f"Error extracting text with selector {selector}: {str(e)}")
            return ""
            
    def _get_attribute(self, element: BeautifulSoup, selector: str, attr: str) -> str:
        """Helper to safely extract attribute from an element"""
        try:
            result = element.select_one(selector)
            return result[attr] if result and attr in result.attrs else ""
        except Exception as e:
            logger.warning(f"Error extracting attribute {attr} with selector {selector}: {str(e)}")
            return ""
            
    def _fetch_search_results(self, page: int = 1) -> Optional[BeautifulSoup]:
        """Fetch search results page"""
        try:
            logger.info(f"Fetching search results page {page}...")
            params = {
                'q': f"{self.brand} {self.query}",
                'page': page
            }
            logger.debug(f"Request parameters: {params}")
            
            response = requests.get(self.search_url, params=params)
            response.raise_for_status()
            
            logger.info(f"Successfully fetched page {page}")
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logger.error(f"Error fetching search results page {page}: {str(e)}", exc_info=True)
            return None
            
    def _find_product_listings(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        """Find all product listings on the page"""
        try:
            logger.debug("Finding product listings...")
            listings = soup.select('div.product-listing')
            logger.info(f"Found {len(listings)} product listings")
            return listings
        except Exception as e:
            logger.error(f"Error finding product listings: {str(e)}", exc_info=True)
            return []
            
    def _save_results(self, products: List[Dict]) -> str:
        """Save scraped products to CSV file"""
        try:
            logger.info("Saving results to CSV...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"RAW_ITALIST_{self.brand.upper()}_{self.query.upper()}_{timestamp}_{self.query_hash}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            df = pd.DataFrame(products)
            df.to_csv(filepath, index=False)
            
            logger.info(f"Results saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}", exc_info=True)
            raise
            
    def run(self):
        """Scrapes Italist website and writes results to a CSV."""
        

       
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        if self.local:
            url = local_url
        else:
            url = f"https://www.italist.com/us/brands/{self.brand}/110/women/?categories%5B%5D=76"

        driver = self.get_driver()
        try:
            driver.get(url)
            time.sleep(2)
            listings = self.get_listings(driver)
            
            data = []
            for listing in listings:
                data.append(self.extract_listing_data(listing))
            
            # Initialize with S3 client
            s3 = boto3.client('s3')
            utils = ScraperUtils(self.output_dir, s3_client=s3)
            
            # Scrape and save locally
            temp_file, filename = utils.save_to_file(data, self.brand, self.query, 
                                                       self.source, self.output_dir, 
                                                       self.query_hash, 0)

            # Upload to S3 if needed
            s3_key = f"{self.output_dir}/{filename}"
            utils.upload_to_s3(temp_file, s3_key)

            # Cleanup
            utils.cleanup()
            
            return temp_file
        
        finally:
            driver.quit()

    

# Running the scraper
if __name__ == "__main__":

    brand = 'prada'
    category = 'bags'
    query = f"{brand}_{category}"

    current_date = datetime.now().strftime('%Y-%d-%m')


    scraped_data_root_dir_raw = output_dir = os.path.join(os.path.dirname(__file__), '..', 'scrape_file_output','raw')
    
    filtered_data_root_dir = output_dir = os.path.join(os.path.dirname(__file__), '..', 'scrape_file_output','filtered')
    
    scraper_util = ScraperUtils(scraped_data_root_dir_raw,filtered_data_root_dir)

    query_hash = scraper_util.generate_hash(query,None,current_date)
    output_dir = scraper_util.make_scraped_sub_dir_raw(brand,query,query_hash)
    

    scraper = ItalistScraper(brand, query, output_dir,query_hash,local=True)
    scraper.run()

