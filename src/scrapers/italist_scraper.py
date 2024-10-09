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


from src.scrapers.base_scraper import BaseScraper
from utils.ScraperUtils import ScraperUtils



class ItalistScraper(BaseScraper):


    def __init__(self, brand, query,output_dir,local):
        super().__init__(brand, query)
        self.local = local
        self.output_dir = output_dir
        self.source = "italist"
        

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
            return product_id, brand, product_name, price, listing_url
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
            
            scraped_file = self.save_to_file(data,self.source,self.output_dir)
            return scraped_file
        finally:
            driver.quit()

    

# Running the scraper
if __name__ == "__main__":

    brand = 'prada'
    query = 'bags'

    scraped_data_root_dir = output_dir = os.path.join(os.path.dirname(__file__), '..', 'file_output')
    scraper_util = ScraperUtils(scraped_data_root_dir)
    output_dir = scraper_util.make_scraped_sub_dir(brand,query)

    scraper = ItalistScraper(brand, query, output_dir, local=True)
    scraper.run()

