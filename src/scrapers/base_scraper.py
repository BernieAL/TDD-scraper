from abc import ABC, abstractmethod
from datetime import datetime
import csv,os

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


"""

urls = [
    # 'https://www.italist.com/us/',
    'https://www.cettire.com/pages/search?q=locky%20bb&qTitle=locky%20bb&menu%5Bdepartment%5D=women',
    'https://shop.rebag.com/search?q=locky+bb',
    'https://us.vestiairecollective.com/search/?q=locky+bb#gender=Women%231',
    'https://www.fashionphile.com/shop?search=locky+bb',
    # 'https://www.therealreal.com/' #requires login
]
"""


class BaseScraper():

    def __init__(self,brand,query):
        self.brand = brand
        self.query = query
        
    
     

    @abstractmethod
    def run(self,output_dir):
        pass

    def get_driver(self):
        """Initialize Selenium ChromeDriver."""
        uc_chrome_options = uc.ChromeOptions()
        uc_chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        uc_chrome_options.add_argument('--ignore-ssl-errors=yes')
        uc_chrome_options.add_argument('--ignore-certificate-errors')
        uc_chrome_options.add_argument('--allow-running-insecure-content')

        driver = uc.Chrome(service=Service(ChromeDriverManager().install()), options=uc_chrome_options)
        return driver

    @abstractmethod
    def get_listings(self): 
        pass
    
    @abstractmethod
    def extract_listing_data(self):
        pass
    
    def save_to_file(self, data, source, output_dir):
        """Save the scraped data to a CSV file in the given directory."""
        current_date = datetime.now().strftime('%Y-%d-%m')
        output_file = os.path.join(output_dir, f"{source}_{self.brand}_{current_date}_{self.query}_scrape.csv")
        
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            file.write(f"Scraped: {current_date} \n")
            file.write(f"Query: {self.brand}-{self.query} \n")
            writer.writerow(['product_id','brand','product_namfe','curr_price','listing_url'])
            file.write('---------------------- \n')
            for row in data:
                writer.writerow(row)

        print(f"Data successfully saved to {output_file}")