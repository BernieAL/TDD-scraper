import requests,os,csv
from datetime import datetime

import time
import random
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

from scrapers.italist_scraper import italist_driver



src_dir = os.path.dirname(os.path.abspath(__file__))
input_data_file_path = os.path.join(src_dir,'input_data','search_criteria.csv')




def run_scrapers(brand,query,local):

    """
        this is the driver script for all website scrapers
    """
    with open(input_data_file_path,'r',newline='',encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        
        header = next(csv_reader) #skip header row
        for row in csv_reader:
            print(row)
            if not row:
                print("skipping row ")

            
            if len(row) == 2:
                brand,query = row
                print(brand)
                print(query)
                # italist_driver(brand,query,local)

            elif len(row) == 3:
                brand,query,specific = row
                #italist_driver not set up for specific item yet
                # italist_driver(brand,query,specific,local)  
            


if __name__ == "__main__":
    
    brand = "prada"
    query = "bags"
    local = True
    run_scrapers(brand,query,local)