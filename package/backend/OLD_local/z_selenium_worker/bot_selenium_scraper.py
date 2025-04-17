"""

Purpose of this script is for setting up and testing
CRON job deployed on a vm

This script will be scheduled to automatically run in vm and return some kind of result

"""

import requests,os

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


def get_driver():

    seleniumwire_options = {
            # 'proxy': {
            #     'http':'http://S9ut1ooaahvD1OLI:DGHQMuozSx9pfIDX_country-us@geo.iproyal.com:12321',
            #     'https':'https://S9ut1ooaahvD1OLI:DGHQMuozSx9pfIDX_country-us@geo.iproyal.com:12321'
            # },
            'detach':True
        }

    uc_chrome_options = uc.ChromeOptions()
    
    # Stop images from loading to save bandwidth
    uc_chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        
    # Headless mode (no UI needed)
    uc_chrome_options.add_argument('--headless')
    uc_chrome_options.add_argument('--no-sandbox')  # Bypass OS security model
    uc_chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems
    uc_chrome_options.add_argument('--disable-gpu')  # Applicable for Windows
    uc_chrome_options.add_argument('--window-size=1920,1080')  # Set window size for headless mode
    
    # Ignore SSL certificate errors
    uc_chrome_options.add_argument('--ignore-ssl-errors=yes')
    uc_chrome_options.add_argument('--ignore-certificate-errors')
    uc_chrome_options.add_argument('--allow-running-insecure-content')

    
            
    #undetected chromedriver with proxy with chromedriver manager no .exe path
    driver = uc.Chrome(service=Service(ChromeDriverManager().install()),seleniumwire_options=seleniumwire_options,options=uc_chrome_options)
    
    return driver

# driver = get_driver()

def perform_url_scrape(url):

    driver = get_driver()

    driver.get(url)
    title = driver.title
    print(f" Successfully accessed {url}, Page title is {title}")
    return True

if __name__ == "__main__":
    print(perform_url_scrape("https://bot.sannysoft.com"))