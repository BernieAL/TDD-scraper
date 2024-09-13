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



def run_scrapers(brand,query,local):

    """
        this is the driver script for all website scrapers
    """
    italist_driver(brand,query,local)


if __name__ == "__main__":
    
    brand = "prada"
    query = "bags"
    local = True
    run_scrapers(brand,query,local)