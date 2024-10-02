"""
whole app process originates here 

this driver will orchestrate reading input data - which is brands and categories to target and generate reports for
"""



import requests,os,csv,sys
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

# from scrapers.italist_scraper import italist_driver

#get parent dir  from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# print(parent_dir)

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


curr_dir= os.path.dirname(os.path.abspath(__file__))
# print(curr_dir)
user_query_data_file = os.path.join(curr_dir,'input_data','search_criteria.csv')
# print(os.path.isfile(user_query_data_file))

scraped_data_dir = os.path.join(curr_dir,'file_output')
# print(scraped_data_dir)

from Analysis.compare_data import compare_driver
from rbmq.price_change_producer import publish_to_queue



def run_scrapers(brand,query,local):

    """
        this is the driver script for all website scrapers
    """
    with open(user_query_data_file,'r',newline='',encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        
        header = next(csv_reader) #skip header row
        for row in csv_reader:
            print(row)
            if not row:
                print("skipping row ")

            #if row length is 2, row only has brand and query provided
            if len(row) == 2:
                brand,query = row
                print(brand)
                print(query)
                italist_driver(brand,query,local)
                #other website drivers

            elif len(row) == 3:
                brand,query,specific = row
                #italist_driver not set up for specific item yet
                # italist_driver(brand,query,specific,local)  
            
def run_comparisons(scraped_data_dir):
    """
    Function to trigger comparison logic for all scraped data.
    """
    # Logic to run the comparison across multiple CSV files
    print("Running comparison on all scraped data...")

    for dirpath,subdirs,files in os.walk(scraped_data_dir):
        for scraped_file in files:
            file_path = os.path.join(dirpath,scraped_file)
            compare_driver(file_path)

    #once all scraped files processed - send signal to attach price diff reports to email and send
    #signal sent directly to queue worker
    publish_to_queue({"type":"PROCESSED ALL SCRAPED FILES FOR QUERY"})

def driver_function():
    """
    Main function to orchestrate the whole process.
    """
    brand = "prada"
    query = "bags"
    local = True



    # # Step 1: Run all scrapers
    # run_scrapers(brand, query, local)
    
    # Step 2: Run comparison on scraped data
    run_comparisons(scraped_data_dir)



if __name__ == "__main__":
    
    # brand = "prada"
    # query = "bags"
    # local = True
    # run_scrapers(brand,query,local)


    driver_function()