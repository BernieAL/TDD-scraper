import requests,os,csv
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

# from utils.copy_file import copy_file

# Create a session object
session = requests.Session()

#target purse
target = "locky bb"



urls = [
    # 'https://www.italist.com/us/',
    'https://www.cettire.com/pages/search?q=locky%20bb&qTitle=locky%20bb&menu%5Bdepartment%5D=women',
    'https://shop.rebag.com/search?q=locky+bb',
    'https://us.vestiairecollective.com/search/?q=locky+bb#gender=Women%231',
    'https://www.fashionphile.com/shop?search=locky+bb',
    # 'https://www.therealreal.com/' #requires login
]

brand = "prada"
italist_general_param_url = f"https://www.italist.com/us/brands/{brand}/110/women/"
# next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=1&categories%5B%5D=437&skip=60

italist_branded_bags_url = f"https://www.italist.com/us/brands/{brand}/110/women/?categories%5B%5D=76"
#next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=76&skip=60


#for testing using locally saved copy of website
""" Reminder - this is a single saved web page that will be specific to specific bag or search
"""
local_saved_file_path = os.path.abspath('src/local_websites/Prada Bags for Women ALWAYS LIKE A SALE.html')
local_url = 'file:///' + local_saved_file_path.replace('\\','/')




def get_driver():

    seleniumwire_options = {
            # 'proxy': {
            #     'http':'http://S9ut1ooaahvD1OLI:DGHQMuozSx9pfIDX_country-us@geo.iproyal.com:12321',
            #     'https':'https://S9ut1ooaahvD1OLI:DGHQMuozSx9pfIDX_country-us@geo.iproyal.com:12321'
            # },
            'detach':True
        }

    uc_chrome_options = uc.ChromeOptions()
    
    #stop images from loading - improve page speed and reduce proxy data usage
    # uc_chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    
    #ignore ssl issues from https
    # uc_chrome_options.set_capability('acceptSslCerts',True)
   
    # uc_chrome_options.add_argument('--ignore-certificate-errors')
    # uc_chrome_options.add_argument('--allow-running-insecure-content')
    uc_chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    uc_chrome_options.add_argument('--ignore-ssl-errors=yes')
    uc_chrome_options.add_argument('--ignore-certificate-errors')
    uc_chrome_options.add_argument('--allow-running-insecure-content')

            
    #undetected chromedriver with proxy with chromedriver manager no .exe path
    driver = uc.Chrome(service=Service(ChromeDriverManager().install()),options=uc_chrome_options)
    
    return driver

def get_numeric_only(price_str):
    """
    raw extract price looks like "USD 4089" - this function strips out chars and spaces, returning only numeric
    
    """
    price = price_str.split("USD")[1]
    return int(price)

def get_num_listings(driver):

    """
    Gets num of listings displayed on page 
    """
    try:
        #element on web page shows numeric value of search results
        num_listings = driver.find_element(By.XPATH,"//span[contains(@class, 'result-count')]").text
        return int(num_listings)
    except NoSuchElementException:
        print("Could not find the number of listings.")
        return 0

def get_listings(driver):
    """
    Finds all <a> elements inside product grid container
    """
    try:
        return driver.find_elements(By.XPATH, "//div[contains(@class, 'product-grid-container')]//a")
    except NoSuchElementException:
        print("Could not find listings")
        return []

def generate_id_from_url(url):
    """
    Based on url structure:

    https://www.italist.com/us/women/bags/luggage/calf-leather-pouch/14430526/14598217/prada/
    
    Generate a unique id to track if a listing is still present on the page.
    Also used to track changes in listing price over the duration it is available.

    """
    try:
        tokens = url.split('/')
        
        # Ensure there are enough tokens to form a unique ID
        if len(tokens) < 5:
            raise ValueError("URL structure is invalid or incomplete.")
        
        unique_product_id = f"{tokens[-3]}-{tokens[-4]}"
        return unique_product_id

    except Exception as e:
        print(f"Error in function 'generate_id_from_url': {e}")
        return None


# print(generate_id_from_url("https://www.italist.com/us/women/bags/luggage/calf-leather-pouch/14430526/14598217/prada/"))

def extract_listing_data(listing):

    """
    Extracts the brand, product name, and price from a single listing.
    """
    
    try:
        listing_url = listing.get_attribute('href')
        product_id = generate_id_from_url(listing_url)
        # print(listing_url)
        
    except NoSuchElementException:
        print("no url")

    try:
        brand = listing.find_element(By.CSS_SELECTOR, "div.brand").text
    except NoSuchElementException:
        brand = "No brand"

    try:
        product_name = listing.find_element(By.XPATH, ".//div[contains(@class, 'productName')]").text
    except NoSuchElementException:
        product_name = "No product name"

    try:
        sale_price = listing.find_element(By.XPATH, ".//span[contains(@class, 'sales-price')]").text
        price = sale_price
    except NoSuchElementException:
        try:
            price = listing.find_element(By.XPATH, ".//span[contains(@class, 'price')]").text
        except NoSuchElementException:
            price = "No price"

    price = get_numeric_only(price)

    return product_id, brand, product_name, price, listing_url

def build_output_file_name(output_dir,brand,query):
    """Generates a default output file name based on the current date."""
    current_date = datetime.now().strftime('%Y-%d-%m')
    italist_output_file = f"{output_dir}/italist_{current_date}_{brand}_{query}.csv"
    return os.path.abspath(italist_output_file)

def italist_scrape_2(output_dir,brand,query,specific_item,url,output_file=None):
   
    """
    Scrapes the specified Italist URL and writes the results (Brand, Product Name, Price) to a CSV file.

    Parameters:
    - url (str): The URL to scrape.
    - output_file (str): Optional path to the output file. If not provided, a default path will be used.
    """

    #initialize webdriver
    driver = get_driver()

    try:
        driver.get(url)
        time.sleep(2) #wait for page to load

        #get all listings in product grid container
        all_listings = get_listings(driver)
        
        #get number of search results
        num_listings = get_num_listings(driver)
        
        #set default output file if none provided
        if output_file is None:
            output_file = build_output_file_name(output_dir,brand,query)
        
        with open(output_file,mode='w',newline='',encoding='utf-8') as file:
            writer = csv.writer(file)
            current_date = datetime.now().strftime('%Y-%d-%m')
            file.write(f"Scraped: {current_date} \n")
            file.write(f"Query: {brand}-{query} \n")
            writer.writerow(['product_id','brand','product_name','curr_price','listing_url'])
            file.write('---------------------- \n')

            #process listings and write to file
            for listing in all_listings[:num_listings]:
                product_id, brand, product_name, price, listing_url= extract_listing_data(listing)
              
                writer.writerow([product_id,brand,product_name,price,listing_url])
                print(f"Written: {product_id},{brand}, {product_name}, {price},{listing_url}")
        
        #create copy of file, store in LTR storage
        # copy_file(output_file)
    
    except Exception as e:
        print(f"Error Occured: {e}")
    
    finally:
        driver.quit()



#testing italist extraction using local file as url
# file_path = os.path.abspath('src/local_websites/Prada Bags for Women ALWAYS LIKE A SALE.html')
# url = 'file:///' + file_path.replace('\\','/')
# italist_scrape_2(url)


def italist_driver(output_dir,brand,query,specific_item,local=True):

    """
    local means we are testing with locally saved copy of website 
    to limit requests to live site 
    """
    try:
        match (query,specific_item,local):
            
            case ("bags",False):   
                italist_branded_bags_url = f"https://www.italist.com/us/brands/{brand}/110/women/?categories%5B%5D=76"
                #next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=76
                italist_scrape_2(italist_branded_bags_url,brand,query)
            case ("all",False):
                italist_general_param_url = f"https://www.italist.com/us/brands/{brand}/110/women/"
                # next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=1&categories%5B%5D=437&skip=60
                italist_scrape_2(italist_general_param_url)
            
            case ("bags",True):   
                #next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=76
                italist_scrape_2(local_url,brand,query)
            case ("all",True):
                #next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=76
                italist_scrape_2(local_url,brand,query)
            case ("bags",specific_item,True):
                italist_general_param_url = f"https://www.italist.com/us/brands/{brand}/110/women/"
                # next page https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=1&categories%5B%5D=437&skip=60
                italist_scrape_2(output_dir,brand,query,specific_item,local_url)
            
            case _:
                print("No matching case found.")

            #use bags url and paramterize into it
    except Exception as e:
        # Log the error and handle it gracefully
        print(f"An error occurred in italist_driver: {e}")
        # Optionally: log the stack trace for debugging
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    
    specific_item = False
    output_dir = os.path.join(os.path.dirname(__file__),'..','file_output','RAW_SCRAPE_2024-05-10_bags_a7b5c73d')
    # print(os.path.isdir(output_dir))
    italist_driver(output_dir,"prada","bags",specific_item,True)
    # italist_driver("prada","general")



