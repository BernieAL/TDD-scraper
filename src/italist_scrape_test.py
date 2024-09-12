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

# driver = get_driver()


#for each url in urls, nav to and search for target purse
#the process of this could be different for each depending on site layout. 
# Define a function to fetch the page content
def fetch_page(url):
    response = requests.get(url)
    response.raise_for_status()  # Ensure we get a valid response
    return response.text

#parse url name to use as filename
def parse_url(url):

    url_tokens = url.split("//")
    url_tokens = url_tokens[1].split(".com")
    name = url_tokens = url_tokens[0]
    return name

"""
Creates file in file_output dir using name
"""
def create_file(name):
    #get cwd - src dir
    src_dir = os.getcwd()

    #build path to src/file_output dir
    file_output_dir = os.path.join(src_dir,"file_output")

    #if dir doesnt exist, make it
    if not os.path.exists(file_output_dir):
        os.makedirs(file_output_dir)
    # print(file_output_dir)
    
    #build file path to create 
    file_to_create = os.path.join(file_output_dir,name)
    print(file_to_create)

    if not os.path.exists(file_to_create):
        #open file and write to it
        with open(file_to_create,'w') as file:
            file.write(" ")

    return file_to_create

def extract_html_url(url):

    response = requests.get(url)
    html = response.text
    return html

def write_file(file_path,html):

    #open file by name, write html content to it
    # file_path = f'{name}.txt'
    file_path = file_path
    with open(file_path,'w',encoding='utf-8') as file:
        file.write(html)


def italist_elements():
    
    current_date = datetime.now()
    formatted_date = current_date.strftime('%Y-%d-%m')
    italist_output_file = f"src/file_output/italist_{formatted_date}"
    date_italist_output_file = os.path.abspath(italist_output_file)
    

    """
        for specific brand -> https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=76
        second page for specific brand -> https://www.italist.com/us/brands/prada/110/women/?categories%5B%5D=76&skip=60

        first page for all brands -> https://www.italist.com/us/women/bags/76/?on_sale=1
        second page for all brands -> https://www.italist.com/us/women/bags/76/?skip=60&on_sale=1

        pagination using url - "skip=60" puts you at 2nd page. "skip=120" puts you at 3rd page

        element targeting:
            jsx-680171959 product-grid-container - this is parent container of all listings
                jsx-4016361043 brand - card element brand name
                jsx-4016361043 productName - card element product name
                jsx-3297103580 price - card element price 


        approach:
            get all elements in parent container
                for each element
                    extract brand, productName,price

    """
    driver = get_driver()
    # # driver.get("https://bot.sannysoft.com/")
    # driver.get("Prada Bags for Women ALWAYS LIKE A SALE.html")

    file_path = os.path.abspath('src/Prada Bags for Women ALWAYS LIKE A SALE.html')
    file_url = 'file:///' + file_path.replace('\\','/')
    # print(file_path)
    driver.get(file_url)

    # product_container = driver.find_element(By.CLASS_NAME,'product-grid-container')
    
    product_container = driver.find_element(By.XPATH, "//div[contains(@class, 'product-grid-container')]")
    time.sleep(5)
    num_listings = driver.find_element(By.XPATH,"//span[contains(@class, 'result-count')]").text

    """
    all listings is supposed to grab 'a' tag elements inside product-grid-container, 
    but it may include 'a' elements from outside of product-grid-container
    to combat this, we use the number of results displayed on the web page to filter all_listings
    - so we dont bother checking 'a' tag elements that are outside of product-grid-container
    Ex. if theres 171 results on the page, we slice all_listings to [:171]
    """
    all_listings = driver.find_elements(By.XPATH, "//div[contains(@class, 'product-grid-container')]//a")

   
    with open(date_italist_output_file,mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Brand','Product Name','Price'])


        for listing in all_listings[:171]:
            
            # print(listing.text)
            #LOCATING BRAND NAME
            try:
                # brand = listing.find_element(By.XPATH,".//div[contains(@class, 'brand')]")
                brand = listing.find_element(By.CSS_SELECTOR,"div.brand").text
                print(brand)
                # print(brand.get_attribute('innerText'))
            except NoSuchElementException:
                print("no brand found - skipping to next iteration")
                continue


            #LOCATION PRODUCT NAME
            try:
                productName = listing.find_element(By.XPATH,".//div[contains(@class, 'productName')]").text
                print(productName)
                # print(productName.get_attribute('innerText'))

            except NoSuchElementException:
                print("no product name found")

            #LOCATING PRICE
            #a listing card will either have sale price or regular price. 
            #attempt to find sale price first, if thats not found, return price
            try:
                salePrice = listing.find_element(By.XPATH,".//span[contains(@class, 'sales-price')]")
                price_result = salePrice.text
                print("Found price:", price_result)
                print("-------")
            except NoSuchElementException:
                try:
                    price = listing.find_element(By.XPATH,".//span[contains(@class, 'price')]")
                    price_result = price.text
                    print("Found price:", price_result)
                    print("--------")
                except NoSuchElementException:
                    price_result = None
                    print("Neither 'sales-price' nor 'price' could be found.")
                    print("--------")
                
                    
                
            # # Now you can use price_element as needed
            # if price_result:
            #     print("Found price:", price_result)
            # else:
            #     print("No price element found.")

            listing_line = f"{brand},{productName},{price_result} \n"
            file.write(listing_line)

        time.sleep(5)
        driver.close()
italist_elements()

def process_url_runner(urls):

   

    for url in urls:
        
        name = parse_url(url)
        file_path = create_file(name)
        html = extract_html_url(url)
        write_file(file_path,html)




# -----------------------------------------------------------------------------------------------------
def test_parse_url():

    url = "https://us.vestiairecollective.com/"
    url_tokens = url.split("//")
    url_tokens = url_tokens[1].split(".com")
    name = url_tokens = url_tokens[0]
    print(name)
    assert name == 'us.vestiairecollective'


@pytest.fixture
#fixture to handle file creation
def create_file():
    #get cwd - src dir
    src_dir = os.getcwd()

    #build path to src/file_output dir
    file_output_dir = os.path.join(src_dir,"file_output")

    #if dir doesnt exist, make it
    if not os.path.exists(file_output_dir):
        os.makedirs(file_output_dir)
    # print(file_output_dir)
    
    #build file path to create 
    file_to_create = os.path.join(file_output_dir,'us.vestiairecollective.txt')
    print(file_to_create)

    #open file and write to it
    with open(file_to_create,'w') as file:
        file.write("test")

    return file_to_create

# Test to check if the file was created
def test_file_creation(create_file):
    assert os.path.exists(create_file),f"File '{create_file}' was not created."
   
def test_file_content(create_file):
    with open(create_file,'r') as file:
        contents = file.read().strip()

    assert contents == "test",f"File content is '{contents}, but expected 'test'."
