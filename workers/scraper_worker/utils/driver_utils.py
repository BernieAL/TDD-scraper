from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def get_driver():
    """
    Get a configured Selenium WebDriver.
    
    Returns:
        webdriver: Configured Chrome WebDriver
    """
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    return webdriver.Chrome(options=chrome_options) 