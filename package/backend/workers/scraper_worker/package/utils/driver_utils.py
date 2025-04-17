from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc

def get_driver():
    """Get a configured Selenium WebDriver."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    
    driver = uc.Chrome(options=options)
    return driver 