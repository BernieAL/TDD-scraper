import os
import sys
from simple_chalk import chalk
from dotenv import load_dotenv, find_dotenv
from typing import Any, Optional

# Load environment variables
if os.getenv('RUNNING_IN_DOCKER') == '1':
	load_dotenv(find_dotenv('.env.docker'))   
	print(chalk.green("CONFIG - USING - .env.docker"))
	BASE_DIR = '/app'  # docker base path
else:
	load_dotenv(find_dotenv('.env.local'))   
	print(chalk.green("CONFIG - USING - .env.local"))
	BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add to Python path
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Directory paths
SCRAPER_DIR = os.path.join(BASE_DIR, 'selenium_scraper_container')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

class Config:
    """Configuration settings loaded from environment variables"""
    BASE_DIR = BASE_DIR
    SCRAPER_DIR = SCRAPER_DIR
    OUTPUT_DIR = OUTPUT_DIR
    
    # App settings
    APPLICATION_SECRET_KEY = os.getenv('APPLICATION_SECRET_KEY')
    
    # Database settings
    DB_URI = os.getenv('DB_URI')
    
    # Network settings
    PROXY_HTTPS = os.getenv('PROXY_HTTPS')
    PROXY_HTTP = os.getenv('PROXY_HTTP')
    
    # Google settings
    GOOGLE_APP_PW = os.getenv('GOOGLE_APP_PW')
    GOOGLE_SENDER_EMAIL = os.getenv('GOOGLE_SENDER_EMAIL')
    GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')

    #db seed data path
    DB_SEED_ROOT = os.getenv('DB_SEED_ROOT_PATH')
    
    # Project settings
    PROJECT_PATHS = os.getenv('PROJECT_PATHS')

def get_env_var(key: str, default: Optional[Any] = None) -> Any:
    """Get environment variable with default"""
    return os.environ.get(key, default)




