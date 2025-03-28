"""
Scraper Worker Lambda Handler

This module handles AWS Lambda events for the scraping system. It validates
incoming events, coordinates scraping jobs, and manages AWS service interactions.

Main Components:
- Lambda event handling
- Input validation
- Scraper orchestration
- AWS service integration (S3, SNS)

Key Functions:
    lambda_handler(event, context):
        Main entry point for AWS Lambda

    validate_event(event):
        Validates incoming event structure

    process_scraping_job(event_data):
        Coordinates scraping execution

Note: Detailed function documentation is provided with each function.

Dependencies:
    - boto3
    - ScraperOrchestrator
    - AWS Lambda Runtime
"""

fimport sys,csv,json,os
from typing import Dict, Set
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree  # For removing directories
import boto3
from pathlib import Path
from .utils.sku_generator import process_scraped_file

# For local development
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# For Docker
if os.getenv('RUNNING_IN_DOCKER') == '1' and '/app' not in sys.path:
    sys.path.insert(0, '/app')



def ensure_init_files():
    """
    for each dir in list dir, 
    check if each dir has __init__.py,
    if not make one. 

    This avoids the module not found issue that results
    from copying specific files without including __init__.py

    """
    #get current dir contents
    dirs = os.listdir()

    for dir in dirs:

        #get full path
        full_path = os.path.abspath(dir)

        #check if its a dir
        if os.path.isdir(full_path):
            
            #build path for __init__ file in curr dir
            init_file_path = os.path.join(full_path,'__init__.py')

            #check if init file exists
            if not os.path.exists(init_file_path):
                print(f"Creating __init__.py file for dir: {dir}")

                with open(init_file_path, 'w') as f:
                    pass
            else:
                print(f"__init__.py already exists for {dir}")

ensure_init_files()  



from config.config import BASE_DIR, RBMQ_DIR  
from utils.ScraperUtils import ScraperUtils
from scrapers.italist_scraper import ItalistScraper
from utils.sku_generator import generate_master_sku_col



import sys, csv, json, os
import boto3
from simple_chalk import chalk
from datetime import datetime
from typing import Dict, List, Optional, Set
from backend.workers.scraper_worker import ScraperOrchestrator

# Import statements remain the same...



def main():
    orchestrator = ScraperOrchestrator()
    
    # AWS messaging will go here

    # Get environment variables
    bucket = os.environ['S3_BUCKET']
    query_hash = os.environ['QUERY_HASH']
    raw_path = os.environ['RAW_PATH']
    filtered_path = os.environ['FILTERED_PATH']
    
    # Get form data from S3
    s3 = boto3.client('s3')
    form_data = s3.get_object(
        Bucket=bucket,
        Key=f'queries/{query_hash}/form-params.json'
    )
    params = json.loads(form_data['Body'].read())
    
    print(chalk.blue("Starting scrape processes..."))
    
    # Run all scrapers, collects output files as return val
    scraped_files = orchestrator.run_all_scrapers(
        params['brand'],
        params['category'],
        raw_path,  # Use S3 path instead of local dir
        query_hash,
        params.get('local_test', True)
    )
    
    # Upload results to S3
    for scraper_name, file_path in scraped_files.items():
        if file_path and os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                filename = os.path.basename(file_path)
                s3.put_object(
                    Bucket=bucket,
                    Key=f"{raw_path}/{filename}",
                    Body=f
                )
    
    # Get failure information
    failed_scrapers = orchestrator.get_failed_scrapers(query_hash)
    failure_details = orchestrator.get_failure_details(query_hash)
    
    # Check if any scrapers succeeded
    if not scraped_files:
        raise Exception(f"All scrapers failed. Failures: {failure_details}")
    
    # After raw CSV is written
    raw_file_path = Path(file_path)

    #group unique products, generate master sku for each unique product, create and insert new col
    generate_master_sku_col(raw_file_path)

if __name__ == "__main__":
    print(chalk.green("Starting scrape worker..."))
    main()

# def scrape_and_upload(params_data):
#     scraped_data = scrape_website()  # Your scraping logic
    
#     # Upload directly using the path pattern
#     s3.put_object(
#         Bucket='scraper-data-bucket',
#         Key=f'{params_data["paths"]["raw"]}/RAW_ITALIST_{params_data["brand"]}_{datetime.now():%Y-%d-%m}_{params_data["category"]}.csv',
#         Body=scraped_data
#     )

def lambda_handler(event, context):
    """
    AWS Lambda entry point for scraper worker.

    Args:
        event (dict): AWS Lambda event containing:
            - scraper_name (str): Name of scraper to run
            - brand (str): Brand to scrape
            - category (str): Product category
        context (LambdaContext): AWS Lambda context

    Returns:
        dict: Response containing:
            - statusCode (int): HTTP status code
            - body (dict): Response data or error message

    Raises:
        ValueError: If event validation fails
        ScraperError: If scraping operation fails
    
    Example:
        event = {
            "scraper_name": "ITALIST",
            "brand": "PRADA",
            "category": "BAGS"
        }
    """