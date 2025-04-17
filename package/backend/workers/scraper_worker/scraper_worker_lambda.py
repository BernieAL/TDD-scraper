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

import sys,csv,json,os
from typing import Dict, Set
from simple_chalk import chalk
from datetime import datetime
from shutil import rmtree  # For removing directories
import boto3
from pathlib import Path
import logging
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# For Docker
if os.getenv('RUNNING_IN_DOCKER') == '1' and '/app' not in sys.path:
    sys.path.insert(0, '/app')

from config.config import BASE_DIR
from .utils.scraper_utils import ScraperUtils
from .scrapers.italist_scraper import ItalistScraper
from .utils.sku_generator import process_scraped_file
from .scraper_orchestrator import ScraperOrchestrator

# Import statements remain the same...

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

def get_aws_client(service_name: str):
    """
    Get an AWS client configured for either LocalStack or production.
    
    Args:
        service_name: The AWS service name (e.g., 's3', 'sns', 'lambda')
        
    Returns:
        A boto3 client for the specified service
    """
    config = Config(
        region_name='us-east-1',
        retries={'max_attempts': 5, 'mode': 'standard'}
    )
    
    if os.getenv('LOCALSTACK_ENDPOINT'):
        return boto3.client(
            service_name,
            endpoint_url=os.getenv('LOCALSTACK_ENDPOINT'),
            config=config
        )
    return boto3.client(service_name, config=config)

def validate_environment() -> None:
    """
    Validate that all required environment variables are present.
    Raises RuntimeError if any are missing.
    """
    required_vars = ['S3_BUCKET', 'RAW_PATH', 'FILTERED_PATH']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing_vars)}")

def validate_event(event: Dict[str, Any]) -> None:
    """
    Validate the incoming Lambda event.
    Raises ValueError if required fields are missing.
    
    Args:
        event: The Lambda event
    """
    required_fields = ['brand', 'category', 'query_hash']
    missing_fields = [field for field in required_fields if field not in event]
    
    if missing_fields:
        raise ValueError(f"Missing required event fields: {', '.join(missing_fields)}")

def lambda_handler(event, context):
    """
    Main Lambda handler function.
    
    Args:
        event: AWS Lambda event
        context: AWS Lambda context
        
    Returns:
        dict: Response with status and details
    """
    try:
        logger.info("Starting Lambda execution")
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Validate environment variables
        validate_environment()
        logger.info("Environment validation passed")
        
        # Validate event
        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary")
            
        required_fields = ['brand', 'category', 'query_hash']
        missing_fields = [field for field in required_fields if field not in event]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
        
        logger.info("Event validation passed")
        
        # Initialize AWS clients with explicit endpoint for LocalStack
        s3_client = get_aws_client('s3')
        logger.info("AWS clients initialized")
        
        # Create output directory
        output_dir = f"/tmp/{event['query_hash']}"
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
        
        # Initialize orchestrator
        orchestrator = ScraperOrchestrator()
        logger.info("ScraperOrchestrator initialized")
        
        # Run scrapers
        logger.info(f"Starting scraping for brand: {event['brand']}, category: {event['category']}")
        scraped_files = orchestrator.run_all_scrapers(
            event['brand'],
            event['category'],
            output_dir,
            event['query_hash']
        )
        logger.info(f"Scraping completed. Files: {scraped_files}")
        
        # Upload results to S3
        paths = {}
        bucket = os.environ['S3_BUCKET']
        
        for scraper_name, file_path in scraped_files.items():
            if file_path and os.path.exists(file_path):
                filename = os.path.basename(file_path)
                s3_path = f"raw/{event['query_hash']}/{filename}"
                
                logger.info(f"Uploading {filename} to S3")
                with open(file_path, 'rb') as f:
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=s3_path,
                        Body=f
                    )
                paths[scraper_name] = s3_path
        
        # Write paths.json
        paths_json = json.dumps(paths)
        s3_client.put_object(
            Bucket=bucket,
            Key=f"raw/{event['query_hash']}/paths.json",
            Body=paths_json
        )
        
        # Cleanup
        logger.info("Cleaning up temporary files")
        rmtree(output_dir, ignore_errors=True)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'query_hash': event['query_hash'],
                'results': paths
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error': str(e),
                'query_hash': event.get('query_hash', 'unknown')
            })
        }