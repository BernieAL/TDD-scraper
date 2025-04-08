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

def lambda_handler(event, context):
    """
    AWS Lambda handler for the scraper worker.
    
    Args:
        event (dict): The Lambda event containing:
            - query_hash: Unique identifier for the scraping job
            - brand: Brand to scrape
            - category: Product category
            - local_test: Whether to run in local test mode
        context: Lambda context object
    
    Returns:
        dict: Response containing status and results
    """
    try:
        logger.info("=== Starting Scraper Worker Lambda ===")
        logger.info(f"Event received: {json.dumps(event)}")
        
        # Initialize orchestrator
        logger.info("Initializing ScraperOrchestrator...")
        orchestrator = ScraperOrchestrator()
        
        # Get environment variables
        bucket = os.environ['S3_BUCKET']
        query_hash = event['query_hash']
        raw_path = os.environ['RAW_PATH']
        filtered_path = os.environ['FILTERED_PATH']
        
        # Determine if this is a specific product search
        is_specific_search = bool(event.get('specific_item'))  # If specific_item exists, it's a specific search
        
        logger.info("Starting scrape processes...")
        logger.info(f"Configuration:")
        logger.info(f"- Bucket: {bucket}")
        logger.info(f"- Query Hash: {query_hash}")
        logger.info(f"- Raw Path: {raw_path}")
        logger.info(f"- Filtered Path: {filtered_path}")
        logger.info(f"- Brand: {event['brand']}")
        logger.info(f"- Category: {event['category']}")
        logger.info(f"- Search Type: {'Specific' if is_specific_search else 'General'}")
        
        # Run all scrapers
        logger.info("Executing scrapers...")
        scraped_files = orchestrator.run_all_scrapers(
            event['brand'],
            event['category'],
            raw_path,
            query_hash,
            event.get('local_test', True)
        )
        
        # Upload results to S3
        logger.info("Uploading results to S3...")
        s3 = boto3.client('s3')
        for scraper_name, file_path in scraped_files.items():
            if file_path and os.path.exists(file_path):
                logger.info(f"Uploading {scraper_name} results: {file_path}")
                with open(file_path, 'rb') as f:
                    filename = os.path.basename(file_path)
                    s3.put_object(
                        Bucket=bucket,
                        Key=f"{raw_path}/{filename}",
                        Body=f
                    )
                logger.info(f"Successfully uploaded {filename}")
        
        # Check for failures
        logger.info("Checking for failed scrapers...")
        failed_scrapers = orchestrator.get_failed_scrapers(query_hash)
        failure_details = orchestrator.get_failure_details(query_hash)
        
        if failed_scrapers:
            logger.warning(f"Some scrapers failed: {failed_scrapers}")
            logger.warning(f"Failure details: {failure_details}")
        
        if not scraped_files:
            error_msg = f"All scrapers failed. Failures: {failure_details}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        logger.info("=== Scraper Worker Lambda completed successfully ===")

        # After successful scraping and S3 upload
        logger.info("Invoking analysis Lambda...")
        
        # Initialize Lambda client
        lambda_client = boto3.client('lambda')
        
        # Prepare analysis event with all data needed for analysis lambda
        analysis_event = {
            'query_hash': event['query_hash'],
            'brand': event['brand'],
            'category': event['category'],
            'paths': {
                'raw': os.environ['RAW_PATH'],
                'filtered': os.environ['FILTERED_PATH'],
                'analysis': f"queries/{event['query_hash']}/analysis"
            },
            'scraped_files': list(scraped_files.keys()),  # List of which scrapers succeeded
            'timestamp': datetime.now().isoformat(),
            'is_specific_search': is_specific_search,  # Add flag for search type
            'specific_item': event.get('specific_item')  # Include specific item if it exists
        }

        # Add any failure information if relevant
        if failed_scrapers:
            analysis_event['failed_scrapers'] = list(failed_scrapers)
            analysis_event['failure_details'] = failure_details
        
        # Invoke analysis Lambda
        lambda_client.invoke(
            FunctionName='AnalysisFunction',
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(analysis_event)
        )
        
        logger.info("Analysis Lambda invoked successfully")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Scraping completed successfully',
                'analysis_triggered': True,
                'scraped_files': list(scraped_files.keys()),
                'failed_scrapers': list(failed_scrapers) if failed_scrapers else [],
                'search_type': 'specific' if is_specific_search else 'general'
            })
        }
        
    except Exception as e:
        logger.error(f"Error in scraper worker: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }

# Configure boto3 for LocalStack in development
if os.getenv('LOCALSTACK_ENDPOINT'):
    s3_config = Config(
        region_name='us-east-1',
        retries={'max_attempts': 5, 'mode': 'standard'}
    )
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('LOCALSTACK_ENDPOINT'),
        config=s3_config
    )
else:
    s3 = boto3.client('s3')

def validate_event(event: Dict[str, Any]) -> bool:
    """Validate the incoming event.
    
    Args:
        event: The Lambda event
        
    Returns:
        bool: True if event is valid, False otherwise
    """
    required_fields = ['email', 'query_hash', 'bucket_name', 'sns_topic_arn', 'analysis_results']
    return all(field in event for field in required_fields)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for the report worker.
    
    Args:
        event: The Lambda event
        context: The Lambda context
        
    Returns:
        Dict containing the response
    """
    try:
        # Validate event
        if not validate_event(event):
            logger.error("Invalid event: missing required fields")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid event: missing required fields"})
            }
        
        # Initialize AWS clients
        if os.getenv('LOCALSTACK_ENDPOINT'):
            sns = boto3.client(
                'sns',
                endpoint_url=os.getenv('LOCALSTACK_ENDPOINT'),
                config=s3_config
            )
        else:
            sns = boto3.client('sns')
        
        # Generate report
        report_generator = ReportGenerator(event['analysis_results'])
        report_path = report_generator.save_report(event['bucket_name'], s3)
        
        # Send notification
        sns.publish(
            TopicArn=event['sns_topic_arn'],
            Message=json.dumps({
                "email": event['email'],
                "query_hash": event['query_hash'],
                "report_path": report_path,
                "timestamp": datetime.now().isoformat()
            }),
            Subject="Price Analysis Report Ready"
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Report generated successfully",
                "report_path": report_path
            })
        }
        
    except ClientError as e:
        logger.error(f"AWS error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"AWS error: {str(e)}"})
        }
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error generating report: {str(e)}"})
        }