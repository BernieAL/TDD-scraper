import boto3
import json
import os
from datetime import datetime
import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def orchestrate_scraping_pipeline(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Orchestrates the scraping pipeline by:
    1. Using form data from the event
    2. Running scrapers locally
    3. Storing results in S3
    4. Triggering analysis
    """
    print("=== START OF FUNCTION ===")
    print(f"Raw event: {event}")
    print(f"Event type: {type(event)}")
    print(f"Event keys: {event.keys() if isinstance(event, dict) else 'Not a dict'}")
    
    try:
        logger.info("Starting scraping pipeline orchestration")
        logger.info(f"Event type: {type(event)}")
        logger.info(f"Event: {json.dumps(event, indent=2)}")
        
        logger.info(f"Starting scraping pipeline with event: {json.dumps(event)}")
        
        # Initialize S3 client with explicit endpoint URL
        s3 = boto3.client(
            's3',
            endpoint_url=os.environ.get('AWS_ENDPOINT_URL', 'http://localhost:4567'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'test'),
            region_name='us-east-1'
        )
        
        # Get query hash from event
        query_hash = event.get('query_hash')
        if not query_hash:
            raise ValueError("query_hash not found in event")
        print(f"Query hash: {query_hash}")
        
        # Get form data from event
        if 'form_data' not in event:
            raise ValueError("form_data not found in event")
        
        form_data = event['form_data']
        print(f"Form data type: {type(form_data)}")
        print(f"Form data keys: {form_data.keys() if isinstance(form_data, dict) else 'Not a dict'}")
        print(f"Form data: {json.dumps(form_data, indent=2)}")
        
        # Define S3 paths for data storage
        paths = {
            'raw': f"queries/{query_hash}/raw",
            'filtered': f"queries/{query_hash}/filtered",
            'analysis': f"queries/{query_hash}/analysis",
            'reports': f"queries/{query_hash}/reports"
        }
        logger.info(f"Defined S3 paths for data storage: {json.dumps(paths)}")
        
        # Store paths configuration in S3
        logger.info("Storing paths configuration in S3")
        s3.put_object(
            Bucket=os.environ.get('S3_BUCKET', 'tdd-scraper-test'),
            Key=f"queries/{query_hash}/paths.json",
            Body=json.dumps(paths)
        )
        logger.info("Successfully stored paths configuration")
        
        # Run scraper locally
        logger.info("Running scraper locally")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create raw data directory
            raw_dir = temp_path / 'raw'
            raw_dir.mkdir()
            
            # Run scraper
            from backend.workers.scraper_worker.scraper_orchestrator import ScraperOrchestrator
            scraper = ScraperOrchestrator()
            scraper_results = scraper.run_all_scrapers(
                form_data['brand'],
                form_data['category'],
                str(raw_dir),
                query_hash,
                True  # local_test
            )
            logger.info(f"Scraper results: {scraper_results}")
            
            # Upload results to S3
            logger.info("Uploading results to S3")
            for scraper_name, result_file in scraper_results.items():
                with open(result_file, 'rb') as f:
                    s3.put_object(
                        Bucket=os.environ.get('S3_BUCKET', 'tdd-scraper-test'),
                        Key=f"{paths['raw']}/{scraper_name}.csv",
                        Body=f
                    )
            logger.info("Successfully uploaded results to S3")
            
            # Clean up temporary files
            logger.info("Cleaning up temporary files")
            
        logger.info("Pipeline completed successfully")
        return {
            'status': 'success',
            'message': 'Scraping pipeline completed successfully',
            'query_hash': query_hash,
            'paths': paths
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

def get_form_data(s3_client, query_hash: str) -> Dict[str, Any]:
    """Retrieves form data from S3 for the given query hash."""
    try:
        response = s3_client.get_object(
            Bucket=os.environ['S3_BUCKET'],
            Key=f"form_data/{query_hash}.json"
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to retrieve form data: {str(e)}")
        raise 