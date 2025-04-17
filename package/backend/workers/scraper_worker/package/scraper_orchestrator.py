import os
import logging
import json
import boto3
from typing import Dict, List, Optional, Set
from datetime import datetime
from scrapers.italist_scraper import ItalistScraper

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a stream handler if not already present
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def log_step(step_name: str, details: Dict = None):
    """Helper function to log pipeline steps with consistent formatting"""
    logger.info("=" * 50)
    logger.info(f"STEP: {step_name}")
    if details:
        for key, value in details.items():
            logger.info(f"{key}: {value}")
    logger.info("=" * 50)

class MockScraper:
    def __init__(self, brand: str, category: str, output_dir: str, query_hash: str, local: bool):
        self.brand = brand
        self.category = category
        self.output_dir = output_dir
        self.query_hash = query_hash
        self.local = local
        
    def run(self) -> Optional[str]:
        """Mock scraping process that creates a dummy CSV file"""
        try:
            logger.info(f"Running mock scraper for {self.brand} {self.category}")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"RAW_MOCK_{self.brand.upper()}_{self.category.upper()}_{timestamp}_{self.query_hash}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            # Create a dummy CSV file
            with open(filepath, 'w') as f:
                f.write("title,brand,price\n")
                f.write(f"Test {self.brand} {self.category},{self.brand},100.00\n")
            
            logger.info(f"Created mock file: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error in mock scraper: {str(e)}", exc_info=True)
            return None

class ScraperOrchestrator:
    def __init__(self):
        logger.info("Initializing ScraperOrchestrator...")
        self.scrapers = {
            'mock': MockScraper,
            'italist': ItalistScraper
        }
        self.failed_scrapers: Dict[str, Set[str]] = {}
        self.scraper_errors: Dict[str, Dict[str, str]] = {}
        logger.info(f"Available scrapers: {list(self.scrapers.keys())}")
        
    def get_active_scrapers(self) -> List[str]:
        """Returns list of currently active scraper names"""
        active_scrapers = list(self.scrapers.keys())
        logger.info(f"Active scrapers: {active_scrapers}")
        return active_scrapers
    
    def record_failure(self, query_hash: str, scraper_name: str, error_msg: str):
        """Record a scraper failure for a specific query"""
        logger.warning(f"Recording failure for {scraper_name} (query_hash: {query_hash}): {error_msg}")
        if query_hash not in self.failed_scrapers:
            self.failed_scrapers[query_hash] = set()
            self.scraper_errors[query_hash] = {}
            
        self.failed_scrapers[query_hash].add(scraper_name)
        self.scraper_errors[query_hash][scraper_name] = error_msg
    
    def get_failed_scrapers(self, query_hash: str) -> List[str]:
        """Get list of failed scrapers for a query"""
        failed = list(self.failed_scrapers.get(query_hash, set()))
        logger.info(f"Failed scrapers for query {query_hash}: {failed}")
        return failed
    
    def get_failure_details(self, query_hash: str) -> Dict[str, str]:
        """Get error messages for failed scrapers"""
        details = self.scraper_errors.get(query_hash, {})
        logger.info(f"Failure details for query {query_hash}: {details}")
        return details
    
    def run_scraper(self, 
                   scraper_name: str, 
                   brand: str,
                   category: str,
                   output_dir: str,
                   query_hash: str,
                   local: bool) -> Optional[str]:
        """
        Runs a single scraper and returns the path to the scraped file
        """
        try:
            logger.info(f"=== Starting {scraper_name} scraper ===")
            logger.info(f"Configuration:")
            logger.info(f"- Brand: {brand}")
            logger.info(f"- Category: {category}")
            logger.info(f"- Output Directory: {output_dir}")
            logger.info(f"- Query Hash: {query_hash}")
            logger.info(f"- Local Mode: {local}")
            
            if scraper_name not in self.scrapers:
                error_msg = f"Unknown scraper: {scraper_name}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            scraper_class = self.scrapers[scraper_name]
            logger.info(f"Initializing {scraper_name} scraper...")
            scraper = scraper_class(brand, category, output_dir, query_hash, local)
            
            logger.info(f"Starting {scraper_name} scraping process...")
            scraped_file = scraper.run()
            
            if scraped_file and os.path.exists(scraped_file):
                logger.info(f"{scraper_name} completed successfully: {scraped_file}")
                return scraped_file
            else:
                error_msg = f"{scraper_name} completed but produced no results"
                logger.warning(error_msg)
                self.record_failure(query_hash, scraper_name, error_msg)
                return None
                
        except Exception as e:
            error_msg = f"Error running {scraper_name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.record_failure(query_hash, scraper_name, error_msg)
            return None

    def run_all_scrapers(self, 
                        brand: str,
                        category: str,
                        output_dir: str,
                        query_hash: str,
                        local: bool) -> Dict[str, Optional[str]]:
        """
        Runs all active scrapers and returns a dictionary of results
        """
        logger.info("=== Starting all scrapers ===")
        logger.info(f"Configuration:")
        logger.info(f"- Brand: {brand}")
        logger.info(f"- Category: {category}")
        logger.info(f"- Output Directory: {output_dir}")
        logger.info(f"- Query Hash: {query_hash}")
        logger.info(f"- Local Mode: {local}")
        
        results = {}
        active_scrapers = self.get_active_scrapers()
        logger.info(f"Running {len(active_scrapers)} scrapers: {active_scrapers}")
        
        for scraper_name in active_scrapers:
            logger.info(f"=== Processing {scraper_name} scraper ===")
            scraped_file = self.run_scraper(
                scraper_name, brand, category, output_dir, query_hash, local
            )
            if scraped_file:
                results[scraper_name] = scraped_file
                logger.info(f"Successfully completed {scraper_name} scraper")
            else:
                logger.warning(f"{scraper_name} scraper did not produce results")
        
        logger.info(f"Completed all scrapers. Results: {results}")
        return results 

def orchestrate_scraping_pipeline(event: Dict, context: Optional[Dict]) -> Dict:
    """
    Lambda handler function that orchestrates the scraping pipeline.
    
    Args:
        event: Lambda event containing query_hash and form_data
        context: Lambda context (unused)
        
    Returns:
        Dict containing status and results
    """
    try:
        log_step("Starting Scraping Pipeline", {"event": event})
        
        # Validate event
        if 'query_hash' not in event:
            raise ValueError("query_hash not found in event")
            
        query_hash = event['query_hash']
        form_data = event.get('form_data', {})
        brand = form_data.get('brand', '')
        category = form_data.get('category', '')
        
        log_step("Validated Input Parameters", {
            "query_hash": query_hash,
            "brand": brand,
            "category": category
        })
        
        if not brand or not category:
            raise ValueError("brand and category are required in form_data")
        
        # Initialize S3 client
        log_step("Initializing AWS Clients")
        s3 = boto3.client('s3', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))
        bucket = os.environ.get('S3_BUCKET', 'tdd-scraper-test')
        
        # Create output directory
        output_dir = f"/tmp/{query_hash}"
        os.makedirs(output_dir, exist_ok=True)
        log_step("Created Output Directory", {"path": output_dir})
        
        # Initialize paths_file early
        paths_file = os.path.join(output_dir, 'paths.json')
        results = {}
        
        try:
            # Initialize orchestrator
            log_step("Initializing Scraper Orchestrator")
            orchestrator = ScraperOrchestrator()
            
            # Run scrapers
            log_step("Running Scrapers", {
                "brand": brand,
                "category": category,
                "output_dir": output_dir,
                "query_hash": query_hash
            })
            
            results = orchestrator.run_all_scrapers(
                brand=brand,
                category=category,
                output_dir=output_dir,
                query_hash=query_hash,
                local=False
            )
            
            # Upload results to S3
            log_step("Uploading Results to S3")
            for scraper_name, filepath in results.items():
                if filepath and os.path.exists(filepath):
                    s3_key = f"queries/{query_hash}/{os.path.basename(filepath)}"
                    s3.upload_file(filepath, bucket, s3_key)
                    logger.info(f"Uploaded {filepath} to s3://{bucket}/{s3_key}")
            
            # Create and upload paths.json
            paths = {
                scraper_name: f"s3://{bucket}/queries/{query_hash}/{os.path.basename(filepath)}"
                for scraper_name, filepath in results.items()
                if filepath and os.path.exists(filepath)
            }
            with open(paths_file, 'w') as f:
                json.dump(paths, f)
            s3.upload_file(paths_file, bucket, f"queries/{query_hash}/paths.json")
            log_step("Pipeline Complete", {"results": paths})
            
            return {
                'status': 'success',
                'query_hash': query_hash,
                'results': paths
            }
            
        finally:
            # Clean up
            log_step("Cleaning Up")
            for filepath in results.values():
                if filepath and os.path.exists(filepath):
                    os.remove(filepath)
            if os.path.exists(paths_file):
                os.remove(paths_file)
            if os.path.exists(output_dir):
                try:
                    os.rmdir(output_dir)
                except OSError:
                    # Directory not empty, but that's okay
                    pass
        
    except Exception as e:
        logger.error(f"Error in scraping pipeline: {str(e)}", exc_info=True)
        raise 