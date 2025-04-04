import json
import os
import logging
from scraper_orchestrator import ScraperOrchestrator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_scraper_worker():
    # Test event that mimics what the Lambda would receive
    test_event = {
        "query_hash": "test_query_hash_123",
        "brand": "prada",
        "category": "bags",
        "local_test": True
    }
    
    # Set up environment variables
    os.environ['S3_BUCKET'] = 'scraper-data-bucket'
    os.environ['RAW_PATH'] = 'raw'
    os.environ['FILTERED_PATH'] = 'filtered'
    
    # Create necessary directories
    os.makedirs('raw', exist_ok=True)
    os.makedirs('filtered', exist_ok=True)
    
    try:
        # Initialize orchestrator
        logger.info("Initializing ScraperOrchestrator...")
        orchestrator = ScraperOrchestrator()
        
        # Run scrapers
        logger.info("Starting scrape processes...")
        scraped_files = orchestrator.run_all_scrapers(
            test_event['brand'],
            test_event['category'],
            os.environ['RAW_PATH'],
            test_event['query_hash'],
            test_event['local_test']
        )
        
        # Check results
        logger.info("Checking results...")
        if scraped_files:
            logger.info("Scraping completed successfully!")
            logger.info(f"Scraped files: {scraped_files}")
        else:
            logger.error("No files were scraped")
            
        # Check for failures
        failed_scrapers = orchestrator.get_failed_scrapers(test_event['query_hash'])
        if failed_scrapers:
            logger.warning(f"Some scrapers failed: {failed_scrapers}")
            failure_details = orchestrator.get_failure_details(test_event['query_hash'])
            logger.warning(f"Failure details: {failure_details}")
        
    except Exception as e:
        logger.error(f"Error running scraper worker: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    test_scraper_worker() 