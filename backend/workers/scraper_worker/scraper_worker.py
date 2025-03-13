import sys,csv,json,os
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

# Import statements remain the same...

class ScraperOrchestrator:
    def __init__(self):
        self.scrapers = {
            'italist': ItalistScraper,
            # Add other scrapers here
        }
        # Track failures per query hash
        self.failed_scrapers: Dict[str, Set[str]] = {}
        # Track error messages for failed scrapers
        self.scraper_errors: Dict[str, Dict[str, str]] = {}
        
    def get_active_scrapers(self) -> List[str]:
        """Returns list of currently active scraper names"""
        return list(self.scrapers.keys())
    
    def record_failure(self, query_hash: str, scraper_name: str, error_msg: str):
        """Record a scraper failure for a specific query"""
        if query_hash not in self.failed_scrapers:
            self.failed_scrapers[query_hash] = set()
            self.scraper_errors[query_hash] = {}
            
        self.failed_scrapers[query_hash].add(scraper_name)
        self.scraper_errors[query_hash][scraper_name] = error_msg
    
    def get_failed_scrapers(self, query_hash: str) -> List[str]:
        """Get list of failed scrapers for a query"""
        return list(self.failed_scrapers.get(query_hash, set()))
    
    def get_failure_details(self, query_hash: str) -> Dict[str, str]:
        """Get error messages for failed scrapers"""
        return self.scraper_errors.get(query_hash, {})
    
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
            if scraper_name not in self.scrapers:
                raise ValueError(f"Unknown scraper: {scraper_name}")
            
            scraper_class = self.scrapers[scraper_name]
            scraper = scraper_class(brand, category, output_dir, query_hash, local)
            
            print(chalk.blue(f"Running {scraper_name} scraper with:"))
            print(chalk.blue(f"Brand: {brand}"))
            print(chalk.blue(f"Category: {category}"))
            print(chalk.blue(f"Output Dir: {output_dir}"))
            
            scraped_file = scraper.run()
            
            if scraped_file and os.path.exists(scraped_file):
                print(chalk.green(f"{scraper_name} completed successfully: {scraped_file}"))
                return scraped_file
            else:
                error_msg = f"{scraper_name} completed but produced no results"
                print(chalk.yellow(error_msg))
                self.record_failure(query_hash, scraper_name, error_msg)
                return None
                
        except Exception as e:
            error_msg = f"Error running {scraper_name}: {str(e)}"
            print(chalk.red(error_msg))
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
        results = {}
        
        for scraper_name in self.get_active_scrapers():
            scraped_file = self.run_scraper(
                scraper_name, brand, category, output_dir, query_hash, local
            )
            if scraped_file:
                results[scraper_name] = scraped_file
                
        return results

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