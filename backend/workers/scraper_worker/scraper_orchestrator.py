"""

Class for managing scraper operations

    Orchestrates and manages the scraping process across multiple e-commerce sources.

    The ScraperOrchestrator serves as the central coordinator for all scraping operations,
    managing individual scrapers, handling resources, and coordinating the workflow from
    scraping to data storage.

    Responsibilities:
        - Initialize and manage scraper instances for different sources
        - Coordinate the scraping workflow
        - Handle shared resources (S3, temp files)
        - Provide consistent error handling
        - Manage data storage and file operations

    Attributes:
        s3_client: boto3.client
            AWS S3 client for cloud storage operations
        utils: ScraperUtils
            Utility instance for file operations and temp directory management
        scrapers: dict
            Dictionary mapping source names to scraper instances

    Example Usage:
        orchestrator = ScraperOrchestrator(s3_client)
        orchestrator.initialize_scrapers()
        
        # Scrape Prada bags from Italist
        result = orchestrator.scrape_source(
            source='ITALIST',
            brand='PRADA',
            category='BAGS'
        )

    Flow:
        1. Initialize orchestrator with necessary clients
        2. Create scraper instances for each source
        3. Receive scraping request for specific source/brand/category
        4. Delegate to appropriate scraper
        5. Save and store results
        6. Handle any errors during process

    Error Handling:
        - ScraperNotFoundError: When requested source has no scraper
        - ScrapingError: When scraping operation fails
        - StorageError: When file operations fail
    


"""


from config.config import BASE_DIR, RBMQ_DIR  
from .utils.scraper_utils import ScraperUtils
from .scrapers.italist_scraper import ItalistScraper
from .utils.sku_generator import generate_master_sku_col

import sys, csv, json, os
import boto3
from simple_chalk import chalk
from datetime import datetime
from typing import Dict, List, Optional, Set


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
        
        Returns:
            Dict[str, Optional[str]]: Dictionary mapping scraper names to their output file paths
            Example: {
                'italist': '/path/to/temp/italist/RAW_ITALIST_PRADA_...',
                'farfetch': '/path/to/temp/farfetch/RAW_FARFETCH_PRADA_...'
            }
        """
        results = {}
        
        for scraper_name in self.get_active_scrapers():
            scraped_file = self.run_scraper(
                scraper_name, brand, category, output_dir, query_hash, local
            )
            if scraped_file:
                results[scraper_name] = scraped_file
                
        return results