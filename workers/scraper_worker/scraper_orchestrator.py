import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

from workers.scraper_worker.scrapers.rebag_scraper import RebagScraper
from workers.scraper_worker.scrapers.vestiaire_scraper import VestiaireScraper
from workers.scraper_worker.scrapers.italist_scraper import ItalistScraper
from workers.scraper_worker.utils.monitoring_service import MonitoringService
from workers.scraper_worker.utils.status_tracker import StatusTracker

class ScraperOrchestrator:
    def __init__(self, monitoring_service: MonitoringService, status_tracker: StatusTracker, s3_bucket: str, dynamodb_table: str):
        """Initialize the ScraperOrchestrator with necessary services and configuration."""
        self.monitoring_service = monitoring_service
        self.status_tracker = status_tracker
        self.s3_bucket = s3_bucket
        self.dynamodb_table = dynamodb_table
        self.failed_scrapers: Dict[str, List[str]] = {}
        self.logger = logging.getLogger(__name__)
        
        # Initialize scrapers
        self.scrapers = {
            'rebag': RebagScraper(
                monitoring_service=monitoring_service,
                status_tracker=status_tracker,
                s3_bucket=s3_bucket,
                dynamodb_table=dynamodb_table
            ),
            'vestiaire': VestiaireScraper(
                monitoring_service=monitoring_service,
                status_tracker=status_tracker,
                s3_bucket=s3_bucket,
                dynamodb_table=dynamodb_table
            ),
            'italist': ItalistScraper(
                monitoring_service=monitoring_service,
                status_tracker=status_tracker,
                s3_bucket=s3_bucket,
                dynamodb_table=dynamodb_table
            )
        }

    def get_active_scrapers(self) -> Set[str]:
        """Get the set of active scraper names."""
        return set(self.scrapers.keys()) - set(self.failed_scrapers.keys())

    def record_failure(self, scraper_name: str, error_message: str) -> None:
        """Record a scraper failure with error details."""
        if scraper_name not in self.failed_scrapers:
            self.failed_scrapers[scraper_name] = []
        self.failed_scrapers[scraper_name].append(f"{datetime.now()}: {error_message}")
        self.logger.error(f"Scraper {scraper_name} failed: {error_message}")

    def get_failed_scrapers(self) -> Set[str]:
        """Get the set of failed scraper names."""
        return set(self.failed_scrapers.keys())

    def get_failure_details(self, scraper_name: str) -> Optional[List[str]]:
        """Get failure details for a specific scraper."""
        return self.failed_scrapers.get(scraper_name)

    def run_all_scrapers(self) -> List[Dict]:
        """Run all active scrapers and return combined results."""
        self.logger.info("Starting scraper orchestration")
        results = []

        for scraper_name, scraper in self.scrapers.items():
            if scraper_name not in self.failed_scrapers:
                try:
                    self.logger.info(f"Running scraper: {scraper_name}")
                    scraper_results = scraper.run()
                    if scraper_results:
                        results.extend(scraper_results)
                    self.logger.info(f"Scraper {scraper_name} completed successfully")
                except Exception as e:
                    self.record_failure(scraper_name, str(e))
                    self.logger.error(f"Error running scraper {scraper_name}: {str(e)}")

        self.logger.info(f"Scraper orchestration completed. Total results: {len(results)}")
        return results 