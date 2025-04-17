import pytest
import os
import json
from workers.scraper_worker.scraper_orchestrator import orchestrate_scraping_pipeline, ScraperOrchestrator
from workers.scraper_worker.scrapers.italist_scraper import ItalistScraper

def test_italist_scraper_integration():
    """Test that Italist scraper is properly integrated with the orchestrator"""
    orchestrator = ScraperOrchestrator()
    assert 'italist' in orchestrator.get_active_scrapers()
    assert orchestrator.scrapers['italist'] == ItalistScraper

def test_italist_live_scrape():
    """Test a live scrape with Italist"""
    # Create test event
    event = {
        'query_hash': 'test_italist_123',
        'form_data': {
            'brand': 'prada',
            'category': 'bags'
        }
    }
    
    # Run the scraping pipeline
    result = orchestrate_scraping_pipeline(event, None)
    
    # Verify the results
    assert result['status'] == 'success'
    assert 'italist' in result['results']
    assert os.path.exists(result['results']['italist'])
    
    # Check the CSV file contains expected columns
    import pandas as pd
    df = pd.read_csv(result['results']['italist'])
    required_columns = ['title', 'brand', 'price', 'url']
    for col in required_columns:
        assert col in df.columns 