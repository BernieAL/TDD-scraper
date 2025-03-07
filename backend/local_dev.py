from backend.tests.lambda_functions.mock_pipeline import MockScraperPipeline
from backend.lambda.scrape_orchestrator.handler import orchestrate_scraping_pipeline
import json


def run_local_pipeline():
    """Run pipeline locally with mocked AWS services"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    # Simulate pipeline execution
    event = {
        'query_hash': 'test123',
        'brand': 'prada',
        'category': 'bags'
    }
    
    print("Starting mock pipeline...")
    response = orchestrate_scraping_pipeline(event, None)
    
    print(f"Pipeline complete: {json.dumps(response, indent=2)}")

if __name__ == "__main__":
    run_local_pipeline()