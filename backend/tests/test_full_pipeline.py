"""
here we are mock testing the full pipeline,
this involves creating the mock pipeline
and give it the the mock ecs tasks
also mocking the form submission data and giving to pipeline
"""
from moto import mock_ecs, mock_s3
from unittest.mock import patch
import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backend.tests.mock_pipeline import MockScraperPipeline
from backend.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline

@mock_ecs
@mock_s3

def test_complete_pipeline_success():
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()

    with patch('time.sleep'):  # Don't actually wait in tests
        # Test scraping stage
        response = orchestrate_scraping_pipeline({
            'query_hash': '123abc',
            'brand': 'prada',
            'category': 'bags'
        }, None)
        
        # Mock successful completion of each stage
        tasks = mock_pipeline.ecs.list_tasks(cluster='scraper-cluster')
        for task_arn in tasks['taskArns']:
            mock_pipeline.mock_successful_task(task_arn)
            
        assert response['statusCode'] == 200

@mock_ecs
@mock_s3
def test_pipeline_failure_handling():
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    with patch('time.sleep'):
        response = orchestrate_scraping_pipeline({
            'query_hash': '123abc',
            'brand': 'prada',
            'category': 'bags'
        }, None)
        
        # Mock failure of analysis task
        tasks = mock_pipeline.ecs.list_tasks(cluster='scraper-cluster')
        mock_pipeline.mock_failed_task(
            tasks['taskArns'][1], 
            error='Price analysis failed'
        )
        
        assert response['statusCode'] == 500
        assert 'error' in response['body']