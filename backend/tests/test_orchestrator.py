"""

this file is to set up mock cluster and ecs task definitions
to be used in the mock pipeline (mock_pipeline.py)

we're mocking creating cluster and ecs task defintions
mocking event trigger - which is search form data rec'd
search form data is 'sent' into orchestrating_scraping_pipeline

"""


import boto3
import pytest
from moto import mock_ecs, mock_s3
import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backend.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline


@mock_ecs
@mock_s3
def test_scraping_pipeline():
    #set up mock ecs cluster and task definitions
    ecs = boto3.client('ecs')

    #create mock cluster
    ecs.create_cluster(clusterName='scraper-cluster')

    #register mock task defintions
    ecs.register_task_definition(
        family='scraper-task',
        containerDefinitions=[{
            'name': 'scraper',
            'image': 'scraper:latest',
            'cpu': 256,
            'memory': 512,
            'essential': True
        }]
    )

    # Mock event input - 
    event = {
        'query_hash': '123abc',
        'brand': 'prada',
        'category': 'bags'
    }

    #Test pipeline orchestration
    response = orchestrate_scraping_pipeline(event, None)
    
    # Verify task was "launched"
    tasks = ecs.list_tasks(cluster='scraper-cluster')
    assert len(tasks['taskArns']) > 0

