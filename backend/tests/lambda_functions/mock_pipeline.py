import boto3
from moto import mock_ecs, mock_s3
from unittest.mock import patch



"""
this is class where to define the mock pipeline for scrape operations

we set up mock resources:
    ecs cluster to run tasks in
    s3 bucket to store scrape results
    register the task defintions with the cluster 
        -this is us saying, "hey these are the tasks we want to run"
        -for each task defintion, define container compute resources to be used

    then we define 2 functions to mock task completion
        -1 function is to mock response of successful task completion
        -1 function is to mock resposne of failed task completion
"""

class MockScraperPipeline:
    def __init__(self):
        self.ecs = boto3.client('ecs')
        self.s3 = boto3.client('s3')

    def setup_mocks(self):
        # Create mock resources
        self.ecs.create_cluster(clusterName='scraper-cluster')
        self.s3.create_bucket(Bucket='scraper-results')
    
        # Register all task definitions
        self._register_task_definitions()

    def _register_task_definitions(self):
        """Register mock task definitions for each pipeline stage"""
        tasks = [
            ('scraper-task', 'Scraping products'),
            ('analysis-task', 'Analyzing prices'),
            ('report-task', 'Generating reports')
        ]

        for task_name, container_name in tasks:
            self.ecs.register_task_definition(
                family=task_name,
                containerDefinitions=[{
                    'name': container_name,
                    'image': f'{task_name}:latest',
                    'cpu': 256,
                    'memory': 512,
                    'essential': True
                }]
            )
    def mock_successful_task(self, task_arn):
        """Simulate successful task completion"""
        self.ecs.update_task(
            task=task_arn,
            status='STOPPED',
            containers=[{
                'exitCode': 0,
                'lastStatus': 'STOPPED'
            }]
        )

    def mock_failed_task(self, task_arn, error='Task failed'):
        """Simulate task failure"""
        self.ecs.update_task(
            task=task_arn,
            status='STOPPED',
            containers=[{
                'exitCode': 1,
                'lastStatus': 'STOPPED',
                'reason': error
            }]
        )