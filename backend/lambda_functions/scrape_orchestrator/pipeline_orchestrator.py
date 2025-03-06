import boto3
import json
import os
from datetime import datetime
import hashlib


def orchestrate_scraping_pipeline(event, context):
    """
    Main Lambda handler that orchestrates the complete scraping pipeline
    """
    try:
        ecs = boto3.client('ecs')
        s3 = boto3.client('s3')
        query_hash  = event['query_hash']
        # Get latest form submission from S3
     
        #get from data for this specific query
        form_data = s3.get_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/form-params.json'
        )

        params = json.loads(form_data['Body'].read())

        # Define S3 paths for this query
        paths = {
            'raw': f'queries/{query_hash}/raw',
            'filtered': f'queries/{query_hash}/filtered',
            'analysis': f'queries/{query_hash}/analysis',
            'reports': f'queries/{query_hash}/reports'
        }

        #path creation 
        # Launch scraper task first
        scraper_response = launch_scraper_task(ecs)
        
        # Launch analysis task after scraper
        analysis_response = launch_analysis_task(ecs)
        
        # Launch report generation task last
        report_response = launch_report_task(ecs)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Pipeline launched successfully',
                'query_hash': query_hash,
                'scraper_task': scraper_response['tasks'][0]['taskArn'],
                'analysis_task': analysis_response['tasks'][0]['taskArn'],
                'report_task': report_response['tasks'][0]['taskArn'],
                'timestamp': datetime.utcnow().isoformat()
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

def launch_scraper_task(ecs):
    return ecs.run_task(
        cluster='scraper-cluster',
        taskDefinition='scraper-task',
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [os.environ.get('SUBNET_ID', 'dummy-subnet')],
                'securityGroups': [os.environ.get('SECURITY_GROUP_ID', 'dummy-sg')],
                'assignPublicIp': 'ENABLED'
            }
        }
    )

def launch_analysis_task(ecs):
    return ecs.run_task(
        cluster='scraper-cluster',
        taskDefinition='analysis-task',
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [os.environ.get('SUBNET_ID', 'dummy-subnet')],
                'securityGroups': [os.environ.get('SECURITY_GROUP_ID', 'dummy-sg')],
                'assignPublicIp': 'ENABLED'
            }
        }
    )

def launch_report_task(ecs):
    return ecs.run_task(
        cluster='scraper-cluster',
        taskDefinition='report-task',
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [os.environ.get('SUBNET_ID', 'dummy-subnet')],
                'securityGroups': [os.environ.get('SECURITY_GROUP_ID', 'dummy-sg')],
                'assignPublicIp': 'ENABLED'
            }
        }
    ) 