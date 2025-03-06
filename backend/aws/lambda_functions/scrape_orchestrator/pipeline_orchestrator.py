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
        query_hash = event['query_hash']

        # Get form data
        form_data = s3.get_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/form-params.json'
        )
        params = json.loads(form_data['Body'].read())

        # Define paths for this query_hash and store in s3
        paths = {
            'raw': f'queries/{query_hash}/raw',
            'filtered': f'queries/{query_hash}/filtered',
            'analysis': f'queries/{query_hash}/analysis',
            'reports': f'queries/{query_hash}/reports'
        }

        # Store paths in S3 for referecnce and to be used by tasks 
        s3.put_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/paths.json',
            Body=json.dumps(paths)
        )

        # Launch tasks with paths in environment
        scraper_response = launch_scraper_task(ecs, paths, query_hash)
        wait_for_task(scraper_response['tasks'][0]['taskArn'])

        analysis_response = launch_analysis_task(ecs, paths, query_hash)
        wait_for_task(analysis_response['tasks'][0]['taskArn'])

        report_response = launch_report_task(ecs, paths, query_hash)
        
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

def launch_scraper_task(ecs, paths, query_hash):
    return ecs.run_task(
        cluster='scraper-cluster',
        taskDefinition='scraper-task',
        launchType='FARGATE',
        overrides={
            'containerOverrides': [{
                'name': 'scraper',
                'environment': [
                    {'name': 'QUERY_HASH', 'value': query_hash},
                    {'name': 'RAW_PATH', 'value': paths['raw']},
                    {'name': 'FILTERED_PATH', 'value': paths['filtered']},
                    {'name': 'S3_BUCKET', 'value': 'scraper-data-bucket'}
                ]
            }]
        },
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [os.environ.get('SUBNET_ID', 'dummy-subnet')],
                'securityGroups': [os.environ.get('SECURITY_GROUP_ID', 'dummy-sg')],
                'assignPublicIp': 'ENABLED'
            }
        }
    )

def launch_analysis_task(ecs, paths, query_hash):
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
        },
        environment=[
            {'name': 'QUERY_HASH', 'value': query_hash},
            {'name': 'RAW_PATH', 'value': paths['raw']},
            {'name': 'FILTERED_PATH', 'value': paths['filtered']},
            {'name': 'ANALYSIS_PATH', 'value': paths['analysis']},
            {'name': 'REPORTS_PATH', 'value': paths['reports']}
        ]
    )

def launch_report_task(ecs, paths, query_hash):
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
        },
        environment=[
            {'name': 'QUERY_HASH', 'value': query_hash},
            {'name': 'RAW_PATH', 'value': paths['raw']},
            {'name': 'FILTERED_PATH', 'value': paths['filtered']},
            {'name': 'ANALYSIS_PATH', 'value': paths['analysis']},
            {'name': 'REPORTS_PATH', 'value': paths['reports']}
        ]
    )

def wait_for_task(task_arn):
    # Implement the logic to wait for a task to complete
    pass 