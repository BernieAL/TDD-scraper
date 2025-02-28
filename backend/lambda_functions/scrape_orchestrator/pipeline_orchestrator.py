import boto3
import json
import os
from datetime import datetime

def orchestrate_scraping_pipeline(event, context):
    """
    Main Lambda handler that orchestrates the complete scraping pipeline:
    1. Launches scraper containers
    2. Triggers price analysis
    3. Generates reports
    """
    try:
        ecs = boto3.client('ecs')
        s3 = boto3.client('s3')

        s3.put_object(
            Bucket='scraper-data-bucket'
            Key=f'queries/{event["query_hash"]}/params.json',
            Body=json.dumps(event)
        )


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