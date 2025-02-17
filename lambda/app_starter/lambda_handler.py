import boto3
import json
import os
from datetime import datetime

ecs = boto3.client('ecs')
dynamodb = boto3.client('dynamodb')

def start_app_components(event, context):
    """Start the app components if not already running"""
    try:
        # Extract user info from event
        body = json.loads(event.get('body', '{}'))
        user_id = body.get('user_id')
        
        # Check if app is already running
        response = dynamodb.get_item(
            TableName='app_status',
            Key={'id': {'S': 'app_status'}}
        )
        
        if 'Item' in response:
            active_users = int(response['Item']['active_users']['N'])
            if active_users > 0:
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'App already running',
                        'status': 'active'
                    })
                }
        
        # Start ECS tasks
        response = ecs.run_task(
            cluster='price-tracker',
            taskDefinition='price-tracker-workers',
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [os.environ['SUBNET_ID']],
                    'securityGroups': [os.environ['SECURITY_GROUP_ID']],
                    'assignPublicIp': 'ENABLED'
                }
            }
        )
        
        # Update app status in DynamoDB
        dynamodb.put_item(
            TableName='app_status',
            Item={
                'id': {'S': 'app_status'},
                'last_access': {'S': datetime.now().isoformat()},
                'active_users': {'N': '1'},
                'last_user': {'S': user_id}
            }
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
            },
            'body': json.dumps({
                'message': 'App starting',
                'taskArn': response['tasks'][0]['taskArn'],
                'status': 'starting'
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'status': 'error'
            })
        } 