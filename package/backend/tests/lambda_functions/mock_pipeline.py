"""
Mock pipeline for testing the complete scraping workflow.
"""

import boto3
import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

class MockScraperPipeline:
    """Mock pipeline for testing the complete scraping workflow"""
    
    def __init__(self):
        """Initialize mock pipeline"""
        self.s3 = boto3.client('s3')
        self.lambda_client = boto3.client('lambda')
        self.dynamodb = boto3.client('dynamodb')
        self.sns = boto3.client('sns')
        self.ecs = boto3.client('ecs')
        self.iam = boto3.client('iam')
        
    def setup_mocks(self):
        """Setup mock AWS resources"""
        # Create S3 bucket
        self.s3.create_bucket(Bucket='scraper-data-bucket')
        
        # Create DynamoDB table
        self.dynamodb.create_table(
            TableName='scraper-results',
            KeySchema=[
                {'AttributeName': 'query_hash', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'query_hash', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        # Create SNS topic
        self.sns.create_topic(Name='scraper-notifications')
        
        # Create IAM role with proper trust policy
        assume_role_policy = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'lambda.amazonaws.com'
                    },
                    'Action': 'sts:AssumeRole'
                }
            ]
        }
        
        # Create the role first
        self.iam.create_role(
            RoleName='lambda-role',
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        
        # Attach basic Lambda execution policy
        lambda_policy = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Action': [
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                        'logs:PutLogEvents'
                    ],
                    'Resource': 'arn:aws:logs:*:*:*'
                }
            ]
        }
        
        self.iam.put_role_policy(
            RoleName='lambda-role',
            PolicyName='lambda-basic-policy',
            PolicyDocument=json.dumps(lambda_policy)
        )
        
        # Create Lambda function with the role
        self.lambda_client.create_function(
            FunctionName='scraper-worker',
            Runtime='python3.10',
            Handler='index.handler',
            Role='arn:aws:iam::123456789012:role/lambda-role',
            Code={'ZipFile': b''}
        )
        
        # Create ECS cluster
        self.ecs.create_cluster(clusterName='scraper-cluster')
        
        # Create container definition for the scraper task
        container_def = {
            'name': 'scraper-container',
            'image': 'scraper:latest',
            'cpu': 256,
            'memory': 512,
            'essential': True,
            'environment': [
                {'name': 'AWS_REGION', 'value': 'us-east-1'},
                {'name': 'S3_BUCKET', 'value': 'scraper-data-bucket'}
            ],
            'logConfiguration': {
                'logDriver': 'awslogs',
                'options': {
                    'awslogs-group': '/ecs/scraper-task',
                    'awslogs-region': 'us-east-1',
                    'awslogs-stream-prefix': 'ecs'
                }
            }
        }
        
        # Register task definition with container
        self.ecs.register_task_definition(
            family='scraper-task',
            networkMode='awsvpc',
            requiresCompatibilities=['FARGATE'],
            cpu='256',
            memory='512',
            executionRoleArn='arn:aws:iam::123456789012:role/ecs-task-execution-role',
            containerDefinitions=[container_def]
        )
        
        # Mock datetime for consistent query hash generation
        self.datetime_patcher = patch('backend.aws.lambda_functions.form_submission.form_handler.datetime')
        self.mock_datetime = self.datetime_patcher.start()
        self.mock_datetime.now.return_value = datetime(2024, 1, 15)
        
    def cleanup(self):
        """Cleanup mock resources"""
        # Stop datetime mock
        self.datetime_patcher.stop()
        
        # Delete all objects in S3 bucket
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket='scraper-data-bucket'):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        self.s3.delete_object(Bucket='scraper-data-bucket', Key=obj['Key'])
        except self.s3.exceptions.NoSuchBucket:
            pass
            
        # Delete S3 bucket
        try:
            self.s3.delete_bucket(Bucket='scraper-data-bucket')
        except self.s3.exceptions.NoSuchBucket:
            pass
        
        # Delete DynamoDB table
        try:
            self.dynamodb.delete_table(TableName='scraper-results')
        except self.dynamodb.exceptions.ResourceNotFoundException:
            pass
        
        # Delete SNS topic
        try:
            topics = self.sns.list_topics()
            for topic in topics['Topics']:
                self.sns.delete_topic(TopicArn=topic['TopicArn'])
        except self.sns.exceptions.InvalidParameterException:
            pass
        
        # Delete Lambda function
        try:
            self.lambda_client.delete_function(FunctionName='scraper-worker')
        except self.lambda_client.exceptions.ResourceNotFoundException:
            pass
        
        # Delete IAM role policies first
        try:
            # List and delete inline policies
            policies = self.iam.list_role_policies(RoleName='lambda-role')
            for policy_name in policies['PolicyNames']:
                self.iam.delete_role_policy(RoleName='lambda-role', PolicyName=policy_name)
            
            # List and detach managed policies
            attached_policies = self.iam.list_attached_role_policies(RoleName='lambda-role')
            for policy in attached_policies['AttachedPolicies']:
                self.iam.detach_role_policy(
                    RoleName='lambda-role',
                    PolicyArn=policy['PolicyArn']
                )
        except self.iam.exceptions.NoSuchEntityException:
            pass
        
        # Delete IAM role after policies are removed
        try:
            self.iam.delete_role(RoleName='lambda-role')
        except self.iam.exceptions.NoSuchEntityException:
            pass
        
        # Delete ECS cluster
        try:
            self.ecs.delete_cluster(cluster='scraper-cluster')
        except self.ecs.exceptions.ClusterNotFoundException:
            pass
        
    def store_scraper_data(self, query_hash, data):
        """Store mock scraper data in S3"""
        def convert_floats(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats(item) for item in obj]
            return obj
            
        converted_data = convert_floats(data)
        self.s3.put_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/scraper-results.json',
            Body=json.dumps(converted_data, cls=DecimalEncoder)
        )
        
    def store_analysis_data(self, query_hash, data):
        """Store mock analysis data in S3"""
        def convert_floats(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats(item) for item in obj]
            return obj
            
        converted_data = convert_floats(data)
        self.s3.put_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/analysis-results.json',
            Body=json.dumps(converted_data, cls=DecimalEncoder)
        )
        
    def store_report_data(self, query_hash, data):
        """Store mock report data in S3"""
        def convert_floats(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats(item) for item in obj]
            return obj
            
        converted_data = convert_floats(data)
        self.s3.put_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/report-results.json',
            Body=json.dumps(converted_data, cls=DecimalEncoder)
        )
        
    def store_database_entry(self, query_hash, data):
        """Store mock database entry in DynamoDB"""
        def convert_floats(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats(item) for item in obj]
            return obj
            
        converted_data = convert_floats(data)
        timestamp = self.mock_datetime.now().isoformat()  # Use mocked datetime
        
        # Convert the data to DynamoDB format
        item = {
            'query_hash': {'S': query_hash},
            'timestamp': {'S': timestamp}
        }
        
        # Add all other fields with appropriate types
        for key, value in converted_data.items():
            if isinstance(value, str):
                item[key] = {'S': value}
            elif isinstance(value, (int, float, Decimal)):
                item[key] = {'N': str(value)}
            elif isinstance(value, list):
                item[key] = {'L': [{'S': str(v)} for v in value]}
            elif isinstance(value, dict):
                item[key] = {'M': {k: {'S': str(v)} for k, v in value.items()}}
        
        self.dynamodb.put_item(
            TableName='scraper-results',
            Item=item
        )
        
    def send_notification(self, message):
        """Send mock notification via SNS"""
        self.sns.publish(
            TopicArn='arn:aws:sns:us-east-1:123456789012:scraper-notifications',
            Message=json.dumps(message, cls=DecimalEncoder)
        )