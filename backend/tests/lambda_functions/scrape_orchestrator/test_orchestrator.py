"""
Tests for the scrape orchestrator Lambda function.
Tests:
1. Event handling and validation
2. S3 path management
3. Task launching
4. Error handling
5. Local vs AWS execution
6. Invalid form data handling
7. ECS task failure handling
"""

import pytest
import json
import boto3
from datetime import datetime
from unittest.mock import patch, MagicMock
from moto import mock_s3, mock_lambda, mock_ecs, mock_iam

@pytest.fixture
def sample_event():
    """Sample event that the orchestrator would receive"""
    return {
        "query_hash": "test123",
        "brand": "PRADA",
        "category": "BAGS",
        "paths": {
            "raw": "raw",
            "filtered": "filtered"
        }
    }

@pytest.fixture
def mock_s3_bucket():
    """Create and return a mock S3 bucket"""
    with mock_s3():
        s3 = boto3.client('s3')
        bucket_name = 'scraper-data-bucket'
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name

@pytest.fixture
def mock_lambda_function():
    """Create and return a mock Lambda function"""
    with mock_lambda(), mock_iam():
        # Create IAM role first
        iam = boto3.client('iam')
        role_name = 'lambda-role'
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        )
        
        # Create Lambda function
        lambda_client = boto3.client('lambda')
        function_name = 'scraper-worker'
        lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.10',
            Handler='lambda_handler',
            Role=f'arn:aws:iam::123456789012:role/{role_name}',
            Code={'ZipFile': b'empty'}
        )
        yield function_name

@pytest.fixture
def mock_ecs_cluster():
    """Create and return a mock ECS cluster"""
    with mock_ecs():
        ecs = boto3.client('ecs')
        cluster_name = 'scraper-cluster'
        ecs.create_cluster(clusterName=cluster_name)
        yield cluster_name

@pytest.fixture
def form_data():
    """Sample form data"""
    return {
        "brand": "PRADA",
        "category": "BAGS",
        "min_price": 1000,
        "max_price": 2000
    }

def test_event_validation(sample_event, form_data):
    """Test event validation"""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(form_data).encode('utf-8')
            )
        }
        # Mock ECS client
        mock_ecs_client = MagicMock()
        mock_ecs_client.run_task.return_value = {
            'tasks': [{
                'taskArn': 'arn:aws:ecs:region:account:task/task-id',
                'lastStatus': 'PENDING'
            }]
        }
        mock_boto3.side_effect = [mock_s3_client, mock_ecs_client]
        
        # Test valid event
        response = orchestrate_scraping_pipeline(sample_event, None)
        assert response['statusCode'] == 200
        
        # Test missing required fields
        invalid_event = sample_event.copy()
        del invalid_event['query_hash']
        response = orchestrate_scraping_pipeline(invalid_event, None)
        assert response['statusCode'] == 400

def test_s3_path_management(sample_event, mock_s3_bucket, form_data):
    """Test S3 path management"""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(form_data).encode('utf-8')
            )
        }
        # Mock ECS client
        mock_ecs_client = MagicMock()
        mock_ecs_client.run_task.return_value = {
            'tasks': [{
                'taskArn': 'arn:aws:ecs:region:account:task/task-id',
                'lastStatus': 'PENDING'
            }]
        }
        mock_boto3.side_effect = [mock_s3_client, mock_ecs_client]
        
        # Run orchestrator
        response = orchestrate_scraping_pipeline(sample_event, None)
        assert response['statusCode'] == 200
        
        # Verify S3 paths were stored
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args[1]
        assert call_args['Bucket'] == mock_s3_bucket
        assert 'paths.json' in call_args['Key']

def test_task_launching(sample_event, mock_lambda_function, mock_ecs_cluster, form_data):
    """Test task launching"""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(form_data).encode('utf-8')
            )
        }
        # Mock ECS client
        mock_ecs_client = MagicMock()
        mock_ecs_client.run_task.return_value = {
            'tasks': [{
                'taskArn': 'arn:aws:ecs:region:account:task/task-id',
                'lastStatus': 'PENDING'
            }]
        }
        mock_boto3.side_effect = [mock_s3_client, mock_ecs_client]
        
        # Run orchestrator
        response = orchestrate_scraping_pipeline(sample_event, None)
        assert response['statusCode'] == 200
        
        # Verify ECS task was launched
        mock_ecs_client.run_task.assert_called_once()
        call_args = mock_ecs_client.run_task.call_args[1]
        assert call_args['cluster'] == mock_ecs_cluster

def test_error_handling(sample_event, form_data):
    """Test error handling"""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(form_data).encode('utf-8')
            )
        }
        mock_s3_client.put_object.side_effect = Exception("S3 Error")
        mock_boto3.return_value = mock_s3_client
        
        # Run orchestrator
        response = orchestrate_scraping_pipeline(sample_event, None)
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body

def test_local_execution(sample_event, form_data):
    """Test local execution mode"""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    # Add local execution flag
    local_event = sample_event.copy()
    local_event['local'] = True
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(form_data).encode('utf-8')
            )
        }
        # Mock ECS client
        mock_ecs_client = MagicMock()
        mock_ecs_client.run_task.return_value = {
            'tasks': [{
                'taskArn': 'arn:aws:ecs:region:account:task/task-id',
                'lastStatus': 'PENDING'
            }]
        }
        mock_boto3.side_effect = [mock_s3_client, mock_ecs_client]
        
        # Run orchestrator
        response = orchestrate_scraping_pipeline(local_event, None)
        assert response['statusCode'] == 200
        
        # Verify local paths were used
        body = json.loads(response['body'])
        assert 'local_paths' in body
        
        # Verify ECS task was not launched in local mode
        mock_ecs_client.run_task.assert_not_called()

def test_invalid_form_data(sample_event):
    """Test handling of invalid form data"""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client with invalid form data
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps({
                    "invalid": "data"
                }).encode('utf-8')
            )
        }
        mock_boto3.return_value = mock_s3_client
        
        # Run orchestrator
        response = orchestrate_scraping_pipeline(sample_event, None)
        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'error' in body

def test_ecs_task_failure(sample_event, form_data):
    """Test handling of ECS task failure"""
    from backend.aws.lambda_functions.scrape_orchestrator.pipeline_orchestrator import orchestrate_scraping_pipeline
    
    with patch('boto3.client') as mock_boto3:
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(
                read=lambda: json.dumps(form_data).encode('utf-8')
            )
        }
        # Mock ECS client with failed task
        mock_ecs_client = MagicMock()
        mock_ecs_client.run_task.return_value = {
            'tasks': [{
                'taskArn': 'arn:aws:ecs:region:account:task/task-id',
                'lastStatus': 'STOPPED',
                'stoppedReason': 'Task failed'
            }]
        }
        mock_boto3.side_effect = [mock_s3_client, mock_ecs_client]
        
        # Run orchestrator
        response = orchestrate_scraping_pipeline(sample_event, None)
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'error' in body 