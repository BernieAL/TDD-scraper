"""
Tests for the complete scraping pipeline workflow.
Tests:
1. Complete pipeline success flow
2. Pipeline failure handling
3. Data flow through pipeline stages
4. Individual task success/failure
5. Database operations
6. SNS notifications
7. Data validation
8. Data transformation
"""

import pytest
import json
import boto3
from datetime import datetime
from unittest.mock import patch
from moto import mock_ecs, mock_s3, mock_lambda, mock_dynamodb, mock_sns, mock_iam

from backend.aws.lambda_functions.form_submission.form_handler import handle_form_submit
from backend.tests.lambda_functions.mock_pipeline import MockScraperPipeline

@pytest.fixture
def sample_form_data():
    """Sample form submission data"""
    return {
        'brand': 'PRADA',
        'category': 'BAGS',
        'specific_item': 'TOTE',
        'user_email': 'test@example.com'
    }

@pytest.fixture
def mock_scraper_data():
    """Sample scraper results data"""
    return {
        'products': [
            {
                'name': 'Prada Tote Bag',
                'price': 2500.0,
                'source': 'ITALIST',
                'url': 'https://example.com/prada-tote'
            }
        ]
    }

@pytest.fixture
def mock_dynamodb_table():
    """Create and return a mock DynamoDB table"""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.create_table(
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
    return table

@pytest.fixture
def mock_sns_topic():
    """Create and return a mock SNS topic ARN"""
    sns = boto3.client('sns')
    response = sns.create_topic(Name='scraper-notifications')
    return response['TopicArn']

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_complete_pipeline_success(sample_form_data, mock_scraper_data):
    """Test complete pipeline success flow"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Submit form
            form_response = handle_form_submit(sample_form_data, None)
            assert form_response['statusCode'] == 200
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Store analysis data
            analysis_data = {
                'price_stats': {
                    'min': 2500.0,
                    'max': 2500.0,
                    'avg': 2500.0
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            
            # Store report data
            report_data = {
                'summary': 'Found 1 Prada tote bag',
                'recommendations': ['Good price point']
            }
            mock_pipeline.store_report_data(query_hash, report_data)
            
            # Store in database
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'completed',
                'product_count': 1,
                'price_range': '2500.0-2500.0'
            })
            
            # Verify final state
            s3 = boto3.client('s3')
            stored_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/report.json'
            )['Body'].read())
            
            assert stored_data['summary'] == 'Found 1 Prada tote bag'
            assert stored_data['recommendations'] == ['Good price point']
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_pipeline_failure_handling(sample_form_data):
    """Test pipeline failure handling"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Submit form
            form_response = handle_form_submit(sample_form_data, None)
            assert form_response['statusCode'] == 200
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Simulate scraper failure
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'failed',
                'error': 'Scraper failed to connect'
            })
            
            # Send failure notification
            mock_pipeline.send_notification({
                'type': 'error',
                'query_hash': query_hash,
                'message': 'Scraper failed to connect'
            })
            
            # Verify error state
            s3 = boto3.client('s3')
            stored_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/form-params.json'
            )['Body'].read())
            
            assert stored_data['brand'] == 'PRADA'
            assert stored_data['category'] == 'BAGS'
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_pipeline_data_flow(sample_form_data, mock_scraper_data):
    """Test data flow through pipeline stages"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Submit form
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Stage 1: Scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Stage 2: Analysis data
            s3 = boto3.client('s3')
            scraper_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/scraper-results.json'
            )['Body'].read())
            
            # Convert string prices to float for calculations
            prices = [float(product['price']) for product in scraper_data['products']]
            analysis_data = {
                'price_stats': {
                    'min': min(prices),
                    'max': max(prices),
                    'avg': sum(prices) / len(prices)
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            
            # Stage 3: Report data
            report_data = {
                'summary': 'Found 1 Prada tote bag',
                'recommendations': ['Good price point']
            }
            mock_pipeline.store_report_data(query_hash, report_data)
            
            # Verify data flow
            s3 = boto3.client('s3')
            
            # Check scraper data
            scraper_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/scraper-results.json'
            )['Body'].read())
            assert scraper_data['products'][0]['name'] == 'Prada Tote Bag'
            
            # Check analysis data
            analysis_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/analysis-results.json'
            )['Body'].read())
            assert analysis_data['price_stats']['avg'] == 2500.0
            
            # Check report data
            report_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/report.json'
            )['Body'].read())
            assert report_data['summary'] == 'Found 1 Prada tote bag'
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_scraper_task_success(sample_form_data, mock_scraper_data):
    """Test successful scraper task execution"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Store task status
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'scraping_completed',
                'product_count': 1,
                'sources': ['ITALIST']
            })
            
            # Verify scraper results
            s3 = boto3.client('s3')
            stored_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/scraper-results.json'
            )['Body'].read())
            
            assert len(stored_data['products']) == 1
            assert stored_data['products'][0]['name'] == 'Prada Tote Bag'
            assert stored_data['products'][0]['price'] == 2500.0
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_scraper_task_failure(sample_form_data):
    """Test scraper task failure handling"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store failure status
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'scraping_failed',
                'error': 'Connection timeout'
            })
            
            # Send failure notification
            mock_pipeline.send_notification({
                'type': 'error',
                'stage': 'scraping',
                'query_hash': query_hash,
                'message': 'Connection timeout'
            })
            
            # Verify error state
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('scraper-results')
            response = table.get_item(
                Key={
                    'query_hash': query_hash,
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            assert response['Item']['status'] == 'scraping_failed'
            assert response['Item']['error'] == 'Connection timeout'
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_analysis_task_success(sample_form_data, mock_scraper_data):
    """Test successful analysis task execution"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline with successful scraper task
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Store analysis data
            analysis_data = {
                'price_stats': {
                    'min': 2500.0,
                    'max': 2500.0,
                    'avg': 2500.0,
                    'std_dev': 0.0
                },
                'price_trends': {
                    'direction': 'stable',
                    'volatility': 'low'
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            
            # Store task status
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'analysis_completed',
                'price_stats': analysis_data['price_stats'],
                'price_trends': analysis_data['price_trends']
            })
            
            # Verify analysis results
            s3 = boto3.client('s3')
            stored_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/analysis-results.json'
            )['Body'].read())
            
            assert stored_data['price_stats']['avg'] == 2500.0
            assert stored_data['price_trends']['direction'] == 'stable'
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_analysis_task_failure(sample_form_data, mock_scraper_data):
    """Test analysis task failure handling"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline with successful scraper task
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Store failure status
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'analysis_failed',
                'error': 'Insufficient data for analysis'
            })
            
            # Send failure notification
            mock_pipeline.send_notification({
                'type': 'error',
                'stage': 'analysis',
                'query_hash': query_hash,
                'message': 'Insufficient data for analysis'
            })
            
            # Verify error state
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('scraper-results')
            response = table.get_item(
                Key={
                    'query_hash': query_hash,
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            assert response['Item']['status'] == 'analysis_failed'
            assert response['Item']['error'] == 'Insufficient data for analysis'
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_report_task_success(sample_form_data, mock_scraper_data):
    """Test successful report task execution"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline with successful previous tasks
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Store analysis data
            analysis_data = {
                'price_stats': {
                    'min': 2500.0,
                    'max': 2500.0,
                    'avg': 2500.0
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            
            # Store report data
            report_data = {
                'summary': 'Found 1 Prada tote bag',
                'price_analysis': {
                    'current_avg': 2500.0,
                    'price_range': '2500.0-2500.0'
                },
                'recommendations': [
                    'Good price point',
                    'Limited availability'
                ]
            }
            mock_pipeline.store_report_data(query_hash, report_data)
            
            # Store task status
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'report_completed',
                'summary': report_data['summary'],
                'recommendations': report_data['recommendations']
            })
            
            # Send success notification
            mock_pipeline.send_notification({
                'type': 'success',
                'stage': 'report',
                'query_hash': query_hash,
                'message': 'Report generated successfully'
            })
            
            # Verify report results
            s3 = boto3.client('s3')
            stored_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/report.json'
            )['Body'].read())
            
            assert stored_data['summary'] == 'Found 1 Prada tote bag'
            assert stored_data['price_analysis']['current_avg'] == 2500.0
            assert len(stored_data['recommendations']) == 2
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_report_task_failure(sample_form_data, mock_scraper_data):
    """Test report task failure handling"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline with successful previous tasks
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Store analysis data
            analysis_data = {
                'price_stats': {
                    'min': 2500.0,
                    'max': 2500.0,
                    'avg': 2500.0
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            
            # Store failure status
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'report_failed',
                'error': 'Failed to generate report'
            })
            
            # Send failure notification
            mock_pipeline.send_notification({
                'type': 'error',
                'stage': 'report',
                'query_hash': query_hash,
                'message': 'Failed to generate report'
            })
            
            # Verify error state
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('scraper-results')
            response = table.get_item(
                Key={
                    'query_hash': query_hash,
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            assert response['Item']['status'] == 'report_failed'
            assert response['Item']['error'] == 'Failed to generate report'
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_database_operations(sample_form_data, mock_scraper_data, mock_dynamodb_table):
    """Test database operations throughout the pipeline"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store initial state
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'started',
                'brand': 'PRADA',
                'category': 'BAGS'
            })
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'scraping_completed',
                'product_count': 1
            })
            
            # Store analysis data
            analysis_data = {
                'price_stats': {
                    'min': 2500.0,
                    'max': 2500.0,
                    'avg': 2500.0
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'analysis_completed',
                'price_stats': analysis_data['price_stats']
            })
            
            # Store final state
            mock_pipeline.store_database_entry(query_hash, {
                'status': 'completed',
                'summary': 'Pipeline completed successfully'
            })
            
            # Verify database entries
            table = mock_dynamodb_table
            response = table.scan()
            items = response['Items']
            
            assert len(items) > 0
            statuses = [item['status'] for item in items]
            assert 'started' in statuses
            assert 'scraping_completed' in statuses
            assert 'analysis_completed' in statuses
            assert 'completed' in statuses
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_sns_notifications(sample_form_data, mock_sns_topic):
    """Test SNS notifications throughout the pipeline"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Send start notification
            mock_pipeline.send_notification({
                'type': 'info',
                'stage': 'start',
                'query_hash': query_hash,
                'message': 'Pipeline started'
            })
            
            # Send progress notification
            mock_pipeline.send_notification({
                'type': 'info',
                'stage': 'scraping',
                'query_hash': query_hash,
                'message': 'Scraping completed'
            })
            
            # Send completion notification
            mock_pipeline.send_notification({
                'type': 'success',
                'stage': 'complete',
                'query_hash': query_hash,
                'message': 'Pipeline completed'
            })
            
            # Verify notifications
            sns = boto3.client('sns')
            response = sns.list_subscriptions_by_topic(TopicArn=mock_sns_topic)
            assert len(response['Subscriptions']) > 0
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_data_validation(sample_form_data, mock_scraper_data):
    """Test data validation between pipeline stages"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Validate scraper data
            s3 = boto3.client('s3')
            scraper_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/scraper-results.json'
            )['Body'].read())
            
            assert 'products' in scraper_data
            assert len(scraper_data['products']) > 0
            assert all(
                all(key in product for key in ['name', 'price', 'source', 'url'])
                for product in scraper_data['products']
            )
            
            # Store analysis data
            analysis_data = {
                'price_stats': {
                    'min': 2500.0,
                    'max': 2500.0,
                    'avg': 2500.0
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            
            # Validate analysis data
            analysis_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/analysis-results.json'
            )['Body'].read())
            
            assert 'price_stats' in analysis_data
            assert all(
                key in analysis_data['price_stats']
                for key in ['min', 'max', 'avg']
            )
            
            # Store report data
            report_data = {
                'summary': 'Found 1 Prada tote bag',
                'recommendations': ['Good price point']
            }
            mock_pipeline.store_report_data(query_hash, report_data)
            
            # Validate report data
            report_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/report.json'
            )['Body'].read())
            
            assert 'summary' in report_data
            assert 'recommendations' in report_data
            assert isinstance(report_data['recommendations'], list)
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_data_transformation(sample_form_data, mock_scraper_data):
    """Test data transformation between pipeline stages"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        with patch('time.sleep'):
            # Setup pipeline
            form_response = handle_form_submit(sample_form_data, None)
            query_hash = json.loads(form_response['body'])['query_hash']
            
            # Store scraper data
            mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
            
            # Transform scraper data to analysis format
            s3 = boto3.client('s3')
            scraper_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/scraper-results.json'
            )['Body'].read())
            
            # Convert string prices to float for calculations
            prices = [float(product['price']) for product in scraper_data['products']]
            analysis_data = {
                'price_stats': {
                    'min': min(prices),
                    'max': max(prices),
                    'avg': sum(prices) / len(prices)
                }
            }
            mock_pipeline.store_analysis_data(query_hash, analysis_data)
            
            # Transform analysis data to report format
            analysis_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/analysis-results.json'
            )['Body'].read())
            
            # Convert string prices to float for comparison
            avg_price = float(analysis_data['price_stats']['avg'])
            min_price = float(analysis_data['price_stats']['min'])
            max_price = float(analysis_data['price_stats']['max'])
            
            report_data = {
                'query_hash': query_hash,
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_products': len(scraper_data['products']),
                    'current_avg': avg_price,
                    'price_range': f"{min_price}-{max_price}"
                },
                'recommendations': [
                    'Good price point' if avg_price < 3000 else 'Price is high',
                    'Limited availability' if len(scraper_data['products']) < 5 else 'Good availability'
                ]
            }
            mock_pipeline.store_report_data(query_hash, report_data)
            
            # Verify transformations
            report_data = json.loads(s3.get_object(
                Bucket='scraper-data-bucket',
                Key=f'queries/{query_hash}/report.json'
            )['Body'].read())
            
            assert report_data['summary']['total_products'] == len(scraper_data['products'])
            assert report_data['summary']['current_avg'] == avg_price
            assert report_data['summary']['price_range'] == f"{min_price}-{max_price}"
            assert len(report_data['recommendations']) == 2
            
    finally:
        mock_pipeline.cleanup()

@mock_ecs
@mock_s3
@mock_lambda
@mock_dynamodb
@mock_sns
@mock_iam
def test_basic_pipeline_setup(sample_form_data, mock_scraper_data):
    """Test basic pipeline setup and data flow"""
    mock_pipeline = MockScraperPipeline()
    mock_pipeline.setup_mocks()
    
    try:
        # Submit form
        form_response = handle_form_submit(sample_form_data, None)
        assert form_response['statusCode'] == 200
        query_hash = json.loads(form_response['body'])['query_hash']
        
        # Store initial data
        mock_pipeline.store_scraper_data(query_hash, mock_scraper_data)
        
        # Verify data was stored
        s3 = boto3.client('s3')
        stored_data = json.loads(s3.get_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/scraper-results.json'
        )['Body'].read())
        
        assert len(stored_data['products']) == 1
        assert stored_data['products'][0]['name'] == 'Prada Tote Bag'
        assert float(stored_data['products'][0]['price']) == 2500.0
        
        # Store analysis data
        analysis_data = {
            'price_stats': {
                'min': 2500.0,
                'max': 2500.0,
                'avg': 2500.0
            }
        }
        mock_pipeline.store_analysis_data(query_hash, analysis_data)
        
        # Verify analysis data
        stored_analysis = json.loads(s3.get_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/analysis-results.json'
        )['Body'].read())
        
        assert float(stored_analysis['price_stats']['avg']) == 2500.0
        
        # Store in DynamoDB
        mock_pipeline.store_database_entry(query_hash, {
            'status': 'completed',
            'product_count': 1
        })
        
        # Verify DynamoDB entry using the same mocked datetime
        dynamodb = boto3.client('dynamodb')
        timestamp = mock_pipeline.mock_datetime.now().isoformat()
        response = dynamodb.get_item(
            TableName='scraper-results',
            Key={
                'query_hash': {'S': query_hash},
                'timestamp': {'S': timestamp}
            }
        )
        
        assert 'Item' in response
        assert response['Item']['status']['S'] == 'completed'
        assert response['Item']['product_count']['N'] == '1'
        
    finally:
        mock_pipeline.cleanup()