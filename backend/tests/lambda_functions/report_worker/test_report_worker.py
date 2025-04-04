import json
import pytest
from unittest.mock import MagicMock, patch
from report_worker_lambda import lambda_handler, validate_event

@pytest.fixture
def valid_event():
    return {
        'analysis_results': {
            'total_products': 100,
            'files_analyzed': 5,
            'price_stats': {
                'min': 100,
                'max': 1000,
                'avg': 500
            },
            'products_by_brand': {'PRADA': 50, 'GUCCI': 50},
            'products_by_category': {'BAGS': 100}
        },
        'email': 'test@example.com',
        'query_hash': 'test_hash',
        'bucket_name': 'test-bucket',
        'sns_topic_arn': 'arn:aws:sns:region:account:topic'
    }

@pytest.fixture
def mock_s3_client():
    with patch('boto3.client') as mock_client:
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        yield mock_s3

@pytest.fixture
def mock_sns_client():
    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns
        yield mock_sns

def test_validate_event(valid_event):
    assert validate_event(valid_event) is True
    
    # Test missing required fields
    invalid_event = valid_event.copy()
    del invalid_event['email']
    assert validate_event(invalid_event) is False

def test_report_generation(valid_event, mock_s3_client, mock_sns_client):
    response = lambda_handler(valid_event, None)
    
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['message'] == 'Report generated successfully'
    assert body['report_path'].startswith('reports/test_hash/')
    
    # Verify S3 put_object was called
    mock_s3_client.put_object.assert_called_once()
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args['Bucket'] == 'test-bucket'
    assert call_args['Key'].startswith('reports/test_hash/')
    
    # Verify SNS publish was called
    mock_sns_client.publish.assert_called_once()
    message = json.loads(mock_sns_client.publish.call_args[1]['Message'])
    assert message['type'] == 'report_generated'
    assert message['query_hash'] == 'test_hash'
    assert message['email'] == 'test@example.com'

def test_error_handling(valid_event, mock_s3_client):
    # Simulate S3 error
    mock_s3_client.put_object.side_effect = Exception('S3 Error')
    
    response = lambda_handler(valid_event, None)
    
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert 'error' in body

def test_report_content(valid_event, mock_s3_client, mock_sns_client):
    response = lambda_handler(valid_event, None)
    
    # Get the report content from the S3 put_object call
    call_args = mock_s3_client.put_object.call_args[1]
    report = json.loads(call_args['Body'])
    
    # Verify report structure
    assert 'query_hash' in report
    assert 'email' in report
    assert 'timestamp' in report
    assert 'summary' in report
    assert 'brand_distribution' in report
    assert 'category_distribution' in report
    assert 'recommendations' in report
    
    # Verify summary content
    summary = report['summary']
    assert summary['total_products'] == 100
    assert summary['files_analyzed'] == 5
    assert 'price_range' in summary
    
    # Verify recommendations
    assert len(report['recommendations']) > 0 