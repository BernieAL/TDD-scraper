import pytest
from unittest.mock import Mock, patch
import boto3
from botocore.exceptions import ClientError
from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler

class TestScraperWorkerLambda:
    @pytest.fixture
    def mock_s3_client(self):
        """Create mock S3 client with sample response"""
        mock_client = Mock()
        
        # Mock successful response
        mock_client.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'{"brand": "PRADA", "category": "BAGS"}'
            )
        }
        return mock_client

    def test_lambda_handler_s3_form_data(self, mock_s3_client):
        """Test lambda handler successfully retrieves form data from S3"""
        # Setup test event
        event = {
            'bucket': 'test-bucket',
            'query_hash': 'test123'
        }
        context = Mock()

        # Mock boto3.client to return our mock
        with patch('boto3.client') as mock_boto3:
            mock_boto3.return_value = mock_s3_client
            
            # Execute lambda handler
            result = lambda_handler(event, context)
            
            # Verify S3 was called correctly
            mock_s3_client.get_object.assert_called_once_with(
                Bucket='test-bucket',
                Key='queries/test123/form-params.json'
            )

    def test_lambda_handler_s3_error(self, mock_s3_client):
        """Test lambda handler handles S3 errors"""
        event = {
            'bucket': 'test-bucket',
            'query_hash': 'test123'
        }
        context = Mock()

        # Setup mock to raise S3 error
        mock_s3_client.get_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchKey'}},
            operation_name='GetObject'
        )
        
        with patch('boto3.client') as mock_boto3:
            mock_boto3.return_value = mock_s3_client
            
            result = lambda_handler(event, context)
            
            # Verify error handling
            assert result['statusCode'] == 500
            assert 'error' in result['body']

    @pytest.fixture
    def mock_orchestrator(self):
        """Mock ScraperOrchestrator"""
        mock = Mock()
        mock.run_scraper.return_value = "/temp/test/output.csv"
        return mock

    @pytest.fixture
    def mock_sns_client(self):
        """Mock SNS client"""
        return Mock()

    def test_lambda_handler_invalid_event(self):
        """Test handler rejects invalid event structure"""
        event = {}  # Empty event
        context = Mock()

        result = lambda_handler(event, context)

        assert result['statusCode'] == 400
        assert 'Missing required fields' in result['body']['error']

    def test_lambda_handler_successful_scrape(self, mock_s3_client, mock_orchestrator, mock_sns_client):
        """Test successful scraping workflow"""
        event = {
            'bucket': 'test-bucket',
            'query_hash': 'test123'
        }
        context = Mock()

        # Mock form data response
        mock_s3_client.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'{"brand": "PRADA", "category": "BAGS", "scraper_name": "ITALIST"}'
            )
        }

        with patch('boto3.client') as mock_boto3:
            # Return different mocks based on service
            mock_boto3.side_effect = lambda service: {
                's3': mock_s3_client,
                'sns': mock_sns_client
            }[service]

            with patch('backend.workers.scraper_worker.scraper_worker_lambda.ScraperOrchestrator') as mock_orch_class:
                mock_orch_class.return_value = mock_orchestrator
                
                result = lambda_handler(event, context)

                # Verify success response
                assert result['statusCode'] == 200
                assert 'success' in result['body']
                
                # Verify orchestrator called correctly
                mock_orchestrator.run_scraper.assert_called_once_with(
                    scraper_name="ITALIST",
                    brand="PRADA",
                    category="BAGS",
                    output_dir="raw",
                    query_hash="test123",
                    local=True
                )

                # Verify SNS notification sent
                mock_sns_client.publish.assert_called_once()

    def test_lambda_handler_scraper_failure(self, mock_s3_client, mock_orchestrator, mock_sns_client):
        """Test handling of scraper failure"""
        event = {
            'bucket': 'test-bucket',
            'query_hash': 'test123'
        }
        context = Mock()

        # Mock form data
        mock_s3_client.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'{"brand": "PRADA", "category": "BAGS", "scraper_name": "ITALIST"}'
            )
        }

        # Mock scraper failure
        mock_orchestrator.run_scraper.return_value = None

        with patch('boto3.client') as mock_boto3:
            mock_boto3.side_effect = lambda service: {
                's3': mock_s3_client,
                'sns': mock_sns_client
            }[service]

            with patch('backend.workers.scraper_worker.scraper_worker_lambda.ScraperOrchestrator') as mock_orch_class:
                mock_orch_class.return_value = mock_orchestrator
                
                result = lambda_handler(event, context)

                assert result['statusCode'] == 500
                assert 'Scraper failed' in result['body']['error']

    def test_lambda_handler_sns_failure(self, mock_s3_client, mock_orchestrator, mock_sns_client):
        """Test handling of SNS notification failure"""
        event = {
            'bucket': 'test-bucket',
            'query_hash': 'test123'
        }
        context = Mock()

        # Mock form data
        mock_s3_client.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'{"brand": "PRADA", "category": "BAGS", "scraper_name": "ITALIST"}'
            )
        }

        # Mock SNS failure
        mock_sns_client.publish.side_effect = Exception("SNS Error")

        with patch('boto3.client') as mock_boto3:
            mock_boto3.side_effect = lambda service: {
                's3': mock_s3_client,
                'sns': mock_sns_client
            }[service]

            with patch('backend.workers.scraper_worker.scraper_worker_lambda.ScraperOrchestrator') as mock_orch_class:
                mock_orch_class.return_value = mock_orchestrator
                
                result = lambda_handler(event, context)

                assert result['statusCode'] == 500
                assert 'Notification error' in result['body']['error']

    def test_lambda_handler_invalid_form_data(self, mock_s3_client):
        """Test handling of invalid form data format"""
        event = {
            'bucket': 'test-bucket',
            'query_hash': 'test123'
        }
        context = Mock()

        # Mock invalid JSON response
        mock_s3_client.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'invalid json'
            )
        }

        with patch('boto3.client') as mock_boto3:
            mock_boto3.return_value = mock_s3_client
            
            result = lambda_handler(event, context)

            assert result['statusCode'] == 400
            assert 'Invalid form data' in result['body']['error']

    def test_lambda_handler_batch_processing(self):
        """Test batching multiple scrape requests"""
        # Implementation needed
        pass

    def test_lambda_handler_schedule_validation(self):
        """Test handling of scheduled scrape requests"""
        # Implementation needed
        pass

    def test_lambda_handler_tier_limits(self):
        """Test user tier processing limits"""
        # Implementation needed
        pass