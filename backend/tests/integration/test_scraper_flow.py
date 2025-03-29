import pytest
from unittest.mock import Mock, patch
import sys
import os

# Add project root to Python path if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from backend.workers.scraper_worker.scraper_worker_lambda import lambda_handler
from datetime import datetime

class TestScraperFlow:
    """Test full scraping workflow from form submission to report delivery"""

    @pytest.fixture
    def mock_s3(self):
        mock = Mock()
        mock.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'{"brand": "PRADA", "category": "BAGS", "scraper_name": "ITALIST"}'
            )
        }
        return mock

    @pytest.fixture
    def mock_db(self):
        """Mock database operations"""
        mock = Mock()
        mock.query.return_value = [
            {
                'brand': 'PRADA',
                'product': 'Galleria Bag',
                'price': 2000,
                'date': '2024-01-01'
            }
        ]
        return mock

    @pytest.fixture
    def mock_sns(self):
        """Mock SNS notifications"""
        return Mock()

    @pytest.fixture
    def mock_csv_file(self, tmp_path):
        """Create a mock CSV file for testing"""
        file_path = tmp_path / "test_results.csv"
        with open(file_path, 'w') as f:
            f.write("id,brand,name,price\n")
            f.write("1,PRADA,Test Bag,1000\n")
        return str(file_path)

    def test_full_scraping_workflow(self, mock_s3, mock_db, mock_sns, mock_csv_file):
        """Test complete workflow"""
        # Setup form data response
        mock_s3.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'{"brand": "PRADA", "category": "BAGS", "scraper_name": "ITALIST"}'
            )
        }

        # Mock orchestrator to return our test CSV
        with patch('boto3.client') as mock_boto3:
            mock_boto3.side_effect = lambda service: {
                's3': mock_s3,
                'sns': mock_sns,
            }[service]

            with patch('backend.db.connection') as mock_db_conn:
                mock_db_conn.return_value = mock_db

                with patch('backend.workers.scraper_worker.scraper_worker_lambda.ScraperOrchestrator') as mock_orch_class:
                    mock_orchestrator = Mock()
                    mock_orchestrator.run_scraper.return_value = mock_csv_file
                    mock_orch_class.return_value = mock_orchestrator

                    event = {
                        'bucket': 'test-bucket',
                        'query_hash': 'test123'
                    }
                    context = Mock()
                    
                    result = lambda_handler(event, context)

                    # Verify workflow
                    assert result['statusCode'] == 200
                    assert result['body']['success'] is True
                    
                    # Verify scraper was called
                    mock_orchestrator.run_scraper.assert_called_once_with(
                        scraper_name="ITALIST",
                        brand="PRADA",
                        category="BAGS",
                        output_dir="raw",
                        query_hash="test123",
                        local=True
                    )

                    # Verify results were saved to S3
                    mock_s3.upload_file.assert_called_once()

                    # Verify database operations
                    mock_db.insert.assert_called()

                    # Verify notification sent
                    mock_sns.publish.assert_called_once()

    def test_error_handling_workflow(self, mock_s3, mock_db, mock_sns):
        """Test error handling throughout the workflow"""
        # Setup mock form data response
        mock_s3.get_object.return_value = {
            'Body': Mock(
                read=lambda: b'{"brand": "PRADA", "category": "BAGS", "scraper_name": "ITALIST"}'
            )
        }

        mock_orchestrator = Mock()
        mock_orchestrator.run_scraper.side_effect = Exception("Scraping failed")

        with patch('boto3.client') as mock_boto3:
            mock_boto3.side_effect = lambda service: {
                's3': mock_s3,
                'sns': mock_sns,
            }[service]

            with patch('backend.workers.scraper_worker.scraper_worker_lambda.ScraperOrchestrator') as mock_orch_class:
                mock_orch_class.return_value = mock_orchestrator

                event = {
                    'bucket': 'test-bucket',
                    'query_hash': 'test123'
                }
                context = Mock()

                result = lambda_handler(event, context)

                # Verify error handling
                assert result['statusCode'] == 500
                assert 'error' in result['body']
                mock_sns.publish.assert_called()  # Error notification sent

    def test_scheduled_scraping_workflow(self, mock_s3, mock_db, mock_sns):
        """Test scheduled/recurring scraping workflow"""
        # Setup scheduled scraping event
        event = {
            'bucket': 'test-bucket',
            'query_hash': 'test123',
            'schedule': {
                'frequency': 'weekly',
                'day': 'monday'
            }
        }
        context = Mock()

        # Similar mocking structure as above
        with patch('boto3.client') as mock_boto3:
            mock_boto3.side_effect = lambda service: {
                's3': mock_s3,
                'sns': mock_sns,
            }[service]

            # Verify schedule creation and execution
            # Implementation depends on your scheduling mechanism 