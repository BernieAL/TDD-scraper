import json
import pytest
from datetime import datetime
from typing import Dict, Any
import boto3
from moto import mock_s3
import os
import sys

# Add the backend directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from backend.workers.report_worker.report_generator import ReportGenerator

@pytest.fixture
def sample_analysis_data():
    """Sample analysis data for testing report generation."""
    return {
        "changes": [
            {
                "url": "https://example.com/test1",
                "title": "Test Item 1",
                "old_price": 100.00,
                "new_price": 90.00,
                "price_change": -10.00,
                "percentage_change": -10.0
            },
            {
                "url": "https://example.com/test2",
                "title": "Test Item 2",
                "old_price": 200.00,
                "new_price": 220.00,
                "price_change": 20.00,
                "percentage_change": 10.0
            }
        ],
        "metadata": {
            "total_items": 2,
            "items_with_changes": 2,
            "average_price_change": 5.00,
            "average_percentage_change": 0.0
        }
    }

def test_generate_report(sample_analysis_data):
    """Test report generation."""
    generator = ReportGenerator(sample_analysis_data)
    report = generator.generate_report()
    
    assert report["summary"]["total_items"] == 2
    assert report["summary"]["items_with_changes"] == 2
    assert report["summary"]["average_price_change"] == 5.00
    assert report["summary"]["average_percentage_change"] == 0.0
    assert len(report["changes"]) == 2

def test_save_report(sample_analysis_data):
    """Test saving report to S3."""
    with mock_s3():
        # Create S3 bucket
        s3 = boto3.client('s3')
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        
        # Generate and save report
        generator = ReportGenerator(sample_analysis_data)
        report_path = generator.save_report(bucket_name, s3)
        
        # Verify report was saved
        assert report_path.startswith("reports/")
        assert report_path.endswith(".json")
        
        # Get and verify report contents
        report_response = s3.get_object(
            Bucket=bucket_name,
            Key=report_path
        )
        report = json.loads(report_response['Body'].read().decode('utf-8'))
        
        assert report["summary"]["total_items"] == 2
        assert report["summary"]["items_with_changes"] == 2
        assert len(report["changes"]) == 2

def test_empty_analysis():
    """Test report generation with empty analysis data."""
    empty_data = {
        "changes": [],
        "metadata": {
            "total_items": 0,
            "items_with_changes": 0,
            "average_price_change": 0.0,
            "average_percentage_change": 0.0
        }
    }
    
    generator = ReportGenerator(empty_data)
    report = generator.generate_report()
    
    assert report["summary"]["total_items"] == 0
    assert report["summary"]["items_with_changes"] == 0
    assert report["summary"]["average_price_change"] == 0.0
    assert report["summary"]["average_percentage_change"] == 0.0
    assert len(report["changes"]) == 0 