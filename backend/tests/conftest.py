import pytest
from unittest.mock import Mock

@pytest.fixture
def sample_form_data():
    """Sample form submission data"""
    return {
        'brand': 'PRADA',
        'category': 'BAGS',
        'scraper_name': 'ITALIST',
        'query_hash': 'test123',
        'user_tier': 'free'
    }

@pytest.fixture
def sample_scrape_results():
    """Sample scraping results"""
    return [
        {
            'brand': 'PRADA',
            'product': 'Galleria Bag',
            'price': 2500,
            'url': 'http://test.com/product1',
            'date': '2024-01-15'
        }
    ] 