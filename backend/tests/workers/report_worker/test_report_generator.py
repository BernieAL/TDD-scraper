import pytest
from datetime import datetime
from report_generator import ReportGenerator

@pytest.fixture
def sample_analysis_results():
    return {
        'total_products': 100,
        'files_analyzed': 5,
        'price_stats': {
            'min': 100,
            'max': 1000,
            'avg': 500
        },
        'products_by_brand': {'PRADA': 50, 'GUCCI': 50},
        'products_by_category': {'BAGS': 100}
    }

@pytest.fixture
def report_generator():
    return ReportGenerator()

def test_generate_report(report_generator, sample_analysis_results):
    email = 'test@example.com'
    query_hash = 'test_hash'
    
    report = report_generator.generate_report(sample_analysis_results, email, query_hash)
    
    # Verify basic structure
    assert report['query_hash'] == query_hash
    assert report['email'] == email
    assert 'timestamp' in report
    assert isinstance(datetime.fromisoformat(report['timestamp']), datetime)
    
    # Verify summary
    summary = report['summary']
    assert summary['total_products'] == 100
    assert summary['files_analyzed'] == 5
    assert summary['price_range']['min'] == 100
    assert summary['price_range']['max'] == 1000
    assert summary['price_range']['average'] == 500
    
    # Verify distributions
    assert report['brand_distribution'] == {'PRADA': 50, 'GUCCI': 50}
    assert report['category_distribution'] == {'BAGS': 100}
    
    # Verify recommendations
    assert len(report['recommendations']) > 0

def test_recommendations_generation(report_generator, sample_analysis_results):
    recommendations = report_generator._generate_recommendations(sample_analysis_results)
    
    # Verify recommendations based on price stats
    assert any("below average price" in rec for rec in recommendations)
    assert any("above average price" in rec for rec in recommendations)
    
    # Verify recommendations based on brand distribution
    assert any("Multiple brands" in rec for rec in recommendations)
    
    # Verify recommendations based on category distribution
    assert any("multiple categories" in rec for rec in recommendations)

def test_error_handling(report_generator):
    # Test with invalid analysis results
    with pytest.raises(Exception):
        report_generator.generate_report({}, 'test@example.com', 'test_hash')
    
    # Test with missing required fields
    with pytest.raises(Exception):
        report_generator.generate_report({'price_stats': {}}, 'test@example.com', 'test_hash')

def test_edge_cases(report_generator):
    # Test with empty distributions
    empty_results = {
        'total_products': 0,
        'files_analyzed': 0,
        'price_stats': {'min': 0, 'max': 0, 'avg': 0},
        'products_by_brand': {},
        'products_by_category': {}
    }
    
    report = report_generator.generate_report(empty_results, 'test@example.com', 'test_hash')
    assert report['summary']['total_products'] == 0
    assert len(report['recommendations']) == 0
    
    # Test with single product
    single_product = {
        'total_products': 1,
        'files_analyzed': 1,
        'price_stats': {'min': 100, 'max': 100, 'avg': 100},
        'products_by_brand': {'PRADA': 1},
        'products_by_category': {'BAGS': 1}
    }
    
    report = report_generator.generate_report(single_product, 'test@example.com', 'test_hash')
    assert report['summary']['total_products'] == 1
    assert len(report['recommendations']) > 0 