import pytest
from backend.workers.scraper_worker.scrapers.italist_scraper import ItalistScraper
from backend.utils.test_utils import load_test_html

class TestItalistScraper:
    """Test suite for ItalistScraper"""
    
    @pytest.fixture
    def scraper(self):
        return ItalistScraper()
    
    def test_build_url(self, scraper):
        """Test URL construction"""
        url = scraper.build_url('PRADA', 'BAGS')
        assert 'prada' in url.lower()
        assert 'bags' in url.lower()
    
    def test_parse_product(self, scraper):
        """Test product data extraction"""
        html = load_test_html('italist_product.html')
        data = scraper.parse_product(html)
        assert data['brand'] == 'PRADA'

    def test_scraper_run_output(self, italist_scraper):
        """Test detailed output format of ItalistScraper"""
        result = italist_scraper.run()

        # Detailed file checks
        assert result.exists()
        assert result.name.startswith("RAW_ITALIST_PRADA")
        assert result.name.endswith("_BAGS.csv")
        assert result.parent.name == "italist"

        # Check file contents
        with open(result) as f:
            data = f.read()
            assert "brand,name,price" in data
            # More specific content checks 