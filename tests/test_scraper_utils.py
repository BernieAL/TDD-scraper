# tests/test_scraper_utils.py

import unittest
from src.utils.ScraperUtils import ScraperUtils
from datetime import datetime
import os

class TestScraperUtils(unittest.TestCase):

    def setUp(self):
        scraped_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'file_output'))
        self.utils = ScraperUtils(scraped_data_dir)

    def test_generate_hash(self):
        query = 'Bags'
        date = datetime.now().strftime('%Y-%d-%m')
        result = self.utils.generate_hash(query, date)
        self.assertIsInstance(result, str)

    def test_make_scraped_sub_dir(self):
        brand = 'Prada'
        query = 'Bags'
        result = self.utils.make_scraped_sub_dir(brand, query)
        self.assertTrue(os.path.isdir(result))

    def test_make_filtered_sub_dir(self):
        source = 'italist'
        query = 'Bags'
        date = datetime.now().strftime('%Y-%d-%m')
        result = self.utils.make_filtered_sub_dir(source, date, query)
        self.assertTrue(os.path.isdir(result))

if __name__ == '__main__':
    unittest.main()
