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

    # def test_make_scraped_sub_dir(self):
    #     brand = 'Prada'
    #     query = 'Bags'
    #     current_date = datetime.now().strftime('%Y-%d-%m')
    #     result = self.utils.make_scraped_sub_dir(brand, query)
    #     self.assertTrue(os.path.isdir(result))
        
    #     query_hash = self.utils.generate_hash(query,current_date)
    #     expected_dir_path = os.path.join(self.utils.scraped_data_dir,f"RAW_SCRAPE_{brand}_{current_date}_{query}_{query_hash}")
    #     self.assertEqual(result,expected_dir_path)

    
    
    def test_make_filtered_sub_dir(self):
        source = 'italist'
        query = 'Bags'
        date = datetime.now().strftime('%Y-%d-%m')
        brand = 'Prada'

        #check that result is valid dir
        result = self.utils.make_filtered_sub_dir(brand,query)
        self.assertTrue(os.path.isdir(result))
        
        #check that result matches expected dir
        query_hash = self.utils.generate_hash(query,date)
        expected_dir_path = os.path.join(self.utils.scraped_data_dir,f"FILTERED_{brand}_{date}_{query}_{query_hash}")
        self.assertEqual(result,expected_dir_path)
        """
        test make_filtered_sub_dir function
            define scraper source
            define query (bags or all)
            define date
            call function with params, record result
            check that return result is a valid dir
        
        """
if __name__ == '__main__':
    unittest.main()
