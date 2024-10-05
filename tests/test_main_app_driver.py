import unittest
from unittest.mock import patch, mock_open
import os
import src.main_app_driver as main_app_driver  # Adjust the import path as needed


class TestReadUserInputData(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data="brand,query,specific_item\nPrada,bags,\nGucci,bags,Brown Bag\n\n")
    def test_read_user_input_data(self, mock_file):
        expected_output = [
            ('Prada', 'bags', None),
            ('Gucci', 'bags', 'Brown Bag')
        ]
        result = main_app_driver.read_user_input_data('dummy_file_path.csv')
        self.assertEqual(result, expected_output)


class TestGenerateHash(unittest.TestCase):

    def test_generate_hash(self):
        result = main_app_driver.generate_hash('prada_bags', '2024-09-24')
        self.assertEqual(len(result), 8)  # Ensure it's only 8 characters
        self.assertIsInstance(result, str)  # Ensure it's a string


class TestRunScrapers(unittest.TestCase):

    @patch('src.main_app_driver.italist')
    @patch('src.main_app_driver.site_a')
    @patch('src.main_app_driver.site_b')
    @patch('src.main_app_driver.site_c')
    @patch('os.makedirs')
    def test_run_scrapers(self, mock_makedirs, mock_site_c, mock_site_b, mock_site_a, mock_italist):
        query = "bags"
        brand = "Prada"
        specific_item = None

        # Mock the return of the scraper functions
        mock_makedirs.return_value = True
        mock_italist.return_value = None
        mock_site_a.return_value = None
        mock_site_b.return_value = None
        mock_site_c.return_value = None

        # Run function
        scraped_subdir = main_app_driver.run_scrapers(brand, query, specific_item)
        
        # Ensure each scraper function is called
        mock_italist.assert_called_once()
        mock_site_a.assert_called_once()
        mock_site_b.assert_called_once()
        mock_site_c.assert_called_once()


class TestRunComparisons(unittest.TestCase):

    @patch('os.walk')
    @patch('src.main_app_driver.compare_driver')
    def test_run_comparisons(self, mock_compare_driver, mock_walk):
        mock_walk.return_value = [
            ('scraped_data_dir', [], ['scraped_file_1.csv', 'scraped_file_2.csv'])
        ]
        
        main_app_driver.run_comparisons('scraped_data_dir')
        
        # Ensure the comparison driver is called twice for the two files
        self.assertEqual(mock_compare_driver.call_count, 2)


class TestDriverFunction(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data="brand,query,specific_item\nPrada,bags,\nGucci,bags,Brown Bag\n\n")
    @patch('src.main_app_driver.run_scrapers')
    @patch('src.main_app_driver.run_comparisons')
    def test_driver_function(self, mock_run_comparisons, mock_run_scrapers, mock_file):
        mock_run_scrapers.return_value = 'dummy_scraped_dir'
        
        main_app_driver.driver_function()
        
        # Check that scrapers and comparisons were called
        self.assertEqual(mock_run_scrapers.call_count, 2)
        self.assertEqual(mock_run_comparisons.call_count, 2)


if __name__ == '__main__':
    unittest.main()
