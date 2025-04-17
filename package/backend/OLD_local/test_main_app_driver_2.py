import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import os
import sys

# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Ensure src directory is in the path
src_dir = os.path.join(parent_dir, 'src')
sys.path.append(src_dir)

class TestMakeScrapedSubDir(unittest.TestCase):
    @patch('src.main_app_driver.generate_hash')  # Mock generate_hash function
    @patch('os.makedirs')  # Mock os.makedirs to simulate directory creation
    @patch('os.path.exists')  # Mock os.path.exists to control its behavior
    def test_make_scraped_subdir(self, mock_exists, mock_makedirs, mock_generate_hash):
        # Simulate hash value returned from generate_hash
        mock_generate_hash.return_value = 'a7b5c73d'
        
        # Simulate directory existence
        mock_exists.return_value = False

        # Setup test values
        curr_date = datetime.now().strftime('%Y-%d-%m')
        brand = 'Prada'
        query = 'Bags'

        # Expected directory path
        expected_dir = os.path.join(parent_dir, 'src', 'file_output', f"RAW_SCRAPE_{curr_date}_{brand}_{query}_a7b5c73d")

        # Import the function
        from src.main_app_driver import make_scraped_sub_dir
        result = make_scraped_sub_dir(brand, query)

        # Assert generate_hash was called correctly
        mock_generate_hash.assert_called_once_with(query, curr_date)

        # Assert os.makedirs was called correctly
        mock_makedirs.assert_called_once_with(expected_dir)

        # Assert the function returns the correct directory path
        self.assertEqual(result, expected_dir)

if __name__ == '__main__':
    unittest.main()
