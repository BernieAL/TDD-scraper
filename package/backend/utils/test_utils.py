"""
Test utilities for loading test data and HTML files.
"""
import os
from pathlib import Path

def load_test_html(filename: str) -> str:
    """Load a test HTML file from the test data directory.
    
    Args:
        filename: Name of the HTML file to load
        
    Returns:
        str: Contents of the HTML file
    """
    test_data_dir = Path(__file__).parent.parent / 'tests' / 'test_data'
    file_path = test_data_dir / filename
    
    if not file_path.exists():
        raise FileNotFoundError(f"Test HTML file not found: {filename}")
        
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read() 