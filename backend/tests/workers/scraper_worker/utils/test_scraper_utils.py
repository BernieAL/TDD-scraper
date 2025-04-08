from pathlib import Path,sys





from backend.workers.scraper_worker.utils.scraper_utils import ScraperUtils
from backend.utils.project_paths import SCRAPER_WORKER_ROOT

import shutil
import pytest

"""To run all tests -> pytest (from any dir)

To run specific test file:
    # From tests/workers/scraper_worker/utils/
    pytest test_scraper_utils.py

    # Or from anywhere using path
    pytest tests/workers/scraper_worker/utils/test_scraper_utils.py

To run specific test function:
    # Run just the source directory test
    pytest test_scraper_utils.py::test_source_specific_dir_creation

# Show print statements and more details
    pytest -v -s test_scraper_utils.py

"""


"""

This test document has tests for all functions of test_scraper_utils 

"""

# 1. First, create a fixture that will be used by all tests
@pytest.fixture
def scraper_utils():
    """
    Setup: Create a new ScraperUtils instance before each test
    Teardown: Clean up temp directories after each test
    """
    utils = ScraperUtils()
    yield utils  # This provides the utils object to each test
    # After each test, cleanup
    if ScraperUtils.TEMP_DIR.exists():
        shutil.rmtree(ScraperUtils.TEMP_DIR)

# 2. Test temp directory creation
def test_temp_dir_static_configuration(scraper_utils):

    """
    Test that TEMP_DIR static variable is correctly configured and accessible.

    Test setup:
    - ScraperUtils class with static TEMP_DIR
    - SCRAPER_WORKER_ROOT from project_paths
    
    What's being tested:
    - ScraperUtils.TEMP_DIR static variable
    - Directory creation and configuration
    - Path resolution relative to SCRAPER_WORKER_ROOT
    
    Expected results:
    - TEMP_DIR exists as directory
    - Located at: scraper_worker_root/temp/
    - Accessible through class (ScraperUtils.TEMP_DIR)
    - Parent directory matches SCRAPER_WORKER_ROOT
    
    Example:
    >>> ScraperUtils.TEMP_DIR
    PosixPath('/path/to/scraper_worker/temp')
    >>> ScraperUtils.TEMP_DIR.parent == SCRAPER_WORKER_ROOT
    True
    """

    #Test through class (static access)
    assert ScraperUtils.TEMP_DIR.exists() , f"TEMP Dir was not created "
    assert ScraperUtils.TEMP_DIR.is_dir(), f" is not dir "
    assert ScraperUtils.TEMP_DIR.name == 'temp', "TEMP_DIR has wrong name"
    assert ScraperUtils.TEMP_DIR.parent == SCRAPER_WORKER_ROOT, "TEMP_DIR in wrong location"

    

# 3. Test source-specific directory creation
def test_source_specific_dir_creation(scraper_utils):
    """
    we are testing that source-specific directories are created correctly

    Ex. If source is "italist", we should make italist/ as a subdir in temp/
    Result -> temp/italist

    the function we are testing is scraper_utils.make_data_source_output_dir(source)

    function params: source (string)
    function result: creates source-specific subdir in temp/, and returns path

    to check that dir was created
    use assert to ensure existance of dir
    use assert to ensure dir location as intended

    
    """

    #the data we need to pass to function
    sources = ['italist','rebag','ebay']

    for src in sources:
        src_dir = scraper_utils.make_data_source_output_dir(src)

        #verify the directory exists, or else display fail msg
        assert src_dir.exists(), f"Directory {src} not created successfully"

        #verify dir is actually a dir
        assert src_dir.is_dir(), f"{src} path is not a directory"

        #verify dir name is correct
        assert src_dir.name == src.lower(), f"Directory name mismatch for {src}"

        #verify dir is in correct location
        assert src_dir.parent == ScraperUtils.TEMP_DIR, f"Wrong parent directory for {src}"

# 4. Test file saving
def test_file_save(scraper_utils):

    """
    Test that scraped data is saved correctly to source-specific directory.

    Test setup:
    - ScraperUtils instance with temp directory
    - Test data row simulating scraped product
    - Parameters for file naming/organization
    
    What's being tested:
    - ScraperUtils.save_to_file() method
    - File creation and location
    - File naming convention
    - File content structure
    
    Expected results:
    - File created in correct source directory (temp/italist/)
    - Filename follows convention: RAW_ITALIST_PRADA_<date>_BAGS_<hash>.csv
    - File contains:
        - Header rows (date, category)
        - Column headers
        - Data row with test product
    
    Example:
    >>> data = [['123', 'PRADA', 'NEVERFULL', 1000, 'http://test.com', 'ITALIST']]
    >>> temp_file, filename = scraper_utils.save_to_file(
    ...     data=data, brand='PRADA', category='BAGS',
    ...     source='ITALIST', output_dir='raw',
    ...     query_hash='test123', data_type=0
    ... )
    >>> temp_file
    PosixPath('temp/italist/RAW_ITALIST_PRADA_2024-01-15_BAGS_test123.csv')
    """

    # Test data
    test_data = [
        ['123', 'PRADA', 'NEVERFULL', 1000, 'http://test.com', 'ITALIST']
    ]

    # Save file
    temp_file, filename = scraper_utils.save_to_file(
        data=test_data,
        brand='PRADA',
        category='BAGS',
        source='ITALIST',
        output_dir='raw',
        query_hash='test123',
        data_type=0
    )

    #assert the file exists
    assert temp_file.exists(),  "File was not created"

    #verify file location
    assert temp_file.parent.name == 'italist', "File not in correct location"

    #verify file name
    assert "RAW_ITALIST_PRADA" in temp_file.name, "Filename not formatted correctly"

    #verify file contents
    with open(temp_file) as f:
        lines = f.readlines()
        assert len(lines) > 4, "File missing content"
        assert 'PRADA' in lines[4], "Test data not written correctly"


