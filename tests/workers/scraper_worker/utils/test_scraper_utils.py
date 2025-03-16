from pathlib import Path,sys


def find_project_root():
    """
    find project root by looking  for .PROJECT_ROOT marker file
    """

    #get path of current file
    current_path = Path(__file__).resolve()

    #while we arent at filesystem root
    while current_path.parent != current_path: #stops at filesystem root
        
        if(current_path / '.PROJECT_ROOT').exists():
            return current_path
        current_path = current_path.parent

    raise RuntimeError("Could not find project root")


project_root = find_project_root()
sys.path.append(str(project_root))

from backend.workers.scraper_worker.utils.ScraperUtils import ScraperUtils
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
    if utils.temp_dir.exists():
        shutil.rmtree(utils.temp_dir)

# 2. Test temp directory creation
def test_temp_dir_creation(scraper_utils):
    """Test that temp directory is created correctly"""
    # Check temp dir exists
    assert scraper_utils.temp_dir.exists()
    # Check it's actually a directory
    assert scraper_utils.temp_dir.is_dir()
    # Check it's named correctly
    assert scraper_utils.temp_dir.name == 'tmp'


def test_temp_dir_creation2(scraper_utils):

    """
    testing that temp/ is created when scraper_uils is instantiated

    location of creation should be 
    scraper_worker_root/temp

    why? -> scraper_utils is utility class of all scraper scripts.
        Ex. ItalistScraper imports and makes use or scraper_utils  

        scraper scripts will create source-specific subdirs inside of temp/ and store their scraped data as csv files
        Ex. italist -> temp/italist/scrape_file.csv

    params: None
    results: temp/ created in scraper_worker_root/

    
    """

    #create instance of class
    scraper_utils = ScraperUtils()

    #assert that temp/ was created inside of scraper_worker_root
    #obtain path to scraper_worker_root through project_root
    

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
        assert src_dir.parent == scraper_utils.temp_dir, f"Wrong parent directory for {src}"




# 4. Test file saving
def test_save_to_file(scraper_utils):
    """Test saving scraped data to source-specific directory"""
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
    
    # Verify file was created
    assert temp_file.exists(), "File was not created"
    
    # Verify file location
    assert temp_file.parent.name == 'italist', "File not in correct source directory"
    assert 'RAW_ITALIST_PRADA' in temp_file.name, "Filename not formatted correctly"
    
    # Verify file contents
    with open(temp_file) as f:
        lines = f.readlines()
        assert len(lines) > 4, "File missing content"
        assert 'PRADA' in lines[4], "Test data not written correctly"

# Test code here... 