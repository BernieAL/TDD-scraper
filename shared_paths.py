import os
from simple_chalk import chalk
from pathlib import Path

#get proj root dir
PROJECT_ROOT = Path(__file__).parent

#base shared data dir (mounted volume)
#if .env SHARED_DATA_PATH defined, else build path from project root
#this same dir will be mounted as shared volume for containers
SHARED_DATA_PATH = os.getenv('SHARED_DATA_PATH',os.path.join(PROJECT_ROOT,'shared_data'))


# Subdirectories for different types of data
SCRAPE_FILE_OUTPUT = os.path.join(SHARED_DATA_PATH, 'SCRAPE_FILE_OUTPUT')
RAW_SCRAPE_DIR = os.path.join(SCRAPE_FILE_OUTPUT, 'raw_scrapes')
FILTERED_DATA_DIR = os.path.join(SCRAPE_FILE_OUTPUT,'filtered_data')
REPORTS_ROOT_DIR = os.path.join(SHARED_DATA_PATH,'reports')
SOLD_REPORTS_DIR = os.path.join(REPORTS_ROOT_DIR,'sold_report_output')
PRICE_REPORTS_DIR = os.path.join(REPORTS_ROOT_DIR,'price_report_output')
ARCHIVE_DIR = os.path.join(SHARED_DATA_PATH, 'archives')


required_dirs = [
    RAW_SCRAPE_DIR,
    FILTERED_DATA_DIR,
    REPORTS_ROOT_DIR,
    SOLD_REPORTS_DIR,
    PRICE_REPORTS_DIR,
    ARCHIVE_DIR
]

def setup_dirs():

    """
    Create all dirs in shared volume
        
    """

    for dir in required_dirs:
        os.makedirs(dir,exist_ok=True)
        print(chalk.green(f"Created/verified directory: {dir}"))
    return True

def verify_dirs():
    """Verify all required directories exist and are accessible"""
    missing_dirs = []
    for directory in required_dirs:
        if not os.path.exists(directory):
            missing_dirs.append(directory)
        elif not os.access(directory, os.W_OK):
            print(chalk.yellow(f"Warning: Directory exists but not writable: {directory}"))
    
    return missing_dirs


if __name__ == "__main__":

    print(chalk.blue("Setting up shared directories..."))
    
    # Setup directories
    if setup_dirs():
        print(chalk.green("✓ Directory setup complete"))
    else:
        print(chalk.red("✗ Directory setup failed"))
        exit(1)
    
     # Verify directories
    missing = verify_dirs()
    if missing:
        print(chalk.red("Missing directories:"))
        for dir in missing:
            print(chalk.red(f"  - {dir}"))
        exit(1)
    
    print(chalk.green("\nAll directories verified and ready:"))
    for dir in required_dirs:
        print(chalk.blue(f"  - {dir}"))