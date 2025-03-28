from pathlib import Path

def find_project_root():
    """
    Find project root directory by looking for .PROJECT_ROOT marker file.
    
    This function walks up the directory tree from the current file's location
    until it finds the .PROJECT_ROOT file, which marks the project root.
    
    Returns:
        Path: Absolute path to project root directory
        
    Raises:
        RuntimeError: If .PROJECT_ROOT marker file cannot be found
    """
    # Get absolute path of current file, resolving any symlinks
    current = Path(__file__).resolve()
    
    # Walk up directory tree until we hit filesystem root
    while current.parent != current:  # Stop at filesystem root
        # Check if marker file exists in current directory
        if (current / '.PROJECT_ROOT').exists():
            return current
        # Move up one directory
        current = current.parent
    
    #if we hit filesystem root - oops, didnt find .PROJECT_ROOT.txt marker
    raise RuntimeError("Could not find project root")

# Initialize path constants at module level for efficiency
# These are computed once when module is imported

# Root directory of entire project (where .PROJECT_ROOT lives)
PROJECT_ROOT = find_project_root()

# Root directory of backend code
BACKEND_ROOT = PROJECT_ROOT / 'backend'

# Root directory of scraper worker module
SCRAPER_WORKER_ROOT = BACKEND_ROOT / 'workers' / 'scraper_worker' 