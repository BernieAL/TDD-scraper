import os
import sys

def setup_paths():
    if os.getenv('RUNNING_IN_DOCKER') == '1':
        # In Docker, everything is in /app
        base_path = '/app'
    else:
        # Locally, use the directory containing this file
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Add base path to sys.path if not already there
    if base_path not in sys.path:
        sys.path.append(base_path)

# Run setup when imported
setup_paths()