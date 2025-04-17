#!/bin/bash

# Exit on error
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Get the project root directory (parent of frontend)
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." &> /dev/null && pwd )"

# Activate virtual environment if it exists
if [ -d "$SCRIPT_DIR/venv" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

# Install requirements if not already installed
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    pip install -r "$SCRIPT_DIR/requirements.txt"
fi

# Add project root to PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Start the Flask server
echo "Starting Flask server..."
python "$SCRIPT_DIR/server.py" 