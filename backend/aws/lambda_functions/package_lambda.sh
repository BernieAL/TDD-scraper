#!/bin/bash

# Exit on error
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Function to package a Lambda function
package_lambda() {
    local function_name=$1
    local handler_file=$2
    local output_dir="$SCRIPT_DIR/deployment-packages"
    
    echo "Packaging $function_name Lambda function..."
    
    # Create output directory if it doesn't exist
    mkdir -p "$output_dir"
    
    # Create temporary directory for packaging
    local temp_dir=$(mktemp -d)
    echo "Created temporary directory: $temp_dir"
    
    # Copy the handler file
    cp "$SCRIPT_DIR/$handler_file" "$temp_dir/"
    
    # Install dependencies to the temporary directory
    if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
        pip install -r "$SCRIPT_DIR/requirements.txt" -t "$temp_dir"
    fi
    
    # Create deployment package
    cd "$temp_dir"
    zip -r "$output_dir/${function_name}-deployment-package.zip" .
    cd - > /dev/null
    
    # Clean up
    rm -rf "$temp_dir"
    
    echo "Created deployment package: $output_dir/${function_name}-deployment-package.zip"
}

# Package form submission Lambda
package_lambda "form-submission" "form_submission/form_handler.py"

# Package scrape orchestrator Lambda
package_lambda "scrape-orchestrator" "scrape_orchestrator/pipeline_orchestrator.py"

echo "Lambda packaging complete!" 