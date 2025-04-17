#!/bin/bash

# Exit on error
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../.." && pwd )"
DEPLOYMENT_DIR="${SCRIPT_DIR}/deployment-packages"
mkdir -p "$DEPLOYMENT_DIR"

# Function to package a Lambda function
package_lambda() {
    local function_name="$1"
    local source_dir="$2"
    local handler_file="$3"

    echo "=== Packaging ${function_name} Lambda function ==="
    echo "Source directory: ${source_dir}"
    echo "Handler file: ${handler_file}"
    
    local temp_dir=$(mktemp -d)
    echo "Created temporary directory: ${temp_dir}"

    # Create the package directory structure
    mkdir -p "${temp_dir}/python"
    
    # Copy the entire source directory structure
    if [ -d "${source_dir}" ]; then
        echo "Copying source files..."
        cp -r "${source_dir}"/* "${temp_dir}/python/"
        
        # Ensure __init__.py files exist in all directories
        find "${temp_dir}/python" -type d -exec touch {}/__init__.py \;
        
        # Copy common utilities if they exist
        if [ -d "${PROJECT_ROOT}/backend/utils" ]; then
            echo "Copying common utilities..."
            cp -r "${PROJECT_ROOT}/backend/utils" "${temp_dir}/python/"
        fi
        
        # Copy config if it exists
        if [ -d "${PROJECT_ROOT}/backend/config" ]; then
            echo "Copying config..."
            cp -r "${PROJECT_ROOT}/backend/config" "${temp_dir}/python/"
        fi
        
        # Install dependencies
        if [ -f "${source_dir}/requirements.txt" ]; then
            echo "Installing dependencies from ${source_dir}/requirements.txt..."
            cd "${temp_dir}"
            python3 -m pip install -r "${source_dir}/requirements.txt" --target python/ --no-cache-dir
            
            # Clean up unnecessary files
            echo "Cleaning up unnecessary files..."
            find python/ -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
            find python/ -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
            find python/ -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
            find python/ -type f -name "*.pyc" -delete
            find python/ -type f -name "*.pyo" -delete
            find python/ -type f -name "*.pyd" -delete
            
            # Keep only necessary botocore data
            if [ -d "python/botocore/data" ]; then
                echo "Optimizing botocore..."
                mkdir -p python/botocore/data.tmp
                for service in dynamodb s3 sns sqs lambda; do
                    if [ -d "python/botocore/data/${service}" ]; then
                        cp -r "python/botocore/data/${service}" "python/botocore/data.tmp/"
                    fi
                done
                rm -rf python/botocore/data
                mv python/botocore/data.tmp python/botocore/data
            fi
        else
            echo "No requirements.txt found in ${source_dir}"
        fi
    else
        echo "Error: Source directory ${source_dir} not found"
        rm -rf "${temp_dir}"
        return 1
    fi

    # Create deployment package
    echo "Creating deployment package..."
    cd "${temp_dir}" && zip -r "${DEPLOYMENT_DIR}/${function_name}-deployment-package.zip" python/
    
    # Print package size
    echo "Package size: $(du -h "${DEPLOYMENT_DIR}/${function_name}-deployment-package.zip" | cut -f1)"
    
    # Cleanup
    echo "Cleaning up temporary directory..."
    rm -rf "${temp_dir}"
    
    echo "=== Finished packaging ${function_name} ==="
    echo
}

# Clean up old deployment packages
echo "Cleaning up old deployment packages..."
rm -rf "${DEPLOYMENT_DIR}"/*.zip

# Package each Lambda function
echo "Packaging Lambda functions..."
package_lambda "scraper-worker" "${PROJECT_ROOT}/backend/workers/scraper_worker" "scraper_worker_lambda.py"
package_lambda "report-generator" "${PROJECT_ROOT}/backend/workers/report_worker" "report_worker_lambda.py"
package_lambda "price-analyzer" "${PROJECT_ROOT}/backend/workers/analysis_worker" "price_analyzer.py"
package_lambda "form-submission" "${PROJECT_ROOT}/backend/aws/lambda_functions/form_submission" "form_handler.py"

echo "Lambda packaging complete!" 