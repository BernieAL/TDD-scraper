#!/bin/bash

# Exit on error
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEPLOYMENT_DIR="${SCRIPT_DIR}/deployment-packages"
mkdir -p "$DEPLOYMENT_DIR"

# Function to package a Lambda function
package_lambda() {
    local function_name="$1"
    local source_dir="${function_name//-/_}"  # Convert hyphens to underscores for directory names
    local extra_modules="$2"

    echo "Packaging ${function_name} Lambda function..."
    local temp_dir=$(mktemp -d)
    echo "Created temporary directory: ${temp_dir}"

    # Create the base directory structure
    mkdir -p "${temp_dir}/${source_dir}"

    # Copy the main function files
    if [ -d "${SCRIPT_DIR}/${source_dir}" ]; then
        cp -r "${SCRIPT_DIR}/${source_dir}"/* "${temp_dir}/${source_dir}/"
    else
        echo "Warning: Source directory ${SCRIPT_DIR}/${source_dir} not found"
        rm -rf "${temp_dir}"
        return 1
    fi

    # Copy additional modules if specified
    if [ -n "$extra_modules" ]; then
        IFS=',' read -ra MODULES <<< "$extra_modules"
        for module in "${MODULES[@]}"; do
            if [ -d "/home/ubuntu/Documents/Projects/TDD-scraper/backend/workers/${module}" ]; then
                mkdir -p "${temp_dir}/${source_dir}/${module}"
                cp -r "/home/ubuntu/Documents/Projects/TDD-scraper/backend/workers/${module}"/* "${temp_dir}/${source_dir}/${module}/"
            else
                echo "Warning: Module ${module} not found"
            fi
        done
    fi

    # Create deployment package
    cd "${temp_dir}" && zip -r "${DEPLOYMENT_DIR}/${function_name}-deployment-package.zip" .
    echo "Created deployment package: ${DEPLOYMENT_DIR}/${function_name}-deployment-package.zip"

    # Cleanup
    rm -rf "${temp_dir}"
}

# Package each Lambda function with its dependencies
package_lambda "scrape-orchestrator" "scraper_worker"
package_lambda "report-worker" "report_worker"
package_lambda "analysis" "analysis_worker"
package_lambda "form-submission" ""
package_lambda "auth" ""

echo "Lambda packaging complete!" 