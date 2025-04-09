#!/bin/bash

# Exit on error
set -e

echo "Starting deployment process..."

# Create temporary directories
echo "Creating temporary directories..."
rm -rf lambda_layer_parts
mkdir -p lambda_layer_parts

# Package function code
echo "Packaging function code..."
rm -f scraper_orchestrator.zip
zip -r scraper_orchestrator.zip scraper_orchestrator.py __init__.py

# Create and publish layers with optimized dependencies
echo "Creating and publishing layers..."

# Layer 1: Core dependencies (boto3 only)
echo "Creating core layer..."
mkdir -p lambda_layer_parts/core/python
pip install boto3==1.26.137 -t lambda_layer_parts/core/python
cd lambda_layer_parts/core
zip -r ../../lambda_layer_core.zip python
cd ../..
CORE_LAYER_ARN=$(aws lambda publish-layer-version \
    --layer-name scraper-dependencies-core \
    --zip-file fileb://lambda_layer_core.zip \
    --compatible-runtimes python3.9 \
    --query 'LayerVersionArn' \
    --output text)

# Layer 2: Web scraping dependencies (minimal set)
echo "Creating web scraping layer..."
mkdir -p lambda_layer_parts/web/python
pip install beautifulsoup4==4.13.3 requests==2.32.3 -t lambda_layer_parts/web/python
# Remove unnecessary files
find lambda_layer_parts/web/python -type d -name "__pycache__" -exec rm -rf {} +
find lambda_layer_parts/web/python -type d -name "tests" -exec rm -rf {} +
find lambda_layer_parts/web/python -type d -name "docs" -exec rm -rf {} +
cd lambda_layer_parts/web
zip -r ../../lambda_layer_web.zip python
cd ../..
WEB_LAYER_ARN=$(aws lambda publish-layer-version \
    --layer-name scraper-dependencies-web \
    --zip-file fileb://lambda_layer_web.zip \
    --compatible-runtimes python3.9 \
    --query 'LayerVersionArn' \
    --output text)

# Layer 3: Utility dependencies (pytz only)
echo "Creating utility layer..."
mkdir -p lambda_layer_parts/utils/python
pip install pytz==2025.2 -t lambda_layer_parts/utils/python
# Remove unnecessary files
find lambda_layer_parts/utils/python -type d -name "__pycache__" -exec rm -rf {} +
find lambda_layer_parts/utils/python -type d -name "tests" -exec rm -rf {} +
cd lambda_layer_parts/utils
zip -r ../../lambda_layer_utils.zip python
cd ../..
UTILS_LAYER_ARN=$(aws lambda publish-layer-version \
    --layer-name scraper-dependencies-utils \
    --zip-file fileb://lambda_layer_utils.zip \
    --compatible-runtimes python3.9 \
    --query 'LayerVersionArn' \
    --output text)

# Check if function exists
echo "Checking if function exists..."
if aws lambda get-function --function-name scrape-orchestrator &>/dev/null; then
    echo "Updating existing function..."
    aws lambda update-function-code \
        --function-name scrape-orchestrator \
        --zip-file fileb://scraper_orchestrator.zip
    
    aws lambda update-function-configuration \
        --function-name scrape-orchestrator \
        --layers "$CORE_LAYER_ARN" "$WEB_LAYER_ARN" "$UTILS_LAYER_ARN"
else
    echo "Creating new function..."
    aws lambda create-function \
        --function-name scrape-orchestrator \
        --runtime python3.9 \
        --handler scraper_orchestrator.lambda_handler \
        --role arn:aws:iam::000000000000:role/lambda-role \
        --zip-file fileb://scraper_orchestrator.zip \
        --layers "$CORE_LAYER_ARN" "$WEB_LAYER_ARN" "$UTILS_LAYER_ARN"
fi

# Cleanup
echo "Cleaning up..."
rm -rf lambda_layer_parts
rm -f lambda_layer_*.zip

echo "Deployment complete!" 