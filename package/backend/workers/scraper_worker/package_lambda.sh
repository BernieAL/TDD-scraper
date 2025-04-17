#!/bin/bash

# Create a temporary directory for packaging
TEMP_DIR=$(mktemp -d)
echo "Created temporary directory: $TEMP_DIR"

# Create the python directory structure
mkdir -p $TEMP_DIR/python

# Install dependencies to the temporary directory
pip install -r requirements.txt -t $TEMP_DIR/python

# Copy the Lambda function code
cp scraper_worker_lambda.py $TEMP_DIR/
cp scraper_orchestrator.py $TEMP_DIR/
cp -r utils $TEMP_DIR/
cp -r scrapers $TEMP_DIR/

# Create the deployment package
cd $TEMP_DIR
zip -r9 ../function.zip .

# Clean up
cd -
rm -rf $TEMP_DIR

echo "Created deployment package: function.zip" 