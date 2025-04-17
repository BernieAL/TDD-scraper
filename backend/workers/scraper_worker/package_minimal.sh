#!/bin/bash

# Create a temporary directory for packaging
TEMP_DIR=$(mktemp -d)
echo "Created temporary directory: $TEMP_DIR"

# Create a minimal requirements.txt
echo "boto3==1.26.137" > minimal_requirements.txt

# Install dependencies
pip install -r minimal_requirements.txt -t $TEMP_DIR

# Copy the Lambda function code
cp lambda_function.py $TEMP_DIR/

# Create the deployment package
cd $TEMP_DIR
zip -r9 ../minimal_function.zip .

# Clean up
cd -
rm -rf $TEMP_DIR
rm minimal_requirements.txt

echo "Created deployment package: minimal_function.zip" 