#!/bin/bash
set -e  # Exit on error

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AWS_REGION="us-east-1"
TABLE_NAME="app_status"

echo "Creating DynamoDB table..."

# Create DynamoDB table
aws dynamodb create-table \
    --table-name $TABLE_NAME \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --key-schema AttributeName=id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --region $AWS_REGION

# Wait for table to be active
echo "Waiting for table to be active..."
aws dynamodb wait table-exists \
    --table-name $TABLE_NAME \
    --region $AWS_REGION

echo "DynamoDB table '$TABLE_NAME' created successfully!"

# Add table name to core_infra_config.sh
echo "Adding table name to core infrastructure config..."
echo "export DYNAMODB_TABLE=$TABLE_NAME" >> "${SCRIPT_DIR}/core_infra_config.sh" 