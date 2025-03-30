#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../.." && pwd )"

# Wait for Backend LocalStack to be ready
echo "Waiting for Backend LocalStack to be ready..."
TIMEOUT=30
COUNTER=0
while ! curl -s http://localhost:4567/health > /dev/null; do
    echo "Waiting for LocalStack to be available... ($COUNTER seconds)"
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [ $COUNTER -ge $TIMEOUT ]; then
        echo "Timeout waiting for LocalStack to be ready"
        exit 1
    fi
done
echo "LocalStack is ready!"

# Set AWS credentials for Backend LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4567

# Check and package Lambda functions if needed
LAMBDA_PACKAGES_DIR="$PROJECT_ROOT/backend/aws/lambda_functions/deployment-packages"
if [ ! -f "$LAMBDA_PACKAGES_DIR/form-submission-deployment-package.zip" ] || \
   [ ! -f "$LAMBDA_PACKAGES_DIR/scrape-orchestrator-deployment-package.zip" ] || \
   [ ! -f "$LAMBDA_PACKAGES_DIR/price-analyzer-deployment-package.zip" ]; then
    echo "Lambda deployment packages not found. Packaging Lambda functions..."
    cd "$PROJECT_ROOT/backend/aws/lambda_functions"
    ./package_lambda.sh
    cd -
fi

# Create S3 bucket for data storage
echo "Creating S3 bucket for data storage..."
aws --endpoint-url=http://localhost:4567 s3 mb s3://scraper-data-bucket 2>/dev/null || true

# Delete existing DynamoDB tables if they exist
echo "Deleting existing DynamoDB tables..."
aws --endpoint-url=http://localhost:4567 dynamodb delete-table --table-name products-table 2>/dev/null || true
aws --endpoint-url=http://localhost:4567 dynamodb delete-table --table-name price-history-table 2>/dev/null || true

# Wait for tables to be deleted
echo "Waiting for tables to be deleted..."
sleep 5

# Create DynamoDB tables
echo "Creating DynamoDB tables..."
cd "$PROJECT_ROOT"
PYTHONPATH="$PROJECT_ROOT" python3 backend/aws/db/table_schemas.py

# Seed DynamoDB with data
echo "Seeding DynamoDB with data..."
PYTHONPATH="$PROJECT_ROOT" python3 backend/aws/db/seed_dynamodb.py

# Create SNS topics
echo "Creating SNS topics..."
aws --endpoint-url=http://localhost:4567 sns create-topic --name price-change-topic 2>/dev/null || true
aws --endpoint-url=http://localhost:4567 sns create-topic --name sold-items-topic 2>/dev/null || true
aws --endpoint-url=http://localhost:4567 sns create-topic --name analysis-complete-topic 2>/dev/null || true

# Create IAM role for Lambda
echo "Creating IAM role..."
aws --endpoint-url=http://localhost:4567 iam create-role \
    --role-name lambda-role \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }' 2>/dev/null || true

# Attach basic Lambda execution policy
aws --endpoint-url=http://localhost:4567 iam put-role-policy \
    --role-name lambda-role \
    --policy-name lambda-basic-policy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "dynamodb:PutItem",
                    "dynamodb:GetItem",
                    "dynamodb:Scan",
                    "sns:Publish",
                    "sns:Subscribe",
                    "sns:ListSubscriptionsByTopic",
                    "sns:GetTopicAttributes"
                ],
                "Resource": [
                    "arn:aws:logs:*:*:*",
                    "arn:aws:s3:::scraper-data-bucket",
                    "arn:aws:s3:::scraper-data-bucket/*",
                    "arn:aws:dynamodb:us-east-1:000000000000:table/*",
                    "arn:aws:sns:us-east-1:000000000000:price-change-topic",
                    "arn:aws:sns:us-east-1:000000000000:sold-items-topic",
                    "arn:aws:sns:us-east-1:000000000000:analysis-complete-topic"
                ]
            }
        ]
    }'

# Create Lambda functions
echo "Creating Lambda functions..."
aws --endpoint-url=http://localhost:4567 lambda create-function \
    --function-name form-submission \
    --runtime python3.10 \
    --handler form_handler.handle_form_submit \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --zip-file "fileb://$LAMBDA_PACKAGES_DIR/form-submission-deployment-package.zip" 2>/dev/null || \
    aws --endpoint-url=http://localhost:4567 lambda update-function-code \
    --function-name form-submission \
    --zip-file "fileb://$LAMBDA_PACKAGES_DIR/form-submission-deployment-package.zip"

aws --endpoint-url=http://localhost:4567 lambda create-function \
    --function-name scrape-orchestrator \
    --runtime python3.10 \
    --handler pipeline_orchestrator.orchestrate_scraping_pipeline \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --zip-file "fileb://$LAMBDA_PACKAGES_DIR/scrape-orchestrator-deployment-package.zip" 2>/dev/null || \
    aws --endpoint-url=http://localhost:4567 lambda update-function-code \
    --function-name scrape-orchestrator \
    --zip-file "fileb://$LAMBDA_PACKAGES_DIR/scrape-orchestrator-deployment-package.zip"

aws --endpoint-url=http://localhost:4567 lambda create-function \
    --function-name price-analyzer \
    --runtime python3.10 \
    --handler price_analyzer.handle_analysis \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --zip-file "fileb://$LAMBDA_PACKAGES_DIR/price-analyzer-deployment-package.zip" 2>/dev/null || \
    aws --endpoint-url=http://localhost:4567 lambda update-function-code \
    --function-name price-analyzer \
    --zip-file "fileb://$LAMBDA_PACKAGES_DIR/price-analyzer-deployment-package.zip"

echo "Backend LocalStack setup complete!" 