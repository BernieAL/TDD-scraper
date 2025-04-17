#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../.." && pwd )"

# Source LocalStack configuration
source "$PROJECT_ROOT/backend/config/localstack_config.sh"

# Wait for Backend LocalStack to be ready
echo "Waiting for Backend LocalStack to be ready..."
TIMEOUT=30
COUNTER=0
while ! curl -s "${LOCALSTACK_ENDPOINT_URL}/health" > /dev/null; do
    echo "Waiting for LocalStack to be available... ($COUNTER seconds)"
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [ $COUNTER -ge $TIMEOUT ]; then
        echo "Timeout waiting for LocalStack to be ready"
        exit 1
    fi
done
echo "LocalStack is ready!"

# Check and package Lambda functions if needed
LAMBDA_PACKAGES_DIR="$PROJECT_ROOT/backend/aws/lambda_functions/deployment-packages"
if [ ! -f "$LAMBDA_PACKAGES_DIR/form-submission-deployment-package.zip" ] || \
   [ ! -f "$LAMBDA_PACKAGES_DIR/scraper-worker-deployment-package.zip" ] || \
   [ ! -f "$LAMBDA_PACKAGES_DIR/price-analyzer-deployment-package.zip" ] || \
   [ ! -f "$LAMBDA_PACKAGES_DIR/report-generator-deployment-package.zip" ]; then
    echo "Lambda deployment packages not found. Packaging Lambda functions..."
    cd "$PROJECT_ROOT/backend/aws/lambda_functions"
    ./package_lambda.sh
    cd -
fi

# Create S3 bucket for data storage
echo "Creating S3 bucket for data storage..."
aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" s3 mb "s3://${S3_BUCKET_NAME}" 2>/dev/null || true

# Delete existing DynamoDB tables if they exist
echo "Deleting existing DynamoDB tables..."
aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" dynamodb delete-table --table-name products-table 2>/dev/null || true
aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" dynamodb delete-table --table-name price-history-table 2>/dev/null || true

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

# Create SNS topic for report notifications
echo "Creating SNS topic for report notifications..."
aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" sns create-topic --name "${SNS_TOPIC_NAME}" 2>/dev/null || true

# Create IAM role for Lambda
echo "Creating IAM role..."
aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" iam create-role \
    --role-name "${LAMBDA_ROLE_NAME}" \
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
aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" iam put-role-policy \
    --role-name "${LAMBDA_ROLE_NAME}" \
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
                    "sns:Subscribe"
                ],
                "Resource": [
                    "arn:aws:logs:*:*:*",
                    "arn:aws:s3:::'"${S3_BUCKET_NAME}"'",
                    "arn:aws:s3:::'"${S3_BUCKET_NAME}"'/*",
                    "arn:aws:dynamodb:'"${AWS_DEFAULT_REGION}"':000000000000:table/*",
                    "arn:aws:sns:'"${AWS_DEFAULT_REGION}"':000000000000:'"${SNS_TOPIC_NAME}"'"
                ]
            }
        ]
    }'

# Create Lambda functions
echo "Creating Lambda functions..."

# Function to create or update Lambda
create_or_update_lambda() {
    local function_name=$1
    local handler=$2
    local package=$3

    # Try to create function
    if aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" lambda create-function \
        --function-name $function_name \
        --runtime python3.10 \
        --handler $handler \
        --role "arn:aws:iam::000000000000:role/${LAMBDA_ROLE_NAME}" \
        --environment "${LAMBDA_ENVIRONMENT}" \
        --zip-file "fileb://$LAMBDA_PACKAGES_DIR/$package" 2>/dev/null; then
        echo "Created new Lambda function: $function_name"
    else
        # Function exists, update its code
        echo "Updating existing Lambda function: $function_name"
        aws --endpoint-url="${LOCALSTACK_ENDPOINT_URL}" lambda update-function-code \
            --function-name $function_name \
            --zip-file "fileb://$LAMBDA_PACKAGES_DIR/$package"
    fi
}

# Create or update Lambda functions
create_or_update_lambda "form-submission" "form_submission.form_handler.handle_form_submit" "form-submission-deployment-package.zip"
create_or_update_lambda "scraper-worker" "scraper_worker_lambda.lambda_handler" "scraper-worker-deployment-package.zip"
create_or_update_lambda "price-analyzer" "price_analyzer.handle_analysis" "price-analyzer-deployment-package.zip"
create_or_update_lambda "report-generator" "report_worker_lambda.lambda_handler" "report-generator-deployment-package.zip"

echo "Backend LocalStack setup complete!" 