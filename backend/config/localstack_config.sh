#!/bin/bash

# LocalStack endpoint configuration
export LOCALSTACK_ENDPOINT_PORT=4566
export LOCALSTACK_ENDPOINT_URL="http://localhost:${LOCALSTACK_ENDPOINT_PORT}"

# AWS configuration for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL="${LOCALSTACK_ENDPOINT_URL}"

# Common resource names
export S3_BUCKET_NAME="scraper-data-bucket"
export SNS_TOPIC_NAME="report-notifications-topic"
export LAMBDA_ROLE_NAME="lambda-role"

# Lambda environment variables
export LAMBDA_ENVIRONMENT='{
    "Variables": {
        "S3_BUCKET": "'"${S3_BUCKET_NAME}"'",
        "RAW_PATH": "raw",
        "FILTERED_PATH": "filtered",
        "SNS_TOPIC_ARN": "arn:aws:sns:'"${AWS_DEFAULT_REGION}"':000000000000:'"${SNS_TOPIC_NAME}"'"
    }
}' 