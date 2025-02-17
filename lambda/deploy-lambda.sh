#!/bin/bash
set -e  # Exit on error

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Source core infrastructure config
source "${SCRIPT_DIR}/../aws/core_infra_config.sh"

FUNCTION_NAME="price-tracker-app-starter"
ROLE_NAME="price-tracker-lambda-role"
AWS_REGION="us-east-1"

# Verify AWS credentials
echo "Verifying AWS credentials..."
if ! aws sts get-caller-identity --region $AWS_REGION > /dev/null; then
    echo "Error: AWS credentials not valid. Please check your credentials."
    exit 1
fi

# Create a temporary directory for packaging the lambda function
TEMP_DIR=$(mktemp -d)
echo "Created temp dir: $TEMP_DIR"

# Install only boto3 for Lambda
pip install boto3==1.26.137 -t $TEMP_DIR

# Copy lambda handler to temp dir (using absolute path)
echo "Copying lambda handler from: ${SCRIPT_DIR}/app_starter/lambda_handler.py"
if [ ! -f "${SCRIPT_DIR}/app_starter/lambda_handler.py" ]; then
    echo "Error: lambda_handler.py not found at ${SCRIPT_DIR}/app_starter/lambda_handler.py"
    exit 1
fi
cp "${SCRIPT_DIR}/app_starter/lambda_handler.py" "$TEMP_DIR/"

# Create zip file
cd $TEMP_DIR
zip -r "${SCRIPT_DIR}/function.zip" ./*
cd ..

# Create IAM role if it doesn't exist
echo "Setting up IAM role..."
if ! aws iam get-role --role-name $ROLE_NAME --region $AWS_REGION 2>/dev/null; then
    aws iam create-role \
        --region $AWS_REGION \
        --role-name $ROLE_NAME \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }]
        }'
fi

# Attach necessary policies
aws iam attach-role-policy \
    --region $AWS_REGION \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam put-role-policy \
    --region $AWS_REGION \
    --role-name $ROLE_NAME \
    --policy-name "ECSandDynamoDB" \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "ecs:RunTask",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem"
            ],
            "Resource": "*"
        }]
    }'

# Create or update the Lambda function on aws, give it the zip file with our lambda handler
if aws lambda get-function --function-name $FUNCTION_NAME --region $AWS_REGION 2>/dev/null; then
    aws lambda update-function-code \
        --region $AWS_REGION \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://"${SCRIPT_DIR}/function.zip"
else
    aws lambda create-function \
        --region $AWS_REGION \
        --function-name $FUNCTION_NAME \
        --runtime python3.9 \
        --handler lambda_handler.start_app_components \
        --role $(aws iam get-role --role-name $ROLE_NAME --query 'Role.Arn' --output text) \
        --zip-file fileb://"${SCRIPT_DIR}/function.zip" \
        --environment "Variables={SUBNET_ID=$SUBNET_ID,SECURITY_GROUP_ID=$SECURITY_GROUP_ID}" \
        --timeout 30
fi

# Clean up
rm -rf $TEMP_DIR
rm "${SCRIPT_DIR}/function.zip"

echo "Lambda deployment complete!" 