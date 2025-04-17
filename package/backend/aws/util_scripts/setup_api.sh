#!/bin/bash
set -e  # Exit on error

# Check AWS CLI configuration
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "Error: AWS CLI not configured. Run 'aws configure' first."
    exit 1
fi

AWS_REGION="us-east-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)

echo "Using AWS Account: $AWS_ACCOUNT_ID in region: $AWS_REGION"

# Create API Gateway
echo "Creating API Gateway..."
API_ID=$(aws apigateway create-rest-api \
    --region $AWS_REGION \
    --name "price-tracker-api" \
    --query 'id' \
    --output text)

echo "API Gateway created with ID: $API_ID"

# Get root resource ID
echo "Getting root resource..."
ROOT_ID=$(aws apigateway get-resources \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --query 'items[0].id' \
    --output text)

echo "Root resource ID: $ROOT_ID"

# Create start-app resource
echo "Creating start-app resource..."
START_APP_RESOURCE_ID=$(aws apigateway create-resource \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --parent-id $ROOT_ID \
    --path-part "start-app" \
    --query 'id' \
    --output text)

# Create login and signup resources
echo "Creating login and signup resources..."
LOGIN_RESOURCE_ID=$(aws apigateway create-resource \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --parent-id $ROOT_ID \
    --path-part "login" \
    --query 'id' \
    --output text)

SIGNUP_RESOURCE_ID=$(aws apigateway create-resource \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --parent-id $ROOT_ID \
    --path-part "signup" \
    --query 'id' \
    --output text)

# Create POST methods for all endpoints
echo "Creating POST methods..."
# For start-app
aws apigateway put-method \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --resource-id $START_APP_RESOURCE_ID \
    --http-method POST \
    --authorization-type NONE

# For login
aws apigateway put-method \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --resource-id $LOGIN_RESOURCE_ID \
    --http-method POST \
    --authorization-type NONE \
    --no-api-key-required

# For signup
aws apigateway put-method \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --resource-id $SIGNUP_RESOURCE_ID \
    --http-method POST \
    --authorization-type NONE \
    --no-api-key-required

# Set up Lambda integrations
echo "Setting up Lambda integrations..."
# For start-app
aws apigateway put-integration \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --resource-id $START_APP_RESOURCE_ID \
    --http-method POST \
    --type AWS_PROXY \
    --integration-http-method POST \
    --uri arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:price-tracker-app-starter/invocations

# For login
aws apigateway put-integration \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --resource-id $LOGIN_RESOURCE_ID \
    --http-method POST \
    --type AWS_PROXY \
    --integration-http-method POST \
    --uri arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:auth-login/invocations

# For signup
aws apigateway put-integration \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --resource-id $SIGNUP_RESOURCE_ID \
    --http-method POST \
    --type AWS_PROXY \
    --integration-http-method POST \
    --uri arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:auth-signup/invocations

# Enable CORS for all endpoints
echo "Enabling CORS..."
for RESOURCE_ID in $START_APP_RESOURCE_ID $LOGIN_RESOURCE_ID $SIGNUP_RESOURCE_ID; do
    aws apigateway put-method-response \
        --region $AWS_REGION \
        --rest-api-id $API_ID \
        --resource-id $RESOURCE_ID \
        --http-method POST \
        --status-code 200 \
        --response-parameters "method.response.header.Access-Control-Allow-Origin=true"
done

# Deploy API
echo "Deploying API..."
aws apigateway create-deployment \
    --region $AWS_REGION \
    --rest-api-id $API_ID \
    --stage-name prod

# Create config.js directory if it doesn't exist
mkdir -p ../landing-page

# Update config.js with API details
echo "const API_ID = '$API_ID';
const AWS_REGION = '$AWS_REGION';" > ../landing-page/config.js

echo "API Gateway setup complete. API ID: $API_ID" 