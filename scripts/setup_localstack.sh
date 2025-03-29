#!/bin/bash

# Wait for LocalStack to be ready
echo "Waiting for LocalStack to be ready..."
while ! curl -s http://localhost:4566/health | grep -q '"lambda": "available"'; do
    sleep 1
done

# Set AWS credentials for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566

# Create S3 bucket
echo "Creating S3 bucket..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://scraper-data-bucket

# Create IAM role for Lambda
echo "Creating IAM role..."
aws --endpoint-url=http://localhost:4566 iam create-role \
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
    }'

# Attach basic Lambda execution policy
aws --endpoint-url=http://localhost:4566 iam put-role-policy \
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
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }'

# Create ECS cluster
echo "Creating ECS cluster..."
aws --endpoint-url=http://localhost:4566 ecs create-cluster --cluster-name scraper-cluster

# Register task definitions
echo "Registering task definitions..."
aws --endpoint-url=http://localhost:4566 ecs register-task-definition \
    --family scraper-task \
    --network-mode awsvpc \
    --requires-compatibilities FARGATE \
    --cpu 256 \
    --memory 512 \
    --execution-role-arn arn:aws:iam::000000000000:role/lambda-role \
    --container-definitions '[{
        "name": "scraper",
        "image": "scraper:latest",
        "essential": true,
        "environment": [
            {"name": "AWS_REGION", "value": "us-east-1"},
            {"name": "S3_BUCKET", "value": "scraper-data-bucket"}
        ]
    }]'

aws --endpoint-url=http://localhost:4566 ecs register-task-definition \
    --family analysis-task \
    --network-mode awsvpc \
    --requires-compatibilities FARGATE \
    --cpu 256 \
    --memory 512 \
    --execution-role-arn arn:aws:iam::000000000000:role/lambda-role \
    --container-definitions '[{
        "name": "analysis",
        "image": "analysis:latest",
        "essential": true,
        "environment": [
            {"name": "AWS_REGION", "value": "us-east-1"},
            {"name": "S3_BUCKET", "value": "scraper-data-bucket"}
        ]
    }]'

aws --endpoint-url=http://localhost:4566 ecs register-task-definition \
    --family report-task \
    --network-mode awsvpc \
    --requires-compatibilities FARGATE \
    --cpu 256 \
    --memory 512 \
    --execution-role-arn arn:aws:iam::000000000000:role/lambda-role \
    --container-definitions '[{
        "name": "report",
        "image": "report:latest",
        "essential": true,
        "environment": [
            {"name": "AWS_REGION", "value": "us-east-1"},
            {"name": "S3_BUCKET", "value": "scraper-data-bucket"}
        ]
    }]'

# Create Lambda functions
echo "Creating Lambda functions..."
aws --endpoint-url=http://localhost:4566 lambda create-function \
    --function-name form-submission \
    --runtime python3.10 \
    --handler form_handler.handle_form_submit \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --zip-file fileb://backend/aws/lambda_functions/form_submission/deployment-package.zip

aws --endpoint-url=http://localhost:4566 lambda create-function \
    --function-name scrape-orchestrator \
    --runtime python3.10 \
    --handler pipeline_orchestrator.orchestrate_scraping_pipeline \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --zip-file fileb://backend/aws/lambda_functions/scrape_orchestrator/deployment-package.zip

echo "LocalStack setup complete!" 