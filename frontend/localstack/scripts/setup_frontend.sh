#!/bin/bash

# Wait for Frontend LocalStack to be ready
echo "Waiting for Frontend LocalStack to be ready..."
while ! curl -s http://localhost:4567/health | grep -q '"s3": "available"'; do
    sleep 1
done

# Set AWS credentials for Frontend LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4567

# Create S3 bucket for static hosting
echo "Creating S3 bucket for static hosting..."
aws --endpoint-url=http://localhost:4567 s3 mb s3://frontend-static-bucket

# Configure bucket for static website hosting
aws --endpoint-url=http://localhost:4567 s3 website \
    --bucket frontend-static-bucket \
    --index-document index.html \
    --error-document error.html

# Create CloudFront distribution
echo "Creating CloudFront distribution..."
aws --endpoint-url=http://localhost:4567 cloudfront create-distribution \
    --origin-domain-name frontend-static-bucket.s3.amazonaws.com \
    --default-root-object index.html \
    --enabled

echo "Frontend LocalStack setup complete!" 