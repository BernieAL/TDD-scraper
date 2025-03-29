#!/bin/bash

# Wait for Frontend LocalStack to be ready
echo "Waiting for Frontend LocalStack to be ready..."
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

# Set AWS credentials for Frontend LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4567

# Create S3 bucket for static hosting
echo "Creating S3 bucket for static hosting..."
aws --endpoint-url=http://localhost:4567 s3 mb s3://frontend-static-bucket

# Configure bucket for static website hosting
aws --endpoint-url=http://localhost:4567 s3api put-bucket-website \
    --bucket frontend-static-bucket \
    --website-configuration '{"IndexDocument":{"Suffix":"index.html"},"ErrorDocument":{"Key":"error.html"}}'

echo "Frontend LocalStack setup complete!"
echo "Note: CloudFront is not available in the free version of LocalStack" 