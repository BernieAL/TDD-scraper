#!/bin/bash
set -e  # Exit on error

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR  # Change to landing-page directory

BUCKET_NAME="price-tracker-landing"
REGION="us-east-1"

# Create S3 bucket
aws s3api create-bucket \
    --bucket $BUCKET_NAME \
    --region $REGION

# Disable block public access
aws s3api put-public-access-block \
    --bucket $BUCKET_NAME \
    --public-access-block-configuration "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false"

# Enable website hosting
aws s3 website s3://$BUCKET_NAME \
    --index-document index.html \
    --error-document error.html

# Set bucket policy
aws s3api put-bucket-policy \
    --bucket $BUCKET_NAME \
    --policy '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::'$BUCKET_NAME'/*"
            }
        ]
    }'

# Upload files
aws s3 sync . s3://$BUCKET_NAME \
    --exclude "*" \
    --include "index.html" \
    --include "styles.css" \
    --include "config.js" \
    --cache-control "max-age=3600"

# Create CloudFront distribution and capture the ID
DISTRIBUTION_ID=$(aws cloudfront create-distribution \
    --distribution-config "file://cloudfront-distro.json" \
    --output text \
    --query 'Distribution.Id')

if [ -n "$DISTRIBUTION_ID" ]; then
    echo "CloudFront Distribution ID: $DISTRIBUTION_ID"
    echo "Please save this ID for future deployments"
    
    # Wait for distribution to deploy
    echo "Waiting for CloudFront distribution to deploy..."
    aws cloudfront wait distribution-deployed --id "$DISTRIBUTION_ID"
    
    # Get and display the domain name
    DOMAIN_NAME=$(aws cloudfront get-distribution \
        --id "$DISTRIBUTION_ID" \
        --query 'Distribution.DomainName' \
        --output text)
    
    echo "Your site will be available at: https://$DOMAIN_NAME"
else
    echo "Failed to create CloudFront distribution"
    exit 1
fi

echo "Deployment complete!"