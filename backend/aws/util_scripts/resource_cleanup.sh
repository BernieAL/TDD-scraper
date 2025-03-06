#!/bin/bash
#
# cleanup.sh
#
# Purpose: Clean up all AWS resources created for the Price Tracker application.
# This includes:
# - API Gateway endpoints (start-app, login, signup)
# - Lambda functions (app-starter, auth-login, auth-signup)
# - DynamoDB tables (app_status, users)
# - S3 bucket for static website hosting
# - CloudFront distribution
# - VPC resources (security groups, subnets, VPC)
# - IAM roles and policies
#
# Usage: ./cleanup.sh
# Note: Requires AWS CLI and jq to be installed

set -e

AWS_REGION="us-east-1"

echo "Starting AWS resource cleanup..."

# Get API Gateway IDs and delete them
echo "Cleaning up API Gateway..."
API_IDS=$(aws apigateway get-rest-apis --region $AWS_REGION --query 'items[?contains(name, `price-tracker`)].id' --output text)
for API_ID in $API_IDS; do
    echo "Deleting API Gateway: $API_ID"
    aws apigateway delete-rest-api --region $AWS_REGION --rest-api-id $API_ID
    echo "Waiting 30 seconds before next deletion..."
    sleep 30
done

# Delete Lambda functions
echo "Cleaning up Lambda functions..."
FUNCTIONS="price-tracker-app-starter auth-login auth-signup"
for FUNC in $FUNCTIONS; do
    if aws lambda get-function --function-name $FUNC --region $AWS_REGION >/dev/null 2>&1; then
        echo "Deleting Lambda function: $FUNC"
        aws lambda delete-function --function-name $FUNC --region $AWS_REGION
    fi
done

# Delete DynamoDB tables
echo "Cleaning up DynamoDB tables..."
TABLES="app_status users"
for TABLE in $TABLES; do
    if aws dynamodb describe-table --table-name $TABLE --region $AWS_REGION >/dev/null 2>&1; then
        echo "Deleting DynamoDB table: $TABLE"
        aws dynamodb delete-table --table-name $TABLE --region $AWS_REGION
        echo "Waiting 10 seconds before next operation..."
        sleep 10
    fi
done

# Delete S3 bucket
echo "Cleaning up S3 bucket..."
BUCKET_NAME="price-tracker-landing"
if aws s3api head-bucket --region $AWS_REGION --bucket $BUCKET_NAME 2>/dev/null; then
    echo "Emptying S3 bucket: $BUCKET_NAME"
    aws s3 rm s3://$BUCKET_NAME --recursive --region $AWS_REGION
    echo "Deleting S3 bucket: $BUCKET_NAME"
    aws s3api delete-bucket --region $AWS_REGION --bucket $BUCKET_NAME
fi

# Delete CloudFront distribution
echo "Cleaning up CloudFront distribution..."
DISTRO_ID=$(aws cloudfront list-distributions --region $AWS_REGION --query "DistributionList.Items[?contains(Origins.Items[0].DomainName, 'price-tracker')].Id" --output text)
if [ ! -z "$DISTRO_ID" ] && [ "$DISTRO_ID" != "None" ]; then
    echo "Disabling CloudFront distribution: $DISTRO_ID"
    
    # Check if distribution is already disabled
    ENABLED=$(aws cloudfront get-distribution --region $AWS_REGION --id $DISTRO_ID --query 'Distribution.Status' --output text)
    if [ "$ENABLED" = "Deployed" ]; then
        # Get current config and ETag
        CONFIG=$(aws cloudfront get-distribution-config --region $AWS_REGION --id $DISTRO_ID)
        ETAG=$(echo "$CONFIG" | jq -r '.ETag')
        
        # Extract distribution config, set Enabled to false, and remove ETag
        echo "$CONFIG" | jq '.DistributionConfig | .Enabled = false' > cf_config.json
        
        # Update the distribution
        aws cloudfront update-distribution \
            --region $AWS_REGION \
            --id $DISTRO_ID \
            --distribution-config file://cf_config.json \
            --if-match "$ETAG"
        
        rm -f cf_config.json
    fi
    
    echo "Waiting for distribution to be disabled..."
    # Check status every 30 seconds for up to 15 minutes
    for i in {1..30}; do
        STATUS=$(aws cloudfront get-distribution --region $AWS_REGION --id $DISTRO_ID --query 'Distribution.Status' --output text)
        echo "Current status: $STATUS (attempt $i of 30)"
        if [ "$STATUS" = "Deployed" ]; then
            break
        fi
        echo "Waiting 30 seconds..."
        sleep 30
    done
    
    echo "Deleting CloudFront distribution"
    # Get new ETag after disable
    NEW_ETAG=$(aws cloudfront get-distribution-config --region $AWS_REGION --id $DISTRO_ID --query 'ETag' --output text)
    aws cloudfront delete-distribution --region $AWS_REGION --id $DISTRO_ID --if-match "$NEW_ETAG"
else
    echo "No CloudFront distribution found to clean up"
fi

# Delete VPC resources
echo "Cleaning up VPC resources..."
if [ -f "aws/core_infra_config.sh" ]; then
    source aws/core_infra_config.sh
    
    # Delete security group
    if [ ! -z "$SECURITY_GROUP_ID" ]; then
        echo "Deleting security group: $SECURITY_GROUP_ID"
        aws ec2 delete-security-group --region $AWS_REGION --group-id $SECURITY_GROUP_ID
    fi
    
    # Delete subnet
    if [ ! -z "$SUBNET_ID" ]; then
        echo "Deleting subnet: $SUBNET_ID"
        aws ec2 delete-subnet --region $AWS_REGION --subnet-id $SUBNET_ID
    fi
    
    # Delete VPC
    if [ ! -z "$VPC_ID" ]; then
        echo "Deleting VPC: $VPC_ID"
        aws ec2 delete-vpc --region $AWS_REGION --vpc-id $VPC_ID
    fi
fi

# Delete IAM roles
echo "Cleaning up IAM roles..."
ROLES="price-tracker-lambda-role"
for ROLE in $ROLES; do
    if aws iam get-role --region $AWS_REGION --role-name $ROLE >/dev/null 2>&1; then
        echo "Detaching policies from role: $ROLE"
        # Delete inline policies
        for POLICY in $(aws iam list-role-policies --role-name $ROLE --query 'PolicyNames[*]' --output text); do
            echo "Deleting inline policy: $POLICY"
            aws iam delete-role-policy --role-name $ROLE --policy-name $POLICY
        done

        # Detach managed policies
        for POLICY in $(aws iam list-attached-role-policies --region $AWS_REGION --role-name $ROLE --query 'AttachedPolicies[*].PolicyArn' --output text); do
            aws iam detach-role-policy --region $AWS_REGION --role-name $ROLE --policy-arn $POLICY
        done

        echo "Waiting for policies to detach..."
        sleep 10

        echo "Deleting role: $ROLE"
        aws iam delete-role --region $AWS_REGION --role-name $ROLE
    fi
done

echo "Cleanup complete!" 