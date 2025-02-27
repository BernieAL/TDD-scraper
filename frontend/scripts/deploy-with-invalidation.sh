#!/bin/bash
# deploy-with-invalidation.sh

BUCKET_NAME="price-tracker-landing"
DISTRIBUTION_ID="E1PXQKR3Y874T0"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPT_DIR/..  # Change to frontend directory

# Upload files to S3
aws s3 sync . s3://$BUCKET_NAME \
    --exclude "*.sh" \
    --exclude "*.md" \
    --include "pages/*" \
    --include "styles/*" \
    --include "config/*" \
    --cache-control "max-age=3600"

# Invalidate CloudFront cache
aws cloudfront create-invalidation \
    --distribution-id $DISTRIBUTION_ID \
    --paths "/*"

echo "Deployment complete! Please wait a few minutes for CloudFront to update."