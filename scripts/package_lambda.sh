#!/bin/bash

# Create deployment packages for Lambda functions
echo "Creating deployment packages..."

# Form submission Lambda
cd backend/aws/lambda_functions/form_submission
zip -r deployment-package.zip form_handler.py
cd ../../../

# Scrape orchestrator Lambda
cd backend/aws/lambda_functions/scrape_orchestrator
zip -r deployment-package.zip pipeline_orchestrator.py
cd ../../../

echo "Deployment packages created!" 