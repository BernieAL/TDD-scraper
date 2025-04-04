#!/bin/bash

# Set up Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install pytest pytest-cov moto boto3

# Set environment variables
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export S3_BUCKET=tdd-scraper-test
export AWS_ENDPOINT_URL=http://localhost:4566

# Run tests with coverage
pytest backend/tests/lambda_functions/scrape_orchestrator/test_pipeline_integration.py \
    --cov=backend.aws.lambda_functions.scrape_orchestrator \
    --cov-report=term-missing \
    --cov-report=html

# Check test results
if [ $? -eq 0 ]; then
    echo "All tests passed successfully!"
    echo "Coverage report generated in htmlcov/index.html"
else
    echo "Some tests failed. Please check the output above."
    exit 1
fi

# Deactivate virtual environment
deactivate 