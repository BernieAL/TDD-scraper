#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Set environment variables for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4567

# Install test dependencies
pip install pytest pytest-cov moto boto3

# Run integration tests with coverage
pytest backend/tests/integration/test_localstack_integration.py \
    backend/tests/integration/test_full_pipeline.py \
    --cov=backend/aws/lambda_functions/scrape_orchestrator \
    --cov-report=term \
    --cov-report=html:htmlcov/integration

# Check test results
if [ $? -eq 0 ]; then
    echo "Integration tests passed successfully!"
else
    echo "Integration tests failed!"
    exit 1
fi

# Deactivate virtual environment
deactivate 