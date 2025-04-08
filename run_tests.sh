#!/bin/bash

# Exit on error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
TEST_BUCKET="tdd-scraper-test"
LOCALSTACK_ENDPOINT="http://localhost:4567"
PYTHONPATH=$PYTHONPATH:$(pwd)

# Function to check if LocalStack is ready
check_localstack() {
    echo -e "${YELLOW}Waiting for LocalStack to be ready...${NC}"
    max_retries=30
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if aws --endpoint-url=$LOCALSTACK_ENDPOINT s3 ls s3://$TEST_BUCKET 2>/dev/null; then
            echo -e "${GREEN}LocalStack is ready!${NC}"
            return 0
        fi
        
        echo -e "${YELLOW}LocalStack not ready yet, retrying... ($((retry_count + 1))/$max_retries)${NC}"
        sleep 2
        retry_count=$((retry_count + 1))
    done
    
    echo -e "${RED}LocalStack failed to start within the timeout period${NC}"
    return 1
}

# Function to setup test environment
setup_environment() {
    echo -e "${YELLOW}Setting up test environment...${NC}"
    
    # Start LocalStack
    if command -v docker-compose &> /dev/null; then
        docker-compose up -d
    else
        docker compose up -d
    fi
    
    # Wait for LocalStack
    if ! check_localstack; then
        echo -e "${RED}Failed to start LocalStack${NC}"
        exit 1
    fi
    
    # Configure AWS CLI for LocalStack
    aws configure set aws_access_key_id test
    aws configure set aws_secret_access_key test
    aws configure set region us-east-1
    aws configure set output json
    
    # Create test bucket
    aws --endpoint-url=$LOCALSTACK_ENDPOINT s3 mb s3://$TEST_BUCKET
    
    echo -e "${GREEN}Test environment setup complete!${NC}"
}

# Function to run tests
run_tests() {
    echo -e "${YELLOW}Running tests...${NC}"
    
    # Run tests with coverage
    pytest backend/tests/lambda_functions/scrape_orchestrator/test_pipeline_integration.py \
        --cov=backend.workers.scraper_worker.scraper_orchestrator \
        --cov-report=term-missing \
        --cov-report=html
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
    else
        echo -e "${RED}Tests failed!${NC}"
        exit 1
    fi
}

# Function to cleanup
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    
    # Stop LocalStack
    if command -v docker-compose &> /dev/null; then
        docker-compose down
    else
        docker compose down
    fi
    
    echo -e "${GREEN}Cleanup complete!${NC}"
}

# Main execution
trap cleanup EXIT

setup_environment
run_tests 