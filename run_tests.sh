#!/bin/bash

# Exit on error
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print section headers
print_header() {
    echo -e "\n${GREEN}=== $1 ===${NC}"
}

# Function to check if a command exists
check_command() {
    if ! command -v $1 &> /dev/null; then
        echo -e "${RED}Error: $1 is required but not installed.${NC}"
        echo -e "${YELLOW}Please install $1 and try again.${NC}"
        exit 1
    fi
}

# Function to check for docker compose
check_docker_compose() {
    if command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        echo -e "${RED}Error: Neither 'docker-compose' nor 'docker compose' is available.${NC}"
        echo -e "${YELLOW}Please install Docker Compose and try again.${NC}"
        exit 1
    fi
}

# Function to check if LocalStack is ready
wait_for_localstack() {
    print_header "Waiting for LocalStack to be ready"
    until curl -s http://localhost:4566/health | grep -q '"s3": "running"'; do
        echo "Waiting for LocalStack..."
        sleep 2
    done
    echo -e "${GREEN}LocalStack is ready!${NC}"
}

# Function to setup environment
setup_environment() {
    print_header "Setting up environment"
    
    # Set up Python virtual environment
    if [ ! -d "venv" ]; then
        python3 -m venv venv
    fi
    source venv/bin/activate
    
    # Install dependencies
    pip install -r requirements.txt
    pip install pytest pytest-cov moto boto3
    
    # Set AWS environment variables
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_DEFAULT_REGION=us-east-1
    export AWS_ENDPOINT_URL=http://localhost:4566
    export S3_BUCKET=tdd-scraper-test
}

# Function to run tests
run_tests() {
    local test_type=$1
    local test_path=$2
    local coverage_path=$3
    
    print_header "Running $test_type tests"
    
    pytest $test_path \
        --cov=$coverage_path \
        --cov-report=term-missing \
        --cov-report=html:htmlcov/$test_type
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}$test_type tests passed!${NC}"
    else
        echo -e "${RED}$test_type tests failed!${NC}"
        exit 1
    fi
}

# Main execution
print_header "Starting test suite"

# Check for required dependencies
print_header "Checking dependencies"
check_command docker
check_docker_compose
check_command python3
check_command pip
check_command curl

# Start LocalStack
$DOCKER_COMPOSE_CMD up -d localstack
wait_for_localstack

# Setup environment
setup_environment

# Create test bucket
print_header "Creating test bucket"
aws --endpoint-url=$AWS_ENDPOINT_URL s3 mb s3://$S3_BUCKET

# Run different test types
run_tests "lambda" "backend/tests/lambda_functions/scrape_orchestrator/test_pipeline_integration.py" "backend.aws.lambda_functions.scrape_orchestrator"
run_tests "integration" "backend/tests/integration/test_localstack_integration.py backend/tests/integration/test_full_pipeline.py" "backend/aws/lambda_functions/scrape_orchestrator"

# Cleanup
print_header "Cleaning up"
$DOCKER_COMPOSE_CMD down
deactivate

echo -e "\n${GREEN}All tests completed successfully!${NC}"
exit 0 