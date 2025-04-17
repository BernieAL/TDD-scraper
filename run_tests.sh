#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display status messages
log_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

# Function to display error messages
log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

# Function to display success messages
log_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

# Function to check if LocalStack is ready
check_localstack() {
    log_status "Checking LocalStack status..."
    
    # Get container ID and status
    CONTAINER_ID=$(docker compose ps -q localstack)
    if [ -z "$CONTAINER_ID" ]; then
        log_error "LocalStack container not found"
        return 1
    fi
    
    CONTAINER_STATUS=$(docker inspect --format='{{.State.Status}}' $CONTAINER_ID)
    log_status "LocalStack container status: $CONTAINER_STATUS"
    
    if [ "$CONTAINER_STATUS" != "running" ]; then
        log_error "LocalStack container is not running"
        docker compose logs localstack
        return 1
    fi
    
    # Check health endpoint
    log_status "Checking health endpoint..."
    HEALTH_RESPONSE=$(curl -s http://localhost:4567/_localstack/health)
    if [ $? -ne 0 ]; then
        log_error "Failed to connect to LocalStack health endpoint"
        return 1
    fi
    
    log_status "Health endpoint response: $HEALTH_RESPONSE"
    
    # Check for errors in logs
    log_status "Checking LocalStack logs for errors..."
    ERROR_LOG=$(docker compose logs localstack | grep -i "error\|exception\|fail")
    if [ ! -z "$ERROR_LOG" ]; then
        log_error "Found errors in LocalStack logs:"
        echo "$ERROR_LOG"
        return 1
    fi
    
    log_success "LocalStack is ready"
    return 0
}

# Function to set up the environment
setup_environment() {
    log_status "Setting up environment..."
    
    # Clean up any existing containers and volumes
    log_status "Cleaning up existing containers..."
    docker compose down -v
    
    # Start LocalStack
    log_status "Starting LocalStack..."
    docker compose up -d
    
    # Wait for LocalStack to be ready
    local max_attempts=30
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        if check_localstack; then
            break
        fi
        log_status "Attempt $attempt of $max_attempts: Waiting for LocalStack to be ready..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -gt $max_attempts ]; then
        log_error "LocalStack failed to start properly"
        docker compose logs localstack
        exit 1
    fi
    
    # Configure AWS CLI for LocalStack
    log_status "Configuring AWS CLI for LocalStack..."
    aws configure set aws_access_key_id test
    aws configure set aws_secret_access_key test
    aws configure set region us-east-1
    aws configure set output json
    
    # Create S3 bucket
    log_status "Creating S3 bucket..."
    aws --endpoint-url=http://localhost:4567 s3 mb s3://scraper-results
    
    # Create Lambda function
    log_status "Creating Lambda function..."
    aws --endpoint-url=http://localhost:4567 lambda create-function \
        --function-name scraper-worker \
        --runtime python3.9 \
        --handler scraper_orchestrator.orchestrate_scraping_pipeline \
        --zip-file fileb://backend/workers/scraper_worker/scraper_orchestrator.zip \
        --role arn:aws:iam::000000000000:role/lambda-role
    
    log_success "Environment setup complete"
}

# Function to run tests
run_tests() {
    log_status "Running tests..."
    
    # Set PYTHONPATH to include the backend directory
    export PYTHONPATH=$PYTHONPATH:$(pwd)
    
    # Run pytest with coverage
    pytest "$@" --cov=backend.workers.scraper_worker.scraper_orchestrator --cov-report=html
    
    if [ $? -eq 0 ]; then
        log_success "Tests completed successfully"
    else
        log_error "Tests failed"
        exit 1
    fi
}

# Function to clean up
cleanup() {
    log_status "Cleaning up..."
    docker compose down -v
    log_success "Cleanup complete"
}

# Main script
if [ "$1" == "--setup-only" ]; then
    setup_environment
    exit 0
fi

# Set up environment and run tests
setup_environment
run_tests "$@"
cleanup 