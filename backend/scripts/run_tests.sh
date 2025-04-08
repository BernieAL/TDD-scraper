#!/bin/bash

# Set up environment
export PYTHONPATH=$PYTHONPATH:$(pwd)
export PYTHONPATH=$PYTHONPATH:$(pwd)/backend
export ENVIRONMENT=test

# Create logs directory if it doesn't exist
mkdir -p logs

# Run tests with coverage
echo "Running tests with coverage..."
pytest \
    --cov=backend \
    --cov-report=term-missing \
    --cov-report=html:coverage_report \
    --log-cli-level=DEBUG \
    --log-file=logs/test.log \
    --log-file-level=DEBUG \
    -c pytest.ini \
    tests/

# Check test results
if [ $? -eq 0 ]; then
    echo "All tests passed successfully!"
    echo "Coverage report generated in coverage_report/"
    echo "Detailed logs available in logs/test.log"
else
    echo "Some tests failed. Check logs/test.log for details."
    exit 1
fi 