#!/bin/bash

# Create temporary directory for function code
rm -rf package
mkdir -p package

# Copy only the necessary files
cp scraper_orchestrator.py package/
cp __init__.py package/

# Create zip file
cd package
zip -r ../scraper_orchestrator.zip .

# Clean up
cd ..
rm -rf package

echo "Created scraper_orchestrator.zip" 