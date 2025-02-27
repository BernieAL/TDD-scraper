#!/bin/bash

# Build and push Docker images
docker build -t scraper_worker -f Dockerfile.scraper_worker .
docker build -t compare_worker -f Dockerfile.compare_worker .
docker build -t price_change_worker -f Dockerfile.price_change_worker .
docker build -t report_generator -f Dockerfile.ui_container .

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REPO

# Tag and push all images
for image in scraper_worker compare_worker price_change_worker report_generator; do
    docker tag $image:latest $ECR_REPO/$image:latest
    docker push $ECR_REPO/$image:latest
done

# Register all task definitions
aws ecs register-task-definition --cli-input-json file://aws/task-definitions/scraper-task.json
aws ecs register-task-definition --cli-input-json file://aws/task-definitions/analysis-task.json
aws ecs register-task-definition --cli-input-json file://aws/task-definitions/report-task.json

# Update Lambda function
zip -r function.zip lambda/scrape_orchestrator/*
aws lambda update-function-code \
    --function-name scrape-orchestrator \
    --handler handler.orchestrate_scraping_pipeline \
    --zip-file fileb://function.zip 