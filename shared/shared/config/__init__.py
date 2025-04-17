"""Shared configuration for TDD Scraper components."""

import os
from pathlib import Path

# Environment
ENV = os.getenv("ENV", "local")
IS_LOCAL = ENV == "local"

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
if IS_LOCAL:
    AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
else:
    AWS_ENDPOINT_URL = None

# S3 Configuration
S3_BUCKET = "tdd-scraper-local" if IS_LOCAL else "tdd-scraper-prod"

# Lambda Function Names
LAMBDA_FUNCTIONS = {
    "scraper_worker": "scraper-worker",
    "price_analyzer": "price-analyzer",
    "report_generator": "report-generator"
}

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
TEMPLATES_DIR = PROJECT_ROOT / "templates"
