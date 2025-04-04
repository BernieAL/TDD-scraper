"""
This lambda function runs in response to form being submitted
- Receives form data
- Generates query hash
- Places form data inside s3 bucket for later use
- Triggers pipeline orchestrator
"""

import boto3
import json
from datetime import datetime
import hashlib
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def gen_query_hash(brand, category, specific_item=None):
    """Generate a unique hash for the search query"""
    current_date = datetime.now().strftime('%Y-%d-%m')
    query = f"{brand}_{category}"
    hash_string = f"{query}_{specific_item}_{current_date}" if specific_item else f"{query}_{current_date}"
    return hashlib.md5(hash_string.encode()).hexdigest()[:12]

def handle_form_submit(event, context):
    try:
        logger.info(f"Received form submission event: {json.dumps(event)}")
        
        # Extract and clean form data
        form_data = {
            'brand': event['brand'].strip().upper(),
            'category': event['category'].strip().upper(),
            'specific_item': event.get('specific_item', '').strip().upper() or None,
            'email': event['user_email']
        }
        logger.info(f"Cleaned form data: {json.dumps(form_data)}")

        # Generate query hash
        query_hash = gen_query_hash(
            form_data['brand'],
            form_data['category'],
            form_data['specific_item']
        )
        logger.info(f"Generated query hash: {query_hash}")

        # Add query hash to form data
        form_data['query_hash'] = query_hash

        # Store in S3
        logger.info(f"Storing form data in S3 bucket: scraper-data-bucket, key: queries/{query_hash}/form-params.json")
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/form-params.json',
            Body=json.dumps(form_data)
        )
        logger.info("Successfully stored form data in S3")

        # Trigger pipeline orchestrator with query_hash as payload
        logger.info(f"Triggering scrape-orchestrator Lambda with query_hash: {query_hash}")
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName='scrape-orchestrator',
            InvocationType='Event',
            Payload=json.dumps({'query_hash': query_hash})  # Pass query_hash for use as key
        )
        logger.info("Successfully triggered scrape-orchestrator Lambda")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Form submission processed',
                'query_hash': query_hash
            })
        }

    except Exception as e:
        logger.error(f"Error processing form submission: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

