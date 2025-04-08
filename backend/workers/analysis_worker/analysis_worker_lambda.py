"""
Analysis Worker Lambda Handler

This module handles AWS Lambda events for the analysis system. It:
- Reads scraped data from S3
- Compares with historical data in DynamoDB
- Identifies price changes
- Stores analysis results back to S3

Main Components:
- Lambda event handling
- Input validation
- Analysis orchestration
- AWS service integration (S3, DynamoDB)

Key Functions:
    lambda_handler(event, context):
        Main entry point for AWS Lambda

    validate_event(event):
        Validates incoming event structure

    analyze_price_changes(scraped_data, historical_data):
        Compares prices and identifies changes
"""

import boto3
import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, List

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def validate_event(event: Dict[str, Any]) -> None:
    """Validate the Lambda event structure."""
    required_fields = ['query_hash']
    for field in required_fields:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

def analyze_price_changes(scraped_data: List[Dict], historical_data: List[Dict]) -> List[Dict]:
    """Compare scraped prices with historical data."""
    changes = []
    
    for item in scraped_data:
        item_id = item.get('id')
        current_price = item.get('price')
        
        if not item_id or not current_price:
            continue
            
        historical_item = next(
            (h for h in historical_data if h.get('id') == item_id),
            None
        )
        
        if historical_item:
            old_price = historical_item.get('price')
            if old_price != current_price:
                changes.append({
                    'id': item_id,
                    'old_price': old_price,
                    'new_price': current_price,
                    'change_percentage': ((current_price - old_price) / old_price) * 100,
                    'timestamp': datetime.now().isoformat()
                })
        else:
            # New item
            changes.append({
                'id': item_id,
                'old_price': None,
                'new_price': current_price,
                'change_percentage': 100,
                'timestamp': datetime.now().isoformat(),
                'is_new': True
            })
    
    return changes

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler for the analysis worker.
    
    Args:
        event (dict): The Lambda event containing:
            - query_hash: Unique identifier for the analysis job
        context: Lambda context object
    
    Returns:
        dict: Response containing status and results
    """
    try:
        logger.info("=== Starting Analysis Worker Lambda ===")
        logger.info(f"Event received: {json.dumps(event)}")
        
        # Validate event
        validate_event(event)
        
        # Get environment variables
        bucket = os.environ.get('S3_BUCKET', 'scraper-data-bucket')
        dynamodb_table = os.environ.get('DYNAMODB_TABLE', 'historical_prices')
        
        # Initialize AWS clients
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb')
        
        # Read scraped data from S3
        logger.info("Reading scraped data from S3...")
        scraped_data_response = s3.get_object(
            Bucket=bucket,
            Key=f'queries/{event["query_hash"]}/filtered/filtered_data.json'
        )
        scraped_data = json.loads(scraped_data_response['Body'].read().decode('utf-8'))
        logger.info(f"Read {len(scraped_data)} items from S3")
        
        # Read historical data from DynamoDB
        logger.info("Reading historical data from DynamoDB...")
        historical_data_response = dynamodb.scan(TableName=dynamodb_table)
        historical_data = historical_data_response.get('Items', [])
        logger.info(f"Read {len(historical_data)} items from DynamoDB")
        
        # Analyze price changes
        logger.info("Analyzing price changes...")
        changes = analyze_price_changes(scraped_data, historical_data)
        logger.info(f"Found {len(changes)} price changes")
        
        # Store analysis results in S3
        logger.info("Storing analysis results in S3...")
        analysis_result = {
            'query_hash': event['query_hash'],
            'timestamp': datetime.now().isoformat(),
            'changes': changes,
            'total_items_analyzed': len(scraped_data),
            'items_with_changes': len(changes)
        }
        
        s3.put_object(
            Bucket=bucket,
            Key=f'queries/{event["query_hash"]}/analysis/price_changes.json',
            Body=json.dumps(analysis_result)
        )
        logger.info("Successfully stored analysis results")
        
        # Update DynamoDB with new prices
        logger.info("Updating DynamoDB with new prices...")
        for item in scraped_data:
            dynamodb.put_item(
                TableName=dynamodb_table,
                Item={
                    'id': {'S': item['id']},
                    'price': {'N': str(item['price'])},
                    'last_updated': {'S': datetime.now().isoformat()}
                }
            )
        logger.info("Successfully updated DynamoDB")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Analysis completed successfully',
                'changes_found': len(changes)
            })
        }
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        } 