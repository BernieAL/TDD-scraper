"""
This lambda function analyzes scraped price data against historical data in DynamoDB
- Reads scraped data from S3
- Compares with historical data in DynamoDB
- Identifies price changes
- Stores analysis results back to S3
"""

import boto3
import json
from datetime import datetime

def analyze_price_changes(scraped_data, historical_data):
    """Compare scraped prices with historical data"""
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

def handle_analysis(event, context):
    try:
        # Extract query hash from event
        query_hash = event['query_hash']
        
        # Initialize AWS clients
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb')
        
        # Read scraped data from S3
        scraped_data_response = s3.get_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/filtered/filtered_data.json'
        )
        scraped_data = json.loads(scraped_data_response['Body'].read().decode('utf-8'))
        
        # Read historical data from DynamoDB
        historical_data_response = dynamodb.scan(
            TableName='historical_prices'
        )
        historical_data = historical_data_response.get('Items', [])
        
        # Analyze price changes
        changes = analyze_price_changes(scraped_data, historical_data)
        
        # Store analysis results in S3
        analysis_result = {
            'query_hash': query_hash,
            'timestamp': datetime.now().isoformat(),
            'changes': changes,
            'total_items_analyzed': len(scraped_data),
            'items_with_changes': len(changes)
        }
        
        s3.put_object(
            Bucket='scraper-data-bucket',
            Key=f'queries/{query_hash}/analysis/price_changes.json',
            Body=json.dumps(analysis_result)
        )
        
        # Update DynamoDB with new prices
        for item in scraped_data:
            dynamodb.put_item(
                TableName='historical_prices',
                Item={
                    'id': {'S': item['id']},
                    'price': {'N': str(item['price'])},
                    'last_updated': {'S': datetime.now().isoformat()}
                }
            )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Analysis completed successfully',
                'changes_found': len(changes)
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        } 