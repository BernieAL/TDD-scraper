import json
import logging
import os
from typing import Dict, Any
import boto3
from workers.scraper_worker.analysis import PriceAnalyzer

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def load_s3_data(bucket: str, key: str) -> Dict:
    """Load scraped data from S3."""
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error loading data from S3: {str(e)}")
        raise

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for price analysis.
    
    Expected event structure:
    {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "bucket-name"
                    },
                    "object": {
                        "key": "path/to/scraped/data.json"
                    }
                }
            }
        ]
    }
    """
    try:
        # Get environment variables
        dynamodb_table = os.environ['PRICE_HISTORY_TABLE']
        sns_topic_arn = os.environ['PRICE_ALERT_TOPIC']
        
        # Initialize analyzer
        analyzer = PriceAnalyzer(dynamodb_table, sns_topic_arn)
        
        # Process each record
        for record in event['Records']:
            # Get S3 information
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            logger.info(f"Processing file s3://{bucket}/{key}")
            
            # Load scraped data
            scraped_data = load_s3_data(bucket, key)
            
            # Analyze price changes
            price_changes = analyzer.analyze_price_changes(scraped_data)
            
            if price_changes:
                # Generate and send report
                report = analyzer.generate_price_report(price_changes)
                analyzer.send_report(report)
                
                logger.info(f"Processed {len(scraped_data)} products, found {len(price_changes)} price changes")
            else:
                logger.info(f"Processed {len(scraped_data)} products, no significant price changes found")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Price analysis completed successfully',
                'products_processed': len(scraped_data),
                'price_changes_found': len(price_changes)
            })
        }
        
    except Exception as e:
        logger.error(f"Error in price analysis: {str(e)}")
        raise 