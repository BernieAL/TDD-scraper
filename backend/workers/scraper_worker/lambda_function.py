import json
import os
import logging
import boto3
from botocore.config import Config

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_aws_client(service_name: str):
    """Get an AWS client configured for either LocalStack or production."""
    config = Config(
        region_name='us-east-1',
        retries={'max_attempts': 5, 'mode': 'standard'}
    )
    
    endpoint_url = os.environ.get('AWS_ENDPOINT_URL')
    if endpoint_url:
        logger.info(f"Using custom endpoint URL: {endpoint_url}")
        return boto3.client(
            service_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'test'),
            config=config
        )
    return boto3.client(service_name, config=config)

def lambda_handler(event, context):
    """Main Lambda handler function."""
    try:
        logger.info("Starting Lambda execution")
        logger.info(f"Received event: {json.dumps(event)}")
        logger.info(f"Environment variables: {json.dumps({k: v for k, v in os.environ.items()})}")
        
        # Validate event
        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary")
            
        required_fields = ['brand', 'category', 'query_hash']
        missing_fields = [field for field in required_fields if field not in event]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
        
        logger.info("Event validation passed")
        
        # Initialize AWS clients
        s3_client = get_aws_client('s3')
        logger.info("AWS clients initialized")
        
        # Test S3 connection
        bucket = os.environ['S3_BUCKET']
        logger.info(f"Testing S3 connection to bucket: {bucket}")
        s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        logger.info("S3 connection test successful")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'message': 'Lambda function executed successfully',
                'event': event
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error': str(e),
                'query_hash': event.get('query_hash', 'unknown')
            })
        } 