import json
import logging
import boto3
from typing import Dict, Any
from report_generator import ReportGenerator

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to generate reports from analysis results
    """
    try:
        logger.info("Starting report generation")
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Validate event
        if not validate_event(event):
            raise ValueError("Invalid event format")
            
        # Extract data from event
        analysis_results = event['analysis_results']
        email = event['email']
        query_hash = event['query_hash']
        bucket_name = event['bucket_name']
        
        # Generate report
        report_generator = ReportGenerator()
        report = report_generator.generate_report(analysis_results, email, query_hash)
        
        # Store report in S3
        report_key = f"reports/{query_hash}/report.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=report_key,
            Body=json.dumps(report),
            ContentType='application/json'
        )
        
        # Send notification
        sns_client.publish(
            TopicArn=event['sns_topic_arn'],
            Message=json.dumps({
                'type': 'report_generated',
                'query_hash': query_hash,
                'email': email,
                'report_path': report_key
            })
        )
        
        logger.info(f"Report generated and stored at {report_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Report generated successfully',
                'report_path': report_key
            })
        }
        
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

def validate_event(event: Dict[str, Any]) -> bool:
    """
    Validate the event format
    """
    required_fields = ['analysis_results', 'email', 'query_hash', 'bucket_name', 'sns_topic_arn']
    return all(field in event for field in required_fields) 