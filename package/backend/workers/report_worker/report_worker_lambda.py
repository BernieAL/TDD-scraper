"""
Report worker Lambda function for generating and storing price analysis reports.
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

from .report_generator import ReportGenerator

logger = logging.getLogger(__name__)

def validate_event(event: Dict[str, Any]) -> bool:
    """Validate the incoming event.
    
    Args:
        event: The Lambda event
        
    Returns:
        bool: True if event is valid, False otherwise
    """
    required_fields = ['email', 'query_hash', 'bucket_name', 'sns_topic_arn', 'analysis_results']
    return all(field in event for field in required_fields)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for the report worker.
    
    Args:
        event: The Lambda event
        context: The Lambda context
        
    Returns:
        Dict containing the response
    """
    try:
        # Validate event
        if not validate_event(event):
            logger.error("Invalid event: missing required fields")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid event: missing required fields"})
            }
        
        # Initialize AWS clients
        s3 = boto3.client('s3')
        sns = boto3.client('sns')
        
        # Generate report
        report_generator = ReportGenerator(event['analysis_results'])
        report_path = report_generator.save_report(event['bucket_name'], s3)
        
        # Send notification
        sns.publish(
            TopicArn=event['sns_topic_arn'],
            Message=json.dumps({
                "email": event['email'],
                "query_hash": event['query_hash'],
                "report_path": report_path,
                "timestamp": datetime.now().isoformat()
            }),
            Subject="Price Analysis Report Ready"
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Report generated successfully",
                "report_path": report_path
            })
        }
        
    except ClientError as e:
        logger.error(f"AWS error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"AWS error: {str(e)}"})
        }
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error generating report: {str(e)}"})
        } 