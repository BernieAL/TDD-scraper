"""
SNS Client Configuration

This module provides a centralized interface for SNS interactions in the price comparison application.
It handles client configuration and topic management.

The SNSClient class serves as a repository pattern implementation, encapsulating all SNS-related
operations and configurations in one place.

Typical usage:
    sns = SNSClient()
    await sns.publish_message(topic_arn, message)

Environment Variables:
    AWS_REGION (str): AWS region for SNS (default: 'us-east-1')
    AWS_SAM_LOCAL (bool): Flag for local SAM testing
    IS_LOCAL (bool): Flag for local development
"""

import boto3
from typing import Dict, Any
from botocore.exceptions import ClientError
from pathlib import Path
import sys
import json

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from backend.config.config import get_env_var
from simple_chalk import chalk
from functools import wraps

def handle_sns_error(func):
    """Decorator for handling SNS errors"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(chalk.red(f"SNS error in {func.__name__}: {error_code}"))
            raise
        except Exception as e:
            print(chalk.red(f"Unexpected error in {func.__name__}: {str(e)}"))
            raise
    return wrapper

class SNSClient:
    """
    Manages SNS configuration and operations.
    
    This class handles:
    1. Client setup for both local and production environments
    2. Topic reference management
    3. Message publishing operations
    
    Attributes:
        client: Boto3 SNS client
        region (str): AWS region for SNS
        topics (Dict): References to all SNS topics
    """
    
    def __init__(self):
        """Initialize SNS client"""
        self.setup_sns_client()

    def setup_sns_client(self):
        """Initialize SNS connection"""
        try:
            self.region = get_env_var('AWS_REGION', 'us-east-1')
            self.is_local = get_env_var('AWS_SAM_LOCAL') or get_env_var('IS_LOCAL')
            
            # Configure client for local or production
            if self.is_local:
                self.client = boto3.client(
                    'sns',
                    endpoint_url='http://localhost:4567',
                    region_name=self.region,
                    aws_access_key_id='test',
                    aws_secret_access_key='test'
                )
            else:
                self.client = boto3.client('sns', region_name=self.region)
            
        except Exception as e:
            print(chalk.red(f"Error initializing SNS: {e}"))
            raise

    @handle_sns_error
    async def publish_message(self, topic_arn: str, message: Dict[str, Any]) -> Dict:
        """
        Publish message to SNS topic
        
        Args:
            topic_arn: ARN of the SNS topic
            message: Message to publish
            
        Returns:
            Response from SNS
        """
        return await self.client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            MessageAttributes={
                'ContentType': {
                    'DataType': 'String',
                    'StringValue': 'application/json'
                }
            }
        )

    @handle_sns_error
    async def create_topic(self, name: str) -> str:
        """
        Create SNS topic
        
        Args:
            name: Name of the topic
            
        Returns:
            Topic ARN
        """
        response = await self.client.create_topic(Name=name)
        return response['TopicArn']

    @handle_sns_error
    async def delete_topic(self, topic_arn: str) -> Dict:
        """
        Delete SNS topic
        
        Args:
            topic_arn: ARN of the topic to delete
            
        Returns:
            Response from SNS
        """
        return await self.client.delete_topic(TopicArn=topic_arn)

# Singleton instance
sns = SNSClient() 