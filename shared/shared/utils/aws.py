"""AWS utilities shared across components."""

import boto3
from ..config import AWS_REGION, AWS_ENDPOINT_URL, IS_LOCAL

def get_boto3_client(service_name):
    """Get a boto3 client for the specified service."""
    kwargs = {
        "region_name": AWS_REGION,
    }
    
    if IS_LOCAL:
        kwargs["endpoint_url"] = AWS_ENDPOINT_URL
        # For local testing
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    
    return boto3.client(service_name, **kwargs)

def get_boto3_resource(service_name):
    """Get a boto3 resource for the specified service."""
    kwargs = {
        "region_name": AWS_REGION,
    }
    
    if IS_LOCAL:
        kwargs["endpoint_url"] = AWS_ENDPOINT_URL
        # For local testing
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    
    return boto3.resource(service_name, **kwargs) 