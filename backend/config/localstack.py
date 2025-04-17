"""LocalStack configuration module."""
import os

# LocalStack endpoint configuration
LOCALSTACK_ENDPOINT_PORT = int(os.getenv('LOCALSTACK_ENDPOINT_PORT', '4566'))
LOCALSTACK_ENDPOINT_URL = os.getenv('LOCALSTACK_ENDPOINT_URL', f'http://localhost:{LOCALSTACK_ENDPOINT_PORT}')

# AWS configuration for LocalStack
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
AWS_ENDPOINT_URL = os.getenv('AWS_ENDPOINT_URL', LOCALSTACK_ENDPOINT_URL)

# Common resource names
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'scraper-data-bucket')
SNS_TOPIC_NAME = os.getenv('SNS_TOPIC_NAME', 'report-notifications-topic')
LAMBDA_ROLE_NAME = os.getenv('LAMBDA_ROLE_NAME', 'lambda-role')

def get_boto3_client(service_name):
    """Get a boto3 client configured for LocalStack."""
    import boto3
    return boto3.client(
        service_name,
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_DEFAULT_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

def get_boto3_resource(service_name):
    """Get a boto3 resource configured for LocalStack."""
    import boto3
    return boto3.resource(
        service_name,
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_DEFAULT_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    ) 