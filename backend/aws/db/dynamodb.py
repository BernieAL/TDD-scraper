import os
import boto3
from botocore.exceptions import ClientError
from simple_chalk import chalk
from backend.config.config import get_env_var

class DynamoDBClient:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DynamoDBClient, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Initialize DynamoDB connection"""
        try:
            self.region = get_env_var('AWS_REGION', 'us-east-1')
            
            # For local testing with localstack
            if get_env_var('AWS_SAM_LOCAL') or get_env_var('IS_LOCAL'):
                self.client = boto3.resource(
                    'dynamodb',
                    endpoint_url='http://localhost:8000',
                    region_name=self.region,
                    aws_access_key_id='dummy',
                    aws_secret_access_key='dummy'
                )
            else:
                # Production AWS
                self.client = boto3.resource('dynamodb', region_name=self.region)
            
            # Initialize table references
            self.products_table = self.client.Table(get_env_var('PRODUCTS_TABLE', 'products-table'))
            self.price_history_table = self.client.Table(get_env_var('PRICE_HISTORY_TABLE', 'price-history-table'))
            self.query_results_table = self.client.Table(get_env_var('QUERY_RESULTS_TABLE', 'query-results-table'))
            
        except Exception as e:
            print(chalk.red(f"Error initializing DynamoDB: {e}"))
            raise
    
    @property
    def tables(self):
        """Get all table references"""
        return {
            'products': self.products_table,
            'price_history': self.price_history_table,
            'query_results': self.query_results_table
        }
    
    async def update_product(self, product_id: str, source: str, data: dict):
        """Update product in DynamoDB"""
        try:
            update_expr = "SET "
            expr_values = {}
            
            for key, value in data.items():
                update_expr += f"#{key} = :{key}, "
                expr_values[f":{key}"] = value
            
            update_expr = update_expr.rstrip(", ")
            
            await self.products_table.update_item(
                Key={
                    'PK': f'PROD#{product_id}',
                    'SK': f'META#{source}'
                },
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_values,
                ExpressionAttributeNames={f"#{k}": k for k in data.keys()}
            )
            
        except ClientError as e:
            print(chalk.red(f"DynamoDB error updating product: {e}"))
            raise

# Singleton instance
dynamodb = DynamoDBClient()