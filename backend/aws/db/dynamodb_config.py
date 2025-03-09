import os
import boto3
from botocore.exceptions import ClientError
from simple_chalk import chalk
from backend.config.config import get_env_var
from .table_schemas import TABLE_SCHEMAS



class DynamoDB:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DynamoDB, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def setup_dynamodb_client(self):
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

            self.users_table= self.client.Table(get_env_var('PRICE_HISTORY_TABLE', 'price-history-table'))
            
            self.user_searches_table = self.client.Table(get_env_var('PRICE_HISTORY_TABLE', 'price-history-table'))

        except Exception as e:
            print(chalk.red(f"Error initializing DynamoDB: {e}"))
            raise
    
    @property
    def tables(self):
        """Get all table references"""
        return {
            'products': self.products_table,
            'price_history': self.price_history_table
        }
    
   
    def create_tables(self):
        """Create tables if they don't exist"""
        for table_name, schema in TABLES.items():
            try:
                self.client.create_table(
                    TableName=schema.name,
                    KeySchema=[
                        {'AttributeName': schema.partition_key, 'KeyType': 'HASH'},
                        {'AttributeName': schema.sort_key, 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': name, 'AttributeType': type_}
                        for name, type_ in schema.attributes.items()
                        if name in [schema.partition_key, schema.sort_key]
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                print(f"Created table: {schema.name}")
            except self.client.exceptions.ResourceInUseException:
                print(f"Table already exists: {schema.name}")
    
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

    async def add_price_history(self, product_id: str, source: str, price: float):
        """Add price to product's price history"""
        try:
            await self.products_table.update_item(
                Key={
                    'PK': f'PROD#{product_id}',
                    'SK': f'SOURCE#{source}'
                },
                UpdateExpression="SET price_history = list_append(if_not_exists(price_history, :empty), :price)",
                ExpressionAttributeValues={
                    ':price': [{
                        'price': price,
                        'timestamp': datetime.now().isoformat()
                    }],
                    ':empty': []
                }
            )
        except Exception as e:
            print(chalk.red(f"Error adding price history: {e}"))
            raise

    async def get_product_history(self, product_id: str, source: str):
        """Get product's price history"""
        try:
            response = await self.products_table.get_item(
                Key={
                    'PK': f'PROD#{product_id}',
                    'SK': f'SOURCE#{source}'
                }
            )
            return response.get('Item', {}).get('price_history', [])
        except Exception as e:
            print(chalk.red(f"Error getting price history: {e}"))
            raise

# Singleton instance
dynamodb = DynamoDBClient()