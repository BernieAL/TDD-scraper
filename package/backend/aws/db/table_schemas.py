"""DynamoDB table schemas and creation script."""
from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from simple_chalk import chalk
from backend.config.localstack import get_boto3_client

@dataclass
class TableSchema:
    """DynamoDB table schema."""
    name: str
    key_schema: List[dict]
    attribute_definitions: List[dict]
    provisioned_throughput: dict

def get_table_schemas() -> List[TableSchema]:
    """Get all table schemas."""
    return [
        TableSchema(
            name='products-table',
            key_schema=[
                {
                    'AttributeName': 'product_id',
                    'KeyType': 'HASH'
                }
            ],
            attribute_definitions=[
                {
                    'AttributeName': 'product_id',
                    'AttributeType': 'S'
                }
            ],
            provisioned_throughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        ),
        TableSchema(
            name='price-history-table',
            key_schema=[
                {
                    'AttributeName': 'product_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'
                }
            ],
            attribute_definitions=[
                {
                    'AttributeName': 'product_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'
                }
            ],
            provisioned_throughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    ]

def create_table(table: TableSchema) -> None:
    """Create a DynamoDB table."""
    dynamodb = get_boto3_client('dynamodb')
    try:
        dynamodb.create_table(
            TableName=table.name,
            KeySchema=table.key_schema,
            AttributeDefinitions=table.attribute_definitions,
            ProvisionedThroughput=table.provisioned_throughput
        )
        print(chalk.green(f"Created table {table.name}"))
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(chalk.yellow(f"Table {table.name} already exists"))
        else:
            print(chalk.red(f"Error creating table {table.name}: {e}"))
            raise

def create_all_tables() -> None:
    """Create all DynamoDB tables."""
    tables = get_table_schemas()
    for table in tables:
        create_table(table)

if __name__ == "__main__":
    create_all_tables()
