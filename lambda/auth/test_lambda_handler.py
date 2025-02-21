import json
import boto3
import os

# Set required environment variables before importing lambda_handler
os.environ['JWT_SECRET'] = 'test-secret'

from moto import mock_dynamodb
from lambda_handler import login, signup

@mock_dynamodb
def test_login():
    # Create mock DynamoDB table
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    dynamodb.create_table(
        TableName='users',
        KeySchema=[{'AttributeName': 'email', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'email', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )
    
    # Add test user to table
    dynamodb.put_item(
        TableName='users',
        Item={
            'email': {'S': 'test@example.com'},
            'password': {'S': 'password123'},
            'user_id': {'S': 'test123'}
        }
    )

    # Test login
    event = {
        'body': json.dumps({
            'email': 'test@example.com',
            'password': 'password123'
        })
    }
    
    response = login(event, None)
    assert response['statusCode'] == 200 

@mock_dynamodb
def test_login_invalid_password():
    # Create mock DynamoDB table and user
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    dynamodb.create_table(
        TableName='users',
        KeySchema=[{'AttributeName': 'email', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'email', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )
    
    dynamodb.put_item(
        TableName='users',
        Item={
            'email': {'S': 'test@example.com'},
            'password': {'S': 'correctpassword'},
            'user_id': {'S': 'test123'}
        }
    )

    # Test login with wrong password
    event = {
        'body': json.dumps({
            'email': 'test@example.com',
            'password': 'wrongpassword'
        })
    }
    
    response = login(event, None)
    assert response['statusCode'] == 401

@mock_dynamodb
def test_signup():
    # Create mock DynamoDB table
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    dynamodb.create_table(
        TableName='users',
        KeySchema=[{'AttributeName': 'email', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'email', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Test signup
    event = {
        'body': json.dumps({
            'email': 'newuser@example.com',
            'password': 'newpassword123'
        })
    }
    
    response = signup(event, None)
    assert response['statusCode'] == 200 

@mock_dynamodb
def test_signup_duplicate_email():
    # Create mock DynamoDB table
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    dynamodb.create_table(
        TableName='users',
        KeySchema=[{'AttributeName': 'email', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'email', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Add existing user
    dynamodb.put_item(
        TableName='users',
        Item={
            'email': {'S': 'existing@example.com'},
            'password': {'S': 'password123'},
            'user_id': {'S': 'existing123'}
        }
    )

    # Try to signup with same email
    event = {
        'body': json.dumps({
            'email': 'existing@example.com',
            'password': 'newpassword'
        })
    }
    
    response = signup(event, None)
    assert response['statusCode'] == 400 



#test invalid 