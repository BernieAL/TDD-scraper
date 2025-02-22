import uuid
import boto3
import json
import os
import jwt
from datetime import datetime, timedelta
import random

from dotenv import load_dotenv, find_dotenv
import smtplib
import ssl
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from simple_chalk import chalk

load_dotenv(find_dotenv())

# Configuration
port = 465  # For SSL
app_password = os.getenv("GOOGLE_APP_PW")
sender_email = os.getenv("GOOGLE_SENDER_EMAIL")
subject = "Daily Price Change + Sold Reports"

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
SECRET_KEY = os.environ['JWT_SECRET']

def login(event, context):
    body = json.loads(event['body'])
    email = body['email']
    password = body['password']
    
    # Get user from DynamoDB
    response = dynamodb.get_item(
        TableName='users',
        Key={'email': {'S': email}}
    )
    
    if 'Item' not in response:
        return {
            'statusCode': 401,
            'body': json.dumps({'message': 'Invalid credentials'})
        }
    
    user = response['Item']
    # In production, use proper password hashing
    if user['password']['S'] != password:
        return {
            'statusCode': 401,
            'body': json.dumps({'message': 'Invalid credentials'})
        }
    
    # Generate JWT token
    token = jwt.encode({
        'user_id': user['user_id']['S'],
        'email': email,
        'exp': datetime.utcnow() + timedelta(days=1)
    }, SECRET_KEY, algorithm='HS256')
    
    return {
        'statusCode': 200,
        'body': json.dumps({'token': token})
    }

def signup(event, context,regenerate_code=False):
    body = json.loads(event['body'])
    email = body['email']
    password = body['password']
    
    verification_code = generate_verification_code()
    expiration_time = int((datetime.utcnow() + timedelta(minutes=15)).timestamp())
    
    if regenerate_code == True:
        verification_code = generate_verification_code()
        expiration_time = int((datetime.utcnow() + timedelta(minutes=15)).timestamp())
    
    # Store unverified user with verification code
    dynamodb.put_item(
        TableName='users',
        Item={
            'email': {'S': email},
            'password': {'S': password},
            'verification_code': {'S': verification_code},
            'code_expiry': {'N': str(expiration_time)},
            'verified': {'BOOL': False}
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'success': True})
    }

def generate_verification_code():
    return str(random.randint(100000, 999999)) 

def send_verification_email(email, code):
    # Implement email sending logic here
    # This is a placeholder implementation
     # Create a secure SSL context
    context = ssl.create_default_context()

    # Email message setup
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = email
    message['Subject'] = subject

    body = f"""
            Please copy and paste this 
            code back on code verification page
            to confirm your email: CODE  - {code}
            """
    body.append(f"""
            IMPORTANT: CODE EXPIRES IN 15 MINUTES, YOU WILL NEED TO REQUEST A NEW ONE.
            """)
        
    message.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, email, message.as_string())
        print(chalk.green(f"SUCCESSFULLY SENT MESSAGE TO EMAIL: {email}"))
        return True
    except Exception as e:
        print(chalk.red(f"There was an error sending the email: {e}"))
        return False

    print(f"Sending verification email to {email} with code {code}")

def verify_email(event,context):
    body = json.loads(event['body'])
    email = body['email']
    code = body['code']

    #check code and expiration
    response = dynamodb.get_item(
        TableName='users',
        Key={'email':{'S':email}}
    )

    if 'Item' not in response:
        return {'statusCode': 404}
    
    user = response['Item']

    #check for non match or expired code
    if(user['verification_code']['S'] != code or 
       int(user['code_expiry']['N']) < int(datetime.utcnow().timestamp())):
         return {'statusCode': 400}
    
    #update user to verified status if no error thrown
    user_id=str(uuid.uuid4())
    dynamodb.update_item(
        TableName='users',
        Key={'email':{'S':email}},
        UpdateExpression='SET verified = :v, user_id = :u',
        ExpressionAttributeValues={
            ':v': {'BOOL': True},
            ':u': {'S': user_id}
        }
    )
    

    return {
        'statusCode':200,
        'body':json.dumps({'message':'Email verified successfully'})
    }
