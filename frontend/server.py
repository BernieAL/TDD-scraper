from flask import Flask, render_template, request, jsonify, send_from_directory
import boto3
import json
import os
import logging
from datetime import datetime
import sys

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'web_server.log'))
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__, 
            template_folder=os.path.join(SCRIPT_DIR, 'templates'),
            static_folder=os.path.join(SCRIPT_DIR, 'static'))

# Configure AWS clients for LocalStack
lambda_client = boto3.client(
    'lambda',
    endpoint_url='http://localhost:4567',
    region_name='us-east-1',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4567',
    region_name='us-east-1',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

@app.route('/')
def index():
    logger.info("Form page accessed")
    return render_template('form.html')

@app.route('/submit', methods=['POST', 'OPTIONS'])
def submit_form():
    if request.method == 'OPTIONS':
        # Handle preflight request
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        response.headers.add('Access-Control-Allow-Methods', 'POST')
        return response

    try:
        # Get form data
        brand = request.form.get('brand')
        category = request.form.get('category')
        specific_item = request.form.get('specific_item')
        email = request.form.get('email')
        
        logger.info(f"Form submission received - Brand: {brand}, Category: {category}, Email: {email}")
        
        if not all([brand, category, email]):
            logger.warning("Missing required fields in form submission")
            return jsonify({'error': 'Missing required fields'}), 400
            
        # Generate a unique query hash
        query_hash = f"{brand}_{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Generated query hash: {query_hash}")
        
        # Create event for Lambda
        event = {
            'query_hash': query_hash,
            'brand': brand,
            'category': category,
            'email': email
        }
        
        if specific_item:
            event['specific_item'] = specific_item
        
        logger.info(f"Invoking Lambda with event: {json.dumps(event, indent=2)}")
        
        # Invoke the orchestrator Lambda
        response = lambda_client.invoke(
            FunctionName='scrape-orchestrator',
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(event)
        )
        
        logger.info(f"Lambda invocation response: {response}")
        logger.info(f"Form submitted successfully. Query hash: {query_hash}")
        
        return jsonify({
            'message': 'Form submitted successfully',
            'query_hash': query_hash
        })
        
    except Exception as e:
        logger.error(f"Error processing form submission: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting Flask server...")
    app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False) 