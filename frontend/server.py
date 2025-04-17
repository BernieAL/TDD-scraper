"""Frontend server module."""
import json
import logging
from datetime import datetime
import sys
import boto3
from flask import Flask, render_template, request, jsonify
from backend.config.localstack import get_boto3_client

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configure AWS clients for LocalStack
lambda_client = get_boto3_client('lambda')
s3_client = get_boto3_client('s3')

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
        # Get data from either form data or JSON
        if request.is_json:
            data = request.get_json()
            brand = data.get('brand')
            category = data.get('category')
            specific_item = data.get('specific_item')
            email = data.get('email')
        else:
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
        
        # Create form data dictionary
        form_data = {
            'brand': brand,
            'category': category,
            'email': email
        }
        
        if specific_item:
            form_data['specific_item'] = specific_item
        
        # Create event for Lambda with correct structure
        event = {
            'query_hash': query_hash,
            'form_data': form_data
        }
        
        logger.info(f"Invoking Lambda with event: {json.dumps(event, indent=2)}")
        
        # Create Lambda client and invoke the orchestrator Lambda
        response = lambda_client.invoke(
            FunctionName='scraper-worker',
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
    app.run(debug=True) 