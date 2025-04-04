"""
CORS Proxy Server for LocalStack Lambda Functions

This proxy server acts as a middleware between the frontend application and LocalStack Lambda functions.
It solves the Cross-Origin Resource Sharing (CORS) issues that arise when making direct requests from 
a web browser to LocalStack endpoints.

Why is this needed?
1. Browser Security: Modern browsers enforce Same-Origin Policy, which prevents web pages from making 
   direct requests to different domains/ports for security reasons.
2. LocalStack Limitations: LocalStack's Lambda endpoint doesn't include CORS headers by default, making 
   it impossible for browser-based JavaScript to directly access these endpoints.

How it works:
1. Frontend makes requests to this proxy server instead of directly to LocalStack
2. Proxy server adds necessary CORS headers to allow browser requests
3. Proxy forwards requests to LocalStack and returns responses to frontend
4. Handles OPTIONS preflight requests that browsers send before actual POST requests

Usage:
1. Start the proxy server: python proxy.py
2. Frontend can then make requests to http://localhost:5000/lambda/form-submission
3. Proxy will forward these requests to http://localhost:4567/2015-03-31/functions/form-submission/invocations

Dependencies:
- Flask: Web framework for creating the proxy server
- Flask-CORS: Flask extension for handling CORS
- Requests: For making HTTP requests to LocalStack
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

@app.route('/lambda/form-submission', methods=['POST', 'OPTIONS'])
def proxy_to_lambda():
    if request.method == 'OPTIONS':
        # Handle preflight request
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
        return response

    # Forward the request to LocalStack Lambda
    try:
        lambda_url = 'http://localhost:4567/2015-03-31/functions/form-submission/invocations'
        response = requests.post(lambda_url, json=request.json)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000) 