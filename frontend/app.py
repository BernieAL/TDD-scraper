from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import requests
import os

app = Flask(__name__)
CORS(app)

# Serve the form page
@app.route('/')
def serve_form():
    return send_from_directory('pages', 'new_search.html')

# Handle form submission
@app.route('/submit', methods=['POST', 'OPTIONS'])
def handle_form_submission():
    if request.method == 'OPTIONS':
        # Handle preflight request
        response = jsonify({'status': 'ok'})
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
        return response

    try:
        # Forward the request to LocalStack Lambda
        lambda_url = 'http://localhost:4567/2015-03-31/functions/form-submission/invocations'
        response = requests.post(lambda_url, json=request.json)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000) 