"""

this lambda function runs in response to form being submitted

-recieves form data
-places form data inside s3 bucket for later use by other functions
"""

import boto3
import json
import os
from datetime import datetime
import hashlib
from scrape_orchestrator import pipeline_orchestrator


#query hash generation moved from main_app_driver to lambda form handler
def gen_query_hash(brand, category, specific_item=None):
    """Generate a unique hash for the search query"""
    current_date = datetime.now().strftime('%Y-%d-%m')
    query = f"{brand}_{category}"
    
    # Create unique string combining all parameters
    hash_string = f"{query}_{specific_item}_{current_date}" if specific_item else f"{query}_{current_date}"
    
    # Generate hash
    return hashlib.md5(hash_string.encode()).hexdigest()[:12]


#put formdata into s3 bucket
def handle_form_submit(event,context):

    #extract form data
    form_data = {
        'brand': event['brand'],
        'category': event['category'],
        'specific_item': event['specific_item'] or None,
        'email': event['user_email']
    }

    query_hash = gen_query_hash( 
        form_data['brand'],
        form_data['category'],
        form_data['specific_item'])

    form_data.append(query_hash)

    s3 = boto3('s3')
    s3.put_object(
        Bucket='scraper-data-bucket',
        Key=f'queries/{form_data["query_hash"]}/form-params.json',
        Body=json.dumps(form_data)
    )

    pipeline_orchestrator(query_hash)

