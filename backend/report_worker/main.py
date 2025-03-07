import os
import boto3
import json

def generate_report():
    s3 = boto3.client('s3')
    bucket = os.environ['S3_BUCKET']
    query_hash = os.environ['QUERY_HASH']
    analysis_path = os.environ['ANALYSIS_PATH']
    reports_path = os.environ['REPORTS_PATH']
    
    # Get analysis results
    analysis = s3.get_object(
        Bucket=bucket,
        Key=f"{analysis_path}/price_analysis.json"
    )
    analysis_data = json.loads(analysis['Body'].read())
    
    # Get form data for report context
    form_data = s3.get_object(
        Bucket=bucket,
        Key=f'queries/{query_hash}/form-params.json'
    )
    params = json.loads(form_data['Body'].read())
    
    # Generate report
    report = generate_pdf_report(analysis_data, params)
    
    # Save report
    s3.put_object(
        Bucket=bucket,
        Key=f"{reports_path}/price_report.pdf",
        Body=report
    )
    
    # Optional: Send email with report link
    send_report_email(
        email=params['email'],
        report_url=f"s3://{bucket}/{reports_path}/price_report.pdf"
    ) 