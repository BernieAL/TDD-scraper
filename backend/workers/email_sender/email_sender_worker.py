import smtplib
import ssl
import os
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv, find_dotenv
from simple_chalk import chalk
from botocore.exceptions import ClientError

load_dotenv(find_dotenv())

# Configuration
port = 465  # For SSL
app_password = os.getenv("GOOGLE_APP_PW")
sender_email = os.getenv("GOOGLE_SENDER_EMAIL")
subject = "Daily Price Change + Sold Reports"

def attach_files_from_s3(message, report_path, query_hash):
    """
    Attach files from S3 that match the query hash
    """
    try:
        s3 = boto3.client('s3')
        bucket = os.environ['S3_BUCKET']
        
        print(chalk.blue(f"[INFO] Looking for reports in: {report_path}"))
        
        # List objects in report path
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=report_path
        )
        
        if 'Contents' not in response:
            print(chalk.yellow(f"No files found in {report_path}"))
            return
            
        for obj in response['Contents']:
            if query_hash in obj['Key']:
                try:
                    print(chalk.blue(f"[INFO] Attaching file: {obj['Key']}"))
                    
                    # Get file from S3
                    file_data = s3.get_object(
                        Bucket=bucket,
                        Key=obj['Key']
                    )
                    
                    # Attach to email
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(file_data['Body'].read())
                    encoders.encode_base64(part)
                    
                    # Set filename as last part of S3 key
                    filename = obj['Key'].split('/')[-1]
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename={filename}'
                    )
                    message.attach(part)
                    print(chalk.green(f"[SUCCESS] Attached: {filename}"))
                    
                except ClientError as e:
                    print(chalk.red(f"S3 error attaching {obj['Key']}: {e}"))
                except Exception as e:
                    print(chalk.red(f"Error attaching {obj['Key']}: {e}"))
                    
    except ClientError as e:
        print(chalk.red(f"S3 client error: {e}"))
    except Exception as e:
        print(chalk.red(f"Error accessing S3: {e}"))

def send_email_with_report(msg, query_hash, price_reports_path, sold_reports_path, 
                          query, no_price_change_sources=None, empty_scrape_files=None):
    """
    Send email with reports from S3
    """
    if 'email' not in msg:
        print(chalk.red("Error: No email address provided in message"))
        return False

    no_price_change_sources = no_price_change_sources or []
    empty_scrape_files = empty_scrape_files or []

    try:
        # Create a secure SSL context
        context = ssl.create_default_context()

        # Email message setup
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = msg['email']
        message['Subject'] = subject

        # Email body
        if empty_scrape_files:
            body = f"""Hi there,\n
No results were found for your search: {query}. This could be due to:
- Item not available
- Search terms may need adjustment
- Technical issue

If you believe this is an error, please contact support@example.com.

Sources that returned no results:
{chr(10).join(f"- {source}" for source in empty_scrape_files)}
"""
        else:
            body = f"Hi there,\n\nPlease find attached the daily price change and sold reports for {query}."

            if no_price_change_sources:
                body += "\n\nThe following sources had no price changes:\n"
                body += "\n".join(f"- {source}" for source in no_price_change_sources)

        message.attach(MIMEText(body, 'plain'))

        # Attach reports from S3
        attach_files_from_s3(message, price_reports_path, query_hash)
        attach_files_from_s3(message, sold_reports_path, query_hash)

        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, msg['email'], message.as_string())
            
        print(chalk.green(f"[SUCCESS] Email sent to: {msg['email']} for query: {query}"))
        return True
        
    except smtplib.SMTPException as e:
        print(chalk.red(f"SMTP error sending email: {e}"))
        return False
    except Exception as e:
        print(chalk.red(f"Error sending email: {e}"))
        return False