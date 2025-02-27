import smtplib
import ssl
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv, find_dotenv
from simple_chalk import chalk

load_dotenv(find_dotenv())

# Configuration
port = 465  # For SSL
app_password = os.getenv("GOOGLE_APP_PW")
sender_email = os.getenv("GOOGLE_SENDER_EMAIL")
subject = "Daily Price Change + Sold Reports"

def attach_file_from_sub_directory(message, report_subdir,query_hash):
    
    """
    only attaching files that match current query hash
    """
    if os.path.exists(report_subdir):
        try:
            for dirpath, _, files in os.walk(report_subdir):
                for report_file in files:

                    if query_hash in report_file:

                        file_path = os.path.join(dirpath, report_file)
                        print(chalk.blue(f"[INFO] Attaching file: {report_file}"))
                        
                        with open(file_path, 'rb') as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename={os.path.basename(report_file)}'
                            )
                            message.attach(part)
        except Exception as e:
            print(chalk.red(f"Email Attachment error: {e}"))
    else:
        print(chalk.yellow(f"No report directory provided or it does not exist: {report_subdir}"))

def send_email_with_report(msg,query_hash, price_report_subdir, sold_report_subdir, query, no_price_change_sources=None, empty_scrape_files=None):
    # paths shouldnt actually be written to the report being sent in the email. right now theres a column that says 'paths' in the report to the user.
    if 'email' not in msg:
        print(chalk.red("Error: No email address provided in message"))
        return False

    no_price_change_sources = no_price_change_sources or []

    # Create a secure SSL context
    context = ssl.create_default_context()

    # Email message setup
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = msg['email']
    message['Subject'] = subject

    # Email body
    if empty_scrape_files:
        body = f"""Hi there,\nNo results were found for your search: {query}. This could be due to:\n- Item not available\n- Search terms may need adjustment\n- Technical issue\n\nIf you are sure there is an error, please forward this email to support@example.com for assistance.\n\nSources that returned no results:\n"""
        for source in empty_scrape_files:
            body += f"{source}\n"
    else:
        body = f"Hi there,\n\nPlease find attached the daily price change and sold reports for {query}."

    if len(no_price_change_sources) > 0:
        body += "\n\nThe following sources had no price changes:\n"
        for source in no_price_change_sources:
            body += f"{source}\n"

    message.attach(MIMEText(body, 'plain'))

    attach_file_from_sub_directory(message, price_report_subdir,query_hash)
    attach_file_from_sub_directory(message, sold_report_subdir,query_hash)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, msg['email'], message.as_string())
        print(chalk.green(f"SUCCESSFULLY SENT MESSAGE TO EMAIL: {msg['email']} - FOR QUERY: {query}"))
        return True
    except Exception as e:
        print(chalk.red(f"There was an error sending the email: {e}"))
        return False