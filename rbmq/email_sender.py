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
sender_email = os.getenv("GOOGLE_SENDER_EMAIL")  # Your email
subject = "Daily Price Change Report"

def send_email_with_report(receiver_email, report_file_path, veh):
    # Create a secure SSL context
    context = ssl.create_default_context()

    # Email message setup
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject

    # Email body
    body = f"Hi there,\n\nPlease find attached the daily price change report for {veh}."
    message.attach(MIMEText(body, 'plain'))

    # Attach the report
    try:
        with open(report_file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename={os.path.basename(report_file_path)}'
            )
            message.attach(part)

        # Send the email with the report attached
        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_email, message.as_string())
            print(chalk.green(f"SUCCESSFULLY SENT MESSAGE TO EMAIL: {receiver_email} - FOR VEH: {veh}"))
    
    except Exception as e:
        print(chalk.red(f"There was an error sending the email: {e}"))

if __name__ == "__main__":
    # Example usage
    report_path = 'path_to_report.csv'
    recipient_email = 'user@example.com'
    vehicle_name = 'Prada Bags'
    send_email_with_report(recipient_email, report_path, vehicle_name)
