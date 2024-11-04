import smtplib
import ssl
import os,sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv, find_dotenv
from simple_chalk import chalk

# Ensure the project root is accessible
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


load_dotenv(find_dotenv())

# Configuration
port = 465  # For SSL
app_password = os.getenv("GOOGLE_APP_PW")
sender_email = os.getenv("GOOGLE_SENDER_EMAIL")  # Your email
subject = "Daily Price Change Report"



def attach_file_from_subdir(message,subdir):

    """
    Will attach files from given subdir to email
    Subdir can be price_report_subdir or sold_report_subdir.
    
    """

    # if price_report_dir for current source query exists - traverse dir and attach all reports to email
    if os.path.exists(subdir):

        try:
            for dirpath,subdir,files in os.walk(subdir):
                for report_file in files:
                    print(report_file)
                    
                    file_path = os.path.join(dirpath,report_file)

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
            print(chalk.red(f"Email Attachment error - There was an error sending the email: {e}"))
    else:
         print(chalk.yellow(f"No report directory provided or it does not exist: {subdir}"))


def send_email_with_report(receiver_email, price_report_subdir,sold_report_subdir,query,no_price_change_sources):
    
    print(chalk.red(f"(EMAIL SENDER) subdir valid path: {os.path.isdir(price_report_subdir)}"))
    
    # Create a secure SSL context
    context = ssl.create_default_context()

    # Email message setup
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject

    # Email body
    body = f"Hi there,\n\nPlease find attached the daily price change report for {query}. \n"
   
    #add details about sources with no prices changes to message body
    if len(no_price_change_sources) > 0:
        body += "\n The following sources had no price changes: :\n"
        for source in no_price_change_sources:
           body += f"{source}\n"

    message.attach(MIMEText(body, 'plain'))

    attach_file_from_subdir(message,price_report_subdir)
    attach_file_from_subdir(message,sold_report_subdir)
    # # if price_report_dir for current source query exists - traverse dir and attach all reports to email
    # if os.path.exists(price_report_subdir):

    #     try:
    #         for dirpath,subdir,files in os.walk(price_report_subdir):
    #             for report_file in files:
    #                 print(report_file)
                    
    #                 file_path = os.path.join(dirpath,report_file)

    #                 with open(file_path, 'rb') as attachment:
    #                     part = MIMEBase('application', 'octet-stream')
    #                     part.set_payload(attachment.read())
    #                     encoders.encode_base64(part)
    #                     part.add_header(
    #                         'Content-Disposition',
    #                         f'attachment; filename={os.path.basename(report_file)}'
    #                     )
    #                     message.attach(part)
        
    #     except Exception as e:
    #         print(chalk.red(f"Email Attachment error - There was an error sending the email: {e}"))
    # else:
    #      print(chalk.yellow(f"No price report directory provided or it does not exist: {price_report_subdir}"))




    #email gets sent whether theres generate price reports or not     
    try:
        # Send the email with the report attached
        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_email, message.as_string())
            print(chalk.green(f"SUCCESSFULLY SENT MESSAGE TO EMAIL: {receiver_email} - FOR VEH: {query}"))
        return True
    
    except Exception as e:
        print(chalk.red(f"There was an error sending the email: {e}"))
        return False

if __name__ == "__main__":
    # Example usage

    current_dir = os.path.abspath(os.path.dirname(__file__))
    root_dir = os.path.abspath(os.path.join(current_dir,".."))
    print(chalk.red(root_dir))

    price_report_dir = os.path.join(root_dir,'price_report_output','PRICE_REPORT_PRADA_2024-25-10_BAGS_a44eabd1')
    sold_report_dir = os.path.join(root_dir, 'sold_report_output','SOLD_REPORT_PRADA_2024-25-10_BAGS_a44eabd1')

    

    
    recipient_email = 'balmanzar883@gmail.com'
    query_name = 'Prada Bags'

    send_email_with_report(recipient_email,price_report_dir,sold_report_dir,query_name,[])
