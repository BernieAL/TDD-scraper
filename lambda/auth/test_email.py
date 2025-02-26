import os
from dotenv import load_dotenv
from lambda_auth_routes import send_verification_email, generate_verification_code


#load env vars
load_dotenv()

def test_email_sending():
    
    #generate test verification code()
    code = generate_verification_code()

    #test email address to use
    test_email = "balmanzar883@gmail.com"

    #try to send the email
    result = send_verification_email(test_email,code)

    if result:
        print(f"successfully sent verification code")
    else:
        print("failed to send email")

if __name__ == "__main__":
    test_email_sending()