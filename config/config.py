import os
import stat
from simple_chalk import chalk
from dotenv import load_dotenv,find_dotenv

if os.getenv('RUNNING_IN_DOCKER') == '1':
	load_dotenv(find_dotenv('.env.docker'))   
	print(chalk.green(f"CONFIG - USING - .env.docker"))
else:
	load_dotenv(find_dotenv('.env.local'))   
	print(chalk.green(f"CONFIG - USING - .env.local"))

APPLICATION_SECRET_KEY = os.getenv('APPLICATION_SECRET_KEY')
DB_URI = os.getenv('DB_URI')
PROXY_HTTPS = os.getenv('PROXY_HTTPS')
PROXY_HTTP = os.getenv('PROXY_HTTP')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
# BACKEND_DIR = os.getenv('BACKEND_DIR')
PROJECT_PATHS= os.getenv('PROJECT_PATHS')
GOOGLE_APP_PW = os.getenv('GOOGLE_APP_PW')
GOOGLE_SENDER_EMAIL = os.getenv('GOOGLE_SENDER_EMAIL')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
# API_HOST = os.getenv('API_HOST')
# API_PORT = os.getenv('API_PORT')

# Ensure API_HOST and API_PORT are set
# if not API_HOST or not API_PORT:
#     raise ValueError("API_HOST and API_PORT must be set in the environment variables")
