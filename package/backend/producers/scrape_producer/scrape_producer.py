import pika
import os,sys
from dotenv import load_dotenv,find_dotenv
from simple_chalk import chalk
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import datetime

# load_dotenv(find_dotenv())

# RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')

#get parent dir 'backend_copy' from current script dir - append to sys.path to be searched for modules we import
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add the directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection

app = Flask(__name__)
CORS(app)

def SCRAPE_publish_to_queue(msg):
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()

        channel.queue_declare(queue='scrape_queue',durable=True)

        print(msg)
        # publish product to quueue
        channel.basic_publish(exchange='', 
                                routing_key='scrape_queue', 
                                body=json.dumps(msg),
                                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
        print(chalk.green(f"Successfully sent task to queue: {msg} \n -------------"))

    except Exception as e:
        print(chalk.red(f"Error publishing message to queue: {e}"))
        raise

    finally:
        # Close the connection to RabbitMQ
        try:
            connection.close()
        except Exception as e:
            print(chalk.red(f"Error closing RabbitMQ connection: {e}"))

@app.route('/api/scrape', methods=['POST'])
def handle_scrape_request():
    try:
        data = request.get_json()
        urls = data.get('urls', [])
        email = data.get('email')

        if not urls or not email:
            return jsonify({'error': 'Missing required fields'}), 400

        # Create message for queue
        message = {
            'urls': urls,
            'email': email,
            'timestamp': str(datetime.datetime.now())
        }

        # Publish to queue
        SCRAPE_publish_to_queue(message)

        return jsonify({
            'message': 'Search started successfully',
            'status': 'success'
        }), 200

    except Exception as e:
        print(chalk.red(f"Error handling scrape request: {e}"))
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)