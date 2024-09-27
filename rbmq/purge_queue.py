import pika

# Set up the connection parameters
connection_params = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    credentials=pika.PlainCredentials('guest', 'guest')
)

# Establish connection and channel
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Purge the queue
channel.queue_purge(queue='price_change_queue')

print("Queue cleared.")

# Close the connection
connection.close()
