import pika
from simple_chalk import chalk
from config.config import RABBITMQ_HOST
from config.connections import create_rabbitmq_connection


def check_queue_empty(queue_name):
    try:
        connection = create_rabbitmq_connection()
        channel = connection.channel()


        # Declare the queue in passive mode to get its message count
        """
        Declare queue in passive mode to get its contents
        Passive mode lets you check if queue exists w/o creating it. 
        If doesnt exist - exception is raised
        it exists, method returns details about the queue like message_count
        
        """
        queue = channel.queue_declare(queue=queue_name, passive=True)
        
        message_count = queue.method.message_count
        print(f"The queue '{queue_name}' contains {message_count} messages.")

        if message_count == 0:
            print(chalk.green(f"Queue '{queue_name}' is empty."))
        else:
            print(chalk.red(f"Queue '{queue_name}' is NOT empty. It contains {message_count} messages."))

        # Close the connection
        connection.close()

    except Exception as e:
        print(f"Error checking queue: {e}")

if __name__ == "__main__":
    check_queue_empty('price_change_queue')