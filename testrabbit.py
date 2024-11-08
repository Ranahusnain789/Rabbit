import random
import string
import threading
import time

import pika
import pika.exceptions

# RabbitMQ connection details
rabbitmq_address = "amqps://sylriuyc:JRm6YXvQEkNU_KMPpBxyjLQA0BjQYgX0@cougar.rmq.cloudamqp.com/sylriuyc"

def generate_random_string(length=8):
    """Generate a random string of given length."""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# Function to publish messages to the movement_updates queue
def publish_messages():
    counter = 0
    while True:
        connection = None  # Initialize connection as None
        try:
            connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_address))
            channel = connection.channel()
            channel.queue_declare(queue='movement_updates', durable=True)

            random_text = f"Test - {generate_random_string()}"
            if counter % 10 == 0:
                random_text = f"Exam - {generate_random_string()}"
                print(f"Publishing EXAM message: {random_text}")
            else:
                print(f"Publishing TEST message: {random_text}")

            channel.basic_publish(
                exchange='',
                routing_key='movement_updates',
                body=random_text
            )
            counter += 1
            time.sleep(1)  # Delay between messages

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error in publisher: {e}. Reconnecting...")
            time.sleep(5)  # Wait before retrying connection
        finally:
            if connection is not None:
                connection.close()

# Function to subscribe to the movement_updates queue and detect "Exam" messages
def subscribe_to_queue():
    def callback(_, __, ____, body):
        message = body.decode('utf-8')
        if message.startswith("Exam"):
            print(f"Exam message detected: {message}")

    while True:
        connection = None  # Initialize connection as None
        try:
            connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_address))
            channel = connection.channel()
            channel.queue_declare(queue='movement_updates', durable=True)

            # Consumer with auto_ack=False (manual acknowledgment is disabled)
            channel.basic_consume(
                queue='movement_updates',
                on_message_callback=callback,
                auto_ack=False  # Messages stay in queue after being read
            )

            print("Listening for messages on movement_updates queue...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection error in subscriber: {e}. Reconnecting...")
            time.sleep(5)  # Wait before retrying connection
        finally:
            if connection is not None:
                connection.close()

# Run both publishing and subscribing functions in parallel
if __name__ == "__main__":
    threading.Thread(target=publish_messages).start()
    threading.Thread(target=subscribe_to_queue).start()
