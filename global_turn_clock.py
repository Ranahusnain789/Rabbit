import json
import logging
import time

import pika
import pika.exceptions


# Load the configuration file
def load_config():
    try:
        with open('config.json') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        logging.error("Configuration file 'config.json' not found.")
        exit(1)
    except json.JSONDecodeError:
        logging.error("Error decoding JSON from the configuration file.")
        exit(1)


# Set up logging
def setup_logging():
    logging.basicConfig(level=logging.INFO)


# Establish RabbitMQ connection
def setup_rabbitmq(config):
    try:
        rabbitmq_address = config.get("rabbitmq_address")
        connection = pika.BlockingConnection(
            pika.URLParameters(rabbitmq_address))
        channel = connection.channel()

        # Declare turn_updates queue with durability to ensure persistence
        channel.queue_declare(queue='turn_updates', durable=True)
        return connection, channel
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
        exit(1)


# Broadcast the turn update to RabbitMQ
def broadcast_turn(channel, turn):
    try:
        message = json.dumps({"action": "turn_update", "turn": turn})
        channel.basic_publish(
            exchange='',
            routing_key='turn_updates',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make message persistent
            ))
        logging.info(f"Turn {turn} broadcasted")
    except pika.exceptions.AMQPError as e:
        logging.error(f"Error broadcasting turn {turn}: {e}")
        raise


# Main function to run the global turn clock
def main():
    setup_logging()
    config = load_config()
    turn_duration = config.get("turn_duration", 1.0)
    connection, channel = setup_rabbitmq(config)

    turn = 0
    try:
        while True:
            broadcast_turn(channel, turn)
            turn += 1
            time.sleep(turn_duration)
    except KeyboardInterrupt:
        logging.info("Turn clock stopped by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if connection:
            connection.close()
            logging.info("RabbitMQ connection closed.")


if __name__ == "__main__":
    main()
