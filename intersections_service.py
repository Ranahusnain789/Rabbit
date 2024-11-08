import json
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime
import pika
import pika.exceptions


# Load configuration from config.json
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


# Setup logging
logging.basicConfig(level=logging.INFO)


# Initialize RabbitMQ connection
def setup_rabbitmq():
    config = load_config()
    try:
        connection = pika.BlockingConnection(
            pika.URLParameters(config['rabbitmq_address']))
        channel = connection.channel()
        channel.queue_declare(queue='movement_updates', durable=True)
        channel.queue_declare(queue='turn_updates', durable=True)
        logging.info("RabbitMQ connection established.")
        return connection, channel
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
        exit(1)


# Database setup
def setup_database():
    conn = sqlite3.connect(
        'local_database.db')  # Each microservice should have its own database
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT,
            location TEXT,
            turn INTEGER,
            timestamp TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS intersections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            users TEXT,
            location TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    logging.info("Database setup complete.")
    return conn


# Insert movement into database if not exists
def insert_movement_if_not_exists(user, location, turn, timestamp, conn):
    cursor = conn.cursor()
    # Check if the timestamp already exists
    cursor.execute('SELECT COUNT(*) FROM movements WHERE timestamp = ?',
                   (timestamp, ))
    exists = cursor.fetchone()[0]

    if exists == 0:  # Only insert if it doesn't exist
        cursor.execute(
            'INSERT INTO movements (user, location, turn, timestamp) VALUES (?, ?, ?, ?)',
            (user, location, turn, timestamp))
        conn.commit()
        logging.info(
            f"Inserted new movement into database: {user}, {location}, {turn}, {timestamp}"
        )
    else:
        logging.info(
            f"Movement already exists in database for timestamp: {timestamp}")


# Insert intersection into database
def insert_intersection(users, location, timestamp, conn):
    cursor = conn.cursor()
    users_str = ', '.join(users)
    cursor.execute(
        'INSERT INTO intersections (users, location, timestamp) VALUES (?, ?, ?)',
        (users_str, location, timestamp))
    conn.commit()
    logging.info(
        f"Inserted intersection into database: {users_str}, {location}, {timestamp}"
    )


# Storage for tracking user positions by turn
user_positions_by_turn = defaultdict(lambda: defaultdict(list))


# Record intersections in the database
def record_intersection(users, location, timestamp, conn):
    logging.info(
        f"Intersection detected between users {users} at location {location} "
        f"on {timestamp}")
    insert_intersection(users, location, timestamp, conn)


# Callback function for processing movement messages
def on_movement_message(ch, method, _, body,
                        conn):  # `_` marks `properties` as unused
    try:
        message = json.loads(body)
        user = message['user']
        location = tuple(map(int, message['location'].split(',')))
        turn = message['turn']
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert movement to database if not already exists
        insert_movement_if_not_exists(user, f"{location[0]},{location[1]}",
                                      turn, timestamp, conn)

        # Store each user's position by turn
        user_positions_by_turn[turn][location].append(user)

        # Acknowledge the message after processing
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON from movement message.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except KeyError as e:
        logging.error(f"Missing key in movement message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Error processing movement message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


# Callback for processing turn updates and checking intersections
def on_turn_update(ch, method, _, body,
                   conn):  # `_` marks `properties` as unused
    try:
        message = json.loads(body)
        turn = message['turn']
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Check each location for intersections in the current turn
        for location, users in user_positions_by_turn[turn].items():
            if len(users) > 1:  # More than one user in the same location
                record_intersection(users, f"{location[0]},{location[1]}",
                                    timestamp, conn)

        # Clear processed turn data to avoid memory bloat
        user_positions_by_turn.pop(turn, None)

        # Acknowledge the turn update message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON from turn update message.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except KeyError as e:
        logging.error(f"Missing key in turn update message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Error processing turn update message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


# Main function to consume messages
def main():
    connection, channel = setup_rabbitmq()
    db_conn = setup_database()

    # Consume from the movement_updates queue
    channel.basic_consume(
        queue='movement_updates',
        on_message_callback=lambda ch, method, properties, body:
        on_movement_message(ch, method, properties, body, db_conn),
        auto_ack=False)

    # Consume from the turn_updates queue
    channel.basic_consume(
        queue='turn_updates',
        on_message_callback=lambda ch, method, properties, body:
        on_turn_update(ch, method, properties, body, db_conn),
        auto_ack=False)

    logging.info(
        "Intersection service started, listening for movement and turn updates..."
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Intersection service stopped by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if connection:
            connection.close()
            logging.info("RabbitMQ connection closed.")
        if db_conn:
            db_conn.close()
            logging.info("SQLite connection closed.")


if __name__ == "__main__":
    main()
