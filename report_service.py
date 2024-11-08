import json
import logging
import sqlite3
import pika
from flask import Blueprint, Flask, jsonify
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)

# Blueprint setup for Flask routes
report_blueprint = Blueprint('report', __name__)


# Load configuration from config.json
def load_config():
    try:
        with open('config.json') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("Configuration file 'config.json' not found.")
        exit(1)
    except json.JSONDecodeError:
        logging.error("Error decoding JSON from config.")
        exit(1)


# SQLite setup for reports
def get_db_connection():
    conn = sqlite3.connect('reports.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT,
            location TEXT,
            timestamp TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS intersections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user1 TEXT,
            user2 TEXT,
            location TEXT,
            timestamp TEXT
        )
    ''')
    conn.commit()
    return conn


# RabbitMQ Setup
def setup_rabbitmq(config):
    connection = pika.BlockingConnection(
        pika.URLParameters(config['rabbitmq_address']))
    channel = connection.channel()
    channel.queue_declare(queue='movement_updates', durable=True)
    channel.queue_declare(queue='intersections', durable=True)
    return connection, channel


# Process movement updates and log to database
def on_movement_update(ch, method, body, db_conn):
    message = json.loads(body)
    user = message.get("user")
    location = message.get("location")
    timestamp = message.get("timestamp")

    cursor = db_conn.cursor()
    # Check if this movement already exists
    cursor.execute(
        'SELECT COUNT(*) FROM movements WHERE user = ? AND location = ? AND timestamp = ?',
        (user, location, timestamp))

    if cursor.fetchone(
    )[0] == 0:  # If this movement is not already in the database
        cursor.execute(
            'INSERT INTO movements (user, location, timestamp) VALUES (?, ?, ?)',
            (user, location, timestamp))
        db_conn.commit()
        logging.info(f"Movement recorded: {user} at {location} at {timestamp}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


# Process intersections and log to database
def on_intersection_update(ch, method, body, db_conn):
    message = json.loads(body)
    user1 = message.get("user1")
    user2 = message.get("user2")
    location = message.get("location")
    timestamp = message.get("timestamp")

    cursor = db_conn.cursor()
    # Check if this intersection already exists
    cursor.execute(
        'SELECT COUNT(*) FROM intersections WHERE (user1 = ? AND user2 = ? AND location = ? AND timestamp = ?) OR (user1 = ? AND user2 = ? AND location = ? AND timestamp = ?)',
        (user1, user2, location, timestamp, user2, user1, location, timestamp))

    if cursor.fetchone(
    )[0] == 0:  # If this intersection is not already in the database
        cursor.execute(
            'INSERT INTO intersections (user1, user2, location, timestamp) '
            'VALUES (?, ?, ?, ?)', (user1, user2, location, timestamp))
        db_conn.commit()
        logging.info(
            f"Intersection recorded between {user1} and {user2} at {location} at {timestamp}"
        )

    ch.basic_ack(delivery_tag=method.delivery_tag)


# Fetch Movement History
@report_blueprint.route('/report/movement/<user>', methods=['GET'])
def movement_report(user):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            'SELECT location, timestamp FROM movements WHERE user = ?',
            (user, ))
        movements = cursor.fetchall()

        movement_history = [{
            "location": loc,
            "timestamp": ts
        } for loc, ts in movements]

        return jsonify({"movement_history": movement_history}), 200
    except Exception as e:
        logging.error(f"Error fetching movement history: {e}")
        return jsonify({"error": "Error fetching movement history"}), 500
    finally:
        conn.close()


# Fetch Intersections
@report_blueprint.route('/report/intersection/<user>', methods=['GET'])
def intersection_report(user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT user1, user2, location, timestamp FROM intersections '
        'WHERE user1 = ? OR user2 = ?', (user, user))
    intersections = cursor.fetchall()
    conn.close()
    return jsonify({"intersection_history": intersections}), 200


# Main function to consume messages
def main():
    config = load_config()
    connection, channel = setup_rabbitmq(config)
    db_conn = get_db_connection()

    # Consume movement updates
    channel.basic_consume(queue='movement_updates',
                          on_message_callback=lambda ch, method, body:
                          on_movement_update(ch, method, body, db_conn),
                          auto_ack=False)

    # Consume intersection updates
    channel.basic_consume(queue='intersections',
                          on_message_callback=lambda ch, method, body:
                          on_intersection_update(ch, method, body, db_conn),
                          auto_ack=False)

    logging.info(
        "Report Service started, listening for movement and intersection updates..."
    )

    # Start Flask API in a separate thread
    app = Flask(__name__)
    app.register_blueprint(report_blueprint)

    from threading import Thread
    flask_thread = Thread(target=app.run, kwargs={'port': 5001})
    flask_thread.start()

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Report Service stopped by user.")
    finally:
        if connection:
            connection.close()
            logging.info("RabbitMQ connection closed.")
        if db_conn:
            db_conn.close()
            logging.info("SQLite connection closed.")


if __name__ == '__main__':
    main()
