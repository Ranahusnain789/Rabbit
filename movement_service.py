import json
import logging
import random
import sqlite3
import pika
from flask import Flask, jsonify, request

logging.basicConfig(level=logging.INFO)

# Flask app for handling HTTP requests
app = Flask(__name__)


# Load configuration and map
def load_config():
    with open('config.json') as f:
        return json.load(f)


config = load_config()
map_layout = config['map_layout']
map_size = (len(map_layout[0]), len(map_layout))


# RabbitMQ setup for publishing and subscribing to movements
def setup_rabbitmq():
    connection = pika.BlockingConnection(
        pika.URLParameters(config['rabbitmq_address']))
    channel = connection.channel()
    channel.queue_declare(queue='movement_updates', durable=True)
    return connection, channel


connection, channel = setup_rabbitmq()


# Database setup for tracking user positions
def setup_database():
    conn = sqlite3.connect('movements.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_positions (
            user_id TEXT PRIMARY KEY,
            x INTEGER,
            y INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movement_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            x INTEGER,
            y INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    return conn


# Function to publish updates to RabbitMQ
def publish_update(channel, user_id, x, y):
    message = {
        "user_id": user_id,
        "location": f"{x},{y}",
        "timestamp": int(datetime.datetime.now().timestamp())
    }
    channel.basic_publish(exchange='',
                          routing_key='movement_updates',
                          body=json.dumps(message),
                          properties=pika.BasicProperties(delivery_mode=2))
    logging.info(f"Published movement for user {user_id} to {x}, {y}")


# Function to save movement to local database
def save_movement_to_db(conn, user_id, x, y):
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO movement_history (user_id, x, y) VALUES (?, ?, ?)',
        (user_id, x, y))
    conn.commit()


# Function to handle historical data
def check_and_store_historical_data(conn):
    cursor = conn.cursor()
    for method_frame, properties, body in channel.consume(
            'movement_updates', inactivity_timeout=1):
        if body:
            message = json.loads(body)
            user_id = message['user_id']
            x, y = map(int, message['location'].split(','))
            cursor.execute(
                'SELECT COUNT(*) FROM movement_history WHERE user_id = ? AND x = ? AND y = ?',
                (user_id, x, y))
            if cursor.fetchone(
            )[0] == 0:  # If this position is not already in history
                save_movement_to_db(conn, user_id, x, y)
            channel.basic_ack(method_frame.delivery_tag)


# Assign a random starting location within map bounds
def assign_random_position():
    while True:
        x = random.randint(0, map_size[0] - 1)
        y = random.randint(0, map_size[1] - 1)
        if map_layout[y][x] == ' ':  # Ensure it's not an obstacle
            return x, y


# Endpoint to process user movement
@app.route('/move', methods=['POST'])
def move_user():
    if not request.is_json:
        return jsonify({"error": "Invalid JSON format"}), 400

    data = request.get_json() or {}
    user_id = data.get('user_id')
    direction = data.get('direction')

    if not user_id or not direction:
        return jsonify({"error":
                        "Missing 'user_id' or 'direction' in JSON"}), 400

    conn = setup_database()
    cursor = conn.cursor()
    cursor.execute('SELECT x, y FROM user_positions WHERE user_id = ?',
                   (user_id, ))
    row = cursor.fetchone()

    # Assign random position if user has no previous position
    if not row:
        x, y = assign_random_position()
        cursor.execute(
            'INSERT INTO user_positions (user_id, x, y) VALUES (?, ?, ?)',
            (user_id, x, y))
        conn.commit()
    else:
        x, y = row

    # Calculate the new position based on direction
    new_x, new_y = x, y
    if direction == 'N':
        new_y -= 1
    elif direction == 'S':
        new_y += 1
    elif direction == 'E':
        new_x += 1
    elif direction == 'W':
        new_x -= 1

    # Validate move (within bounds and not into an obstacle)
    if (0 <= new_x < map_size[0] and 0 <= new_y < map_size[1]
            and map_layout[new_y][new_x] != 'H'):
        cursor.execute(
            'UPDATE user_positions SET x = ?, y = ? WHERE user_id = ?',
            (new_x, new_y, user_id))
        conn.commit()
        publish_update(channel, user_id, new_x, new_y)
        conn.close()
        return jsonify({
            "status": "Move successful",
            "new_location": f"({new_x}, {new_y})"
        }), 200
    else:
        conn.close()
        return jsonify({"error":
                        "Invalid move: obstacle or out of bounds"}), 400


# Endpoint to handle user login
@app.route('/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({"error": "Invalid JSON format"}), 400

    data = request.get_json() or {}
    user_id = data.get('user_id')
    if not user_id:
        return jsonify({"error": "Missing 'user_id' in JSON"}), 400

    conn = setup_database()
    cursor = conn.cursor()
    cursor.execute('SELECT x, y FROM user_positions WHERE user_id = ?',
                   (user_id, ))
    row = cursor.fetchone()

    if not row:
        x, y = assign_random_position()
        cursor.execute(
            'INSERT INTO user_positions (user_id, x, y) VALUES (?, ?, ?)',
            (user_id, x, y))
        conn.commit()
    else:
        x, y = row

    publish_update(channel, user_id, x, y)
    conn.close()
    return jsonify({
        "status": "Login successful",
        "location": f"({x}, {y})"
    }), 200


def main():
    conn = setup_database()
    check_and_store_historical_data(conn)
    app.run(port=5003)


if __name__ == '__main__':
    main()
