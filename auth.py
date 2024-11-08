import json
import logging
import random
import sqlite3
import uuid

import pika
from flask import Blueprint, jsonify, request, session

auth_blueprint = Blueprint('auth', __name__)

# Singleton pattern for RabbitMQ connection
class RabbitMQConnection:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls._create_connection()
        return cls._instance

    @classmethod
    def _create_connection(cls):
        with open('config.json') as f:
            config = json.load(f)
        connection = pika.BlockingConnection(
            pika.URLParameters(config['rabbitmq_address'])
        )
        channel = connection.channel()
        # Ensuring queue is declared durable
        channel.queue_declare(queue='movement_updates', durable=True)
        return connection, channel

# Database setup
def get_db_connection():
    conn = sqlite3.connect('users.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            location TEXT,
            is_admin BOOLEAN NOT NULL CHECK (is_admin IN (0, 1))
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# Helper functions
def generate_session_token():
    return str(uuid.uuid4())

def generate_random_location():
    with open('config.json') as f:
        config = json.load(f)
    map_size = config['map_size']
    x = random.randint(0, map_size[0] - 1)
    y = random.randint(0, map_size[1] - 1)
    return f"{x},{y}"

# Post login to RabbitMQ with message persistence
def post_login_to_rabbitmq(username, location):
    connection, channel = RabbitMQConnection.get_instance()
    message = {
        'action': 'login',
        'user': username,
        'location': location,
        'turn': 0
    }
    channel.basic_publish(
        exchange='',
        routing_key='movement_updates',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Ensure message persistence
    )
    logging.info(f"Published login message for user {username}")

# Routes
@auth_blueprint.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
    if cursor.fetchone():
        conn.close()
        return jsonify({"error": "User already exists"}), 409

    location = generate_random_location()
    cursor.execute(
        'INSERT INTO users (username, password, location, is_admin) '
        'VALUES (?, ?, ?, ?)',
        (username, password, location, False)
    )
    conn.commit()
    conn.close()
    return jsonify({
        "message": "User registered successfully",
        "location": location
    }), 201

@auth_blueprint.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT * FROM users WHERE username = ? AND password = ?',
        (username, password)
    )
    user = cursor.fetchone()
    conn.close()
    if not user:
        return jsonify({"error": "Invalid username or password"}), 401

    session['username'] = username
    location = user['location']
    post_login_to_rabbitmq(username, location)
    return jsonify({
        "message": "Login successful",
        "session_token": generate_session_token(),
        "location": location
    }), 200

@auth_blueprint.route('/logout', methods=['POST'])
def logout():
    session.pop('username', None)
    return jsonify({"message": "Logout successful"}), 200
