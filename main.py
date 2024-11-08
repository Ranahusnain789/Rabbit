import json
import logging
import threading
import os
import time
import subprocess
import pika
import pika.exceptions
from flask import Flask, render_template
from flask_socketio import SocketIO

from auth import auth_blueprint
from report_service import report_blueprint

# Initialize Flask and SocketIO
app = Flask(__name__)
app.secret_key = 'your_secret_key'
socketio = SocketIO(app)

# Register blueprints
app.register_blueprint(auth_blueprint)
app.register_blueprint(report_blueprint, url_prefix='/report')


# Load configuration from config.json
def load_config():
    with open('config.json') as f:
        return json.load(f)


config = load_config()
rabbitmq_address = config['rabbitmq_address']


# Setup RabbitMQ and declare necessary queues
def setup_rabbitmq():
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_address))
    channel = connection.channel()

    # Declare all relevant queues with durable=True to ensure messages persist
    channel.queue_declare(queue='movement_updates', durable=True)
    channel.queue_declare(queue='turn_updates', durable=True)
    channel.queue_declare(queue='map_layout', durable=True)
    channel.queue_declare(queue='position', durable=True)

    return connection, channel


# Listen to movement_updates queue
def listen_to_movement_updates():
    connection, channel = setup_rabbitmq()
    for method_frame, _, body in channel.consume(queue='movement_updates',
                                                 auto_ack=False):
        if body:  # Check if message body is non-empty
            try:
                message = json.loads(body)
                socketio.emit('movement_update', message)
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except json.JSONDecodeError:
                logging.error(
                    "Failed to decode JSON from movement_updates queue. "
                    "Message skipped.")
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        else:
            logging.warning(
                "Received empty message in movement_updates queue.")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)


# Listen to turn_updates queue
def listen_to_turn_updates():
    connection, channel = setup_rabbitmq()
    for method_frame, _, body in channel.consume(queue='turn_updates',
                                                 auto_ack=False):
        if body:
            try:
                message = json.loads(body)
                socketio.emit('turn_update', {'turn': message.get('turn', 0)})
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except json.JSONDecodeError:
                logging.error(
                    "Failed to decode JSON from movement_updates queue. "
                    "Message skipped.")
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        else:
            logging.warning("Received empty message in turn_updates queue.")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)


# Listen to map_layout queue
def listen_to_map_layout():
    connection, channel = setup_rabbitmq()
    for method_frame, _, body in channel.consume(queue='map_layout',
                                                 auto_ack=False):
        if body:
            try:
                message = json.loads(body)
                socketio.emit('map_update', message)
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except json.JSONDecodeError:
                logging.error(
                    "Failed to decode JSON from movement_updates queue. "
                    "Message skipped.")
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        else:
            logging.warning("Received empty message in map_layout queue.")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)


# Background tasks to start RabbitMQ listeners
def start_rabbitmq_listeners():
    threading.Thread(target=listen_to_movement_updates, daemon=True).start()
    threading.Thread(target=listen_to_turn_updates, daemon=True).start()
    threading.Thread(target=listen_to_map_layout, daemon=True).start()


def run_launcher():
    try:
        # Start the launcher script and capture output
        process = subprocess.Popen(['python', 'launcher.py'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        # Optionally, read the output in a separate thread to avoid blocking
        def read_output(proc):
            if proc.stdout is not None:
                for line in iter(proc.stdout.readline, b''):
                    logging.info(line.decode().strip())
            proc.stdout.close()

        # Start the output reading in a separate thread
        output_thread = threading.Thread(target=read_output, args=(process, ))
        output_thread.daemon = True
        output_thread.start()

        # Wait for the process to complete if needed
        return_code = process.wait(
        )  # This will block until the process completes
        if return_code == 0:
            logging.info("Launcher started successfully.")
        else:
            logging.error(f"Launcher exited with return code {return_code}.")
    except Exception as e:
        logging.error(f"Error starting launcher: {e}")


def main():
    # Start the launcher in a separate thread
    launcher_thread = threading.Thread(target=run_launcher)
    launcher_thread.daemon = True  # Ensures the thread will exit when the main program does
    launcher_thread.start()

    # Start the socket.io app
    socketio.run(app,
                 debug=True,
                 host='0.0.0.0',
                 port=8080,
                 use_reloader=False,
                 log_output=True)


# Flask route for the main page
@app.route('/')
def home():
    return render_template('index.html')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    start_rabbitmq_listeners()
    main()
