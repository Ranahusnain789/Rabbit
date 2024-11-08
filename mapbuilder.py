import json
import logging
import pika
from flask import Flask, jsonify, send_from_directory
from threading import Thread

logging.basicConfig(level=logging.INFO)


# Load configuration
def load_config():
    with open('config.json') as f:
        return json.load(f)


config = load_config()
initial_map_layout = config['map_layout']
map_size = config['map_size']


# Setup RabbitMQ
def setup_rabbitmq():
    connection = pika.BlockingConnection(
        pika.URLParameters(config['rabbitmq_address']))
    channel = connection.channel()
    channel.queue_declare(queue='movement_updates', durable=True)
    return connection, channel


# In-memory map storage
maps_by_turn = {}


# Create a fresh map layout
def create_map_layout():
    return [row[:] for row in initial_map_layout]


# Update the map layout
def update_map_layout(map_layout, user, x, y):
    if map_layout[y][x] == ' ':
        map_layout[y][x] = str(user)
    elif isinstance(map_layout[y][x], str) and map_layout[y][x].isdigit():
        map_layout[y][x] = 'X'
    return map_layout


# Publish map
def publish_map(channel, map_layout, turn):
    map_message = {"turn": turn, "map": map_layout}
    channel.basic_publish(exchange='',
                          routing_key='position',
                          body=json.dumps(map_message),
                          properties=pika.BasicProperties(delivery_mode=2))
    logging.info(f"Published map for turn {turn}")


# Handle movement updates
def on_movement_message(ch, method, properties, body):
    message = json.loads(body)
    user = message['user']
    x, y = map(int, message['location'].split(','))
    turn = message.get('turn', 0)

    if turn not in maps_by_turn:
        maps_by_turn[turn] = create_map_layout()

    maps_by_turn[turn] = update_map_layout(maps_by_turn[turn], user, x, y)
    publish_map(ch, maps_by_turn[turn], turn)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Flask API
app = Flask(__name__)


# Serve the index.html file for initial map load in the browser
@app.route('/')
def serve_index():
    return send_from_directory('.', 'index.html')


# Endpoint to get map by turn
@app.route('/map/<int:turn>', methods=['GET'])
def get_map(turn):
    if turn in maps_by_turn:
        return jsonify({"turn": turn, "map": maps_by_turn[turn]})
    else:
        return jsonify({"error": "Map for this turn not found"}), 404


def main():
    # Initialize RabbitMQ and set up initial map
    connection, channel = setup_rabbitmq()

    # Initialize the map for turn 0
    maps_by_turn[0] = create_map_layout()
    publish_map(channel, maps_by_turn[0], 0)  # Publish initial map for turn 0

    # Subscribe to 'movement_updates'
    channel.basic_consume(queue='movement_updates',
                          on_message_callback=on_movement_message,
                          auto_ack=False)

    # Start Flask API on a separate thread
    api_thread = Thread(target=app.run, kwargs={'port': 5002})
    api_thread.start()

    logging.info(
        "MapBuilder service started, listening for movement updates...")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("MapBuilder service stopped by user.")
    finally:
        if connection:
            connection.close()
            logging.info("RabbitMQ connection closed.")


if __name__ == '__main__':
    main()
