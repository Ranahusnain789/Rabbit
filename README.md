# RabbitRunner
RabbitRunner is a Python-based solution that leverages RabbitMQ to handle task queues and message passing efficiently. Ideal for systems that need high performance, asynchronous task execution, and real-time communication, RabbitRunner simplifies distributed computing by providing an easy-to-use interface for managing messages and tasks.

# Features
- Queue Management: Seamlessly create and manage RabbitMQ queues.
- Message Passing: Enables fast, reliable communication across distributed services.
- Asynchronous Execution: Processes tasks asynchronously, ideal for microservices and background job processing.
- Scalable Architecture: Easily scales up to handle increasing workload by adding more consumers.

# Installation
Clone this repository and install the required packages:

```bash
git clone https://github.com/Ranahusnain789/RabbitRunner.git
cd RabbitRunner
pip install -r requirements.txt
```

## Configure RabbitMQ connection:
Ensure RabbitMQ is running on your system, either locally or on a remote server.
Before running the project, you need to update the RabbitMQ address in the configuration files.

### Step 1: Update Config.json

Open `Config.json` and add your RabbitMQ server's address. It should look like this:

```json
{
  "rabbitmq_address": "your_rabbitmq_address_here"
}
```
Replace `your_rabbitmq_address_here` with the actual RabbitMQ URL or IP address (e.g., localhost for a local server or the full address for a remote server).

### Step 2: Update testrabbit.py

Open `testrabbit.py` and add your RabbitMQ server's address. It should look like this:

```python
# RabbitMQ connection details
rabbitmq_address = "your_rabbitmq_address_here"
```
Replace `your_rabbitmq_address_here` with the actual RabbitMQ URL or IP address (e.g., localhost for a local server or the full address for a remote server).

# Usage
Configure RabbitMQ Settings: Adjust RABBITMQ_HOST, RABBITMQ_PORT, and other parameters in the configuration file.

## Starting a Producer:
Use the producer.py file to send messages to the queue.

```bash
python producer.py
```
## Starting a Consumer:
Run the consumer.py script to process tasks from the queue.

```bash
python consumer.py
```
RabbitRunner handles sending and receiving messages, allowing developers to focus on task processing logic.

### Examples
To enqueue a task:

```python
from producer import send_message

send_message("task_queue", "Process this task")
```
To process messages:

```python
from consumer import start_consuming

start_consuming("task_queue")
```
# Requirements
- Python 3.7+
- RabbitMQ Server
- Pika (Python RabbitMQ client)

# Install dependencies using:

```bash
pip install -r requirements.txt
```
# License
This project is licensed under the MIT License. See the LICENSE file for details.

# Contributing
Contributions are welcome! Please fork the repository, make your changes, and submit a pull request.

# Support
For any questions or issues, please open an issue on GitHub or contact the [ranausnain770@gmail.com](mailto:ranausnain770@gmail.com) .
