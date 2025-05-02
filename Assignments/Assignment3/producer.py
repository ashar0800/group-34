
Original file is located at
    https://colab.research.google.com/drive/10ilfHOWLSSqi8im1DEuEivDdcT43Hw0H

# For the purpose of this assignment, I am using a cloud instance of Kafka, provisioned by Confluent, which can be utilized in Colab via API key.
"""

!pip install confluent_kafka

from confluent_kafka import Producer, Consumer
import json
import time
import random
from datetime import datetime

conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'NJ6GH7RE6V3ZAQM2',
    'sasl.password': 'flVXlUTe/q+MHaGH1G2aFYd58q7Jmx6ls+hL5FVYW9ha2OSa2pgUDWbmRMemHUv3'
}

# Create Kafka Producer
producer = Producer(conf)

# Predefined list of 5 sensor IDs
sensor_ids = ["S101", "S102", "S103", "S104", "S105"]

# Define congestion levels
congestion_levels = ["LOW", "MEDIUM", "HIGH"]

import random
from datetime import datetime
import builtins  # Import builtins module

def generate_traffic_data():
    sensor_id = random.choice(sensor_ids)  # Pick a random sensor
    timestamp = datetime.utcnow().isoformat()
    vehicle_count = random.randint(5, 50)  # Vehicles detected in last second
    average_speed = builtins.round(random.uniform(20, 100), 1)  # Use builtins.round to use Python's round function
    # Determine congestion level
    if vehicle_count > 35:
        congestion_level = "HIGH"
    elif vehicle_count > 20:
        congestion_level = "MEDIUM"
    else:
        congestion_level = "LOW"

    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "vehicle_count": vehicle_count,
        "average_speed": average_speed,  # average_speed is now a regular float
        "congestion_level": congestion_level
    }

# Experimenting locally before using kafka
def send_traffic_data(topic, interval=1, count=20):
    for _ in range(count):
        data = generate_traffic_data()
        producer.produce(topic, key=data["sensor_id"], value=json.dumps(data))
        producer.flush()
        print(f"Sent: {data}")
        time.sleep(1)

send_traffic_data(topic="traffic_data", interval=1, count=100)

"""# Part 1: Kafka Producer"""

import json
import time
import random
import threading

KAFKA_BROKER = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
KAFKA_TOPIC = "traffic_data"

# Configuring Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'NJ6GH7RE6V3ZAQM2',
    'sasl.password': 'flVXlUTe/q+MHaGH1G2aFYd58q7Jmx6ls+hL5FVYW9ha2OSa2pgUDWbmRMemHUv3' # confidential
    }
producer = Producer(producer_conf)

# Simulate Traffic Data
def generate_traffic_data():
    sensors = ["S101", "S102", "S103", "S104", "S105"]
    congestion_levels = ["LOW", "MEDIUM", "HIGH"]

    while True:
        sensor_id = random.choice(sensors)
        vehicle_count = random.randint(5, 50)
        avg_speed = round(random.uniform(20, 80), 2)
        congestion = random.choice(congestion_levels)
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")

        traffic_event = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "vehicle_count": vehicle_count,
            "average_speed": avg_speed,
            "congestion_level": congestion
        }

        producer.produce(KAFKA_TOPIC, json.dumps(traffic_event).encode('utf-8'))
        producer.flush()
        print(f"Sent: {traffic_event}")
        time.sleep(1)

# Running Kafka Producer in a separate thread
producer_thread = threading.Thread(target=generate_traffic_data, daemon=True)
producer_thread.start()