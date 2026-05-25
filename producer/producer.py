from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_BROKER", "kafka:9092")

# Retry logic to wait for Kafka to be ready
producer = None
max_retries = 10
retry_count = 0

while producer is None and retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"✓ Connected to Kafka at {KAFKA_SERVER}")
    except Exception as e:
        retry_count += 1
        print(f"✗ Kafka connection failed (attempt {retry_count}/{max_retries}): {e}")
        time.sleep(3)

if producer is None:
    print("Failed to connect to Kafka after retries")
    exit(1)

users = ["u1", "u2", "u3", "u4"]
contents = ["c1", "c2", "c3", "c4"]
event_types = ["view", "click", "like", "share"]

def current_time():
    return datetime.utcnow()

def generate_event():
    is_late = random.random() < 0.05
    event_time = current_time()
    if is_late:
        event_time = event_time - timedelta(seconds=random.randint(35, 90))
    return {
        "user_id": random.choice(users),
        "content_id": random.choice(contents),
        "event_type": random.choice(event_types),
        "dwell_time_ms": random.randint(100, 5000),
        "timestamp": event_time.isoformat() + "Z"
    }

def send_metadata():
    for c in contents:
        data = {
            "content_id": c,
            "category": random.choice(["sports", "news", "movies"]),
            "creator_id": "creator_" + c,
            "publish_timestamp": current_time().isoformat() + "Z"
        }
        producer.send("content-metadata", key=c.encode(), value=data)
    producer.flush()

print("Sending metadata...")
send_metadata()
print("Metadata sent. Starting event generation...")

while True:
    event = generate_event()
    producer.send("user-events", value=event)
    time.sleep(1)