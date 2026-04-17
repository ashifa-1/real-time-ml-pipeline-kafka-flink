from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta


from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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

send_metadata()

while True:
    event = generate_event()
    producer.send("user-events", value=event)
    time.sleep(1)