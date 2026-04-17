from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
from dotenv import load_dotenv
import os
import time

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def create_consumer(topic):
    while True:
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest"
            )
        except:
            time.sleep(2)

def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except:
            time.sleep(2)

consumer = create_consumer("user-events")
metadata_consumer = create_consumer("content-metadata")
producer = create_producer()

user_data = {}
content_data = {}
metadata_store = {}
category_data = {}

for msg in metadata_consumer:
    data = msg.value
    metadata_store[data["content_id"]] = data
    if len(metadata_store) >= 4:
        break

WINDOW_SIZE = 10

for message in consumer:
    event = message.value

    user = event["user_id"]
    content = event["content_id"]
    event_type = event["event_type"]

    category = metadata_store.get(content, {}).get("category")

    if user not in user_data:
        user_data[user] = {"events": [], "clicks": 0}

    user_data[user]["events"].append(event)

    if len(user_data[user]["events"]) > WINDOW_SIZE:
        removed = user_data[user]["events"].pop(0)
        if removed["event_type"] == "click":
            user_data[user]["clicks"] -= 1

    if event_type == "click":
        user_data[user]["clicks"] += 1

    total_dwell = sum(e["dwell_time_ms"] for e in user_data[user]["events"])
    count = len(user_data[user]["events"])

    avg_dwell = total_dwell / count
    click_rate = user_data[user]["clicks"] / count

    producer.send("feature-store", value={
        "entity_id": user,
        "feature_name": "avg_dwell_time_window",
        "feature_value": avg_dwell,
        "computed_at": datetime.utcnow().isoformat() + "Z"
    })

    producer.send("feature-store", value={
        "entity_id": user,
        "feature_name": "click_rate_window",
        "feature_value": click_rate,
        "computed_at": datetime.utcnow().isoformat() + "Z"
    })

    if content not in content_data:
        content_data[content] = {"views": 0, "likes": 0, "shares": 0}

    if event_type == "view":
        content_data[content]["views"] += 1
    if event_type == "like":
        content_data[content]["likes"] += 1
    if event_type == "share":
        content_data[content]["shares"] += 1

    views = content_data[content]["views"]
    likes = content_data[content]["likes"]
    shares = content_data[content]["shares"]

    engagement_rate = (likes + shares) / views if views > 0 else 0

    producer.send("feature-store", value={
        "entity_id": content,
        "feature_name": "engagement_rate",
        "feature_value": engagement_rate,
        "computed_at": datetime.utcnow().isoformat() + "Z"
    })

    if category:
        if user not in category_data:
            category_data[user] = {}

        if category not in category_data[user]:
            category_data[user][category] = 0

        category_data[user][category] += 1

        for cat in category_data[user]:
            producer.send("feature-store", value={
                "entity_id": user,
                "feature_name": "category_affinity_" + cat,
                "feature_value": category_data[user][cat],
                "computed_at": datetime.utcnow().isoformat() + "Z"
            })

    producer.flush()