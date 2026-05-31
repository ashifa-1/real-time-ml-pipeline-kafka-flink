import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
USER_EVENTS_TOPIC = os.getenv("USER_EVENTS_TOPIC", "user-events")
CONTENT_METADATA_TOPIC = os.getenv("CONTENT_METADATA_TOPIC", "content-metadata")
FEATURE_STORE_TOPIC = os.getenv("FEATURE_STORE_TOPIC", "feature-store")
PIPELINE_METRICS_TOPIC = "pipeline-metrics"

users = ["u1", "u2", "u3", "u4"]
contents = ["c1", "c2", "c3", "c4"]
categories = ["sports", "news", "movies"]

user_archetypes = {
    "u1": {"weights": {"view": 0.70, "click": 0.15, "like": 0.10, "share": 0.05}, "dwell": (200, 1200)},
    "u2": {"weights": {"view": 0.55, "click": 0.25, "like": 0.15, "share": 0.05}, "dwell": (300, 2000)},
    "u3": {"weights": {"view": 0.50, "click": 0.20, "like": 0.20, "share": 0.10}, "dwell": (500, 3000)},
    "u4": {"weights": {"view": 0.65, "click": 0.20, "like": 0.10, "share": 0.05}, "dwell": (150, 900)},
}

def create_topics():
    for attempt in range(1, 15):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, request_timeout_ms=5000)
            existing = admin.list_topics()
            
            topic_list = []
            if USER_EVENTS_TOPIC not in existing:
                topic_list.append(NewTopic(USER_EVENTS_TOPIC, num_partitions=3, replication_factor=1))
            if CONTENT_METADATA_TOPIC not in existing:
                topic_list.append(NewTopic(CONTENT_METADATA_TOPIC, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": "compact"}))
            if FEATURE_STORE_TOPIC not in existing:
                topic_list.append(NewTopic(FEATURE_STORE_TOPIC, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": "compact"}))
            if PIPELINE_METRICS_TOPIC not in existing:
                topic_list.append(NewTopic(PIPELINE_METRICS_TOPIC, num_partitions=1, replication_factor=1))
                
            if topic_list:
                admin.create_topics(new_topics=topic_list)
            admin.close()
            return
        except Exception:
            time.sleep(3)
    raise SystemExit("Kafka Provisioning Intercept Error.")

def get_producer():
    for attempt in range(1, 10):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception:
            time.sleep(3)
    raise SystemExit("Producer connection termination error.")

def to_iso_format(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def generate_user_event(sim_time):
    u_id = random.choice(users)
    arch = user_archetypes[u_id]
    ev_type = random.choices(list(arch["weights"].keys()), weights=list(arch["weights"].values()))[0]
    
    # Strictly ensure that 5% of events are late (35-90 seconds behind)
    is_late = random.random() < 0.05
    event_time = sim_time - timedelta(seconds=random.randint(35, 90)) if is_late else sim_time
    
    return {
        "user_id": u_id,
        "content_id": random.choice(contents),
        "event_type": ev_type,
        "dwell_time_ms": random.randint(*arch["dwell"]),
        "timestamp": to_iso_format(event_time)
    }

def send_static_metadata(prod):
    for index, c_id in enumerate(contents):
        payload = {
            "content_id": c_id,
            "category": categories[index % len(categories)],
            "creator_id": f"creator_{c_id}",
            "publish_timestamp": to_iso_format(datetime.now(timezone.utc) - timedelta(days=1))
        }
        prod.send(CONTENT_METADATA_TOPIC, key=c_id, value=payload)
    prod.flush()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--healthcheck", action="store_true")
    args = parser.parse_args()

    if args.healthcheck:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            topics = admin.list_topics()
            admin.close()
            if USER_EVENTS_TOPIC in topics:
                print("Healthy")
                return
            raise Exception("Topics not initialized.")
        except Exception:
            exit(1)

    create_topics()
    prod = get_producer()
    send_static_metadata(prod)

    # Time acceleration loop: 1 second wall-clock maps to 1 minute of simulated stream time
    simulated_clock = datetime.now(timezone.utc)
    while True:
        for _ in range(5): 
            evt = generate_user_event(simulated_clock)
            prod.send(USER_EVENTS_TOPIC, value=evt)
            simulated_clock += timedelta(seconds=12) # Accelerate time forward
        prod.flush()
        time.sleep(1.0)

if __name__ == "__main__":
    main()