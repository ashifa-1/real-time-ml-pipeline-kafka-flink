import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
USER_EVENTS_TOPIC = os.getenv("USER_EVENTS_TOPIC", "user-events")
CONTENT_METADATA_TOPIC = os.getenv("CONTENT_METADATA_TOPIC", "content-metadata")
FEATURE_STORE_TOPIC = os.getenv("FEATURE_STORE_TOPIC", "feature-store")

users = ["u1", "u2", "u3", "u4"]
contents = ["c1", "c2", "c3", "c4"]
user_archetypes = {
    "u1": {"weights": {"view": 0.7, "click": 0.15, "like": 0.1, "share": 0.05}, "dwell": (200, 1200)},
    "u2": {"weights": {"view": 0.55, "click": 0.25, "like": 0.15, "share": 0.05}, "dwell": (300, 2000)},
    "u3": {"weights": {"view": 0.5, "click": 0.2, "like": 0.2, "share": 0.1}, "dwell": (500, 3000)},
    "u4": {"weights": {"view": 0.65, "click": 0.2, "like": 0.1, "share": 0.05}, "dwell": (150, 900)},
}

SIMULATION_STEP_SECONDS = 60
LATE_EVENT_PROBABILITY = 0.05


def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topic_configs = [
        NewTopic(USER_EVENTS_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(CONTENT_METADATA_TOPIC, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": "compact"}),
        NewTopic(FEATURE_STORE_TOPIC, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": "compact"}),
    ]
    try:
        admin.create_topics(new_topics=topic_configs, validate_only=False)
        print("✓ Created Kafka topics")
    except TopicAlreadyExistsError:
        print("✓ Kafka topics already exist")
    except Exception as e:
        print(f"⚠️ Kafka topic creation warning: {e}")
    finally:
        admin.close()


def make_producer():
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"✓ Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except Exception as exc:
            print(f"✗ Kafka connection failed (attempt {attempt}/10): {exc}")
            time.sleep(3)
    raise SystemExit("Unable to connect to Kafka")


def current_iso(ts=None):
    ts = ts or datetime.now(timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")


def weighted_choice(weights):
    choices, probabilities = zip(*weights.items())
    return random.choices(choices, probabilities, k=1)[0]


def generate_event(simulation_time):
    user_id = random.choice(users)
    archetype = user_archetypes[user_id]
    is_late = random.random() < LATE_EVENT_PROBABILITY
    timestamp = simulation_time
    if is_late:
        timestamp -= timedelta(seconds=random.randint(35, 90))
    return {
        "user_id": user_id,
        "content_id": random.choice(contents),
        "event_type": weighted_choice(archetype["weights"]),
        "dwell_time_ms": random.randint(*archetype["dwell"]),
        "timestamp": current_iso(timestamp),
    }


def send_metadata(producer):
    content_categories = ["sports", "news", "movies"]
    for content_id in contents:
        metadata = {
            "content_id": content_id,
            "category": random.choice(content_categories),
            "creator_id": f"creator_{content_id}",
            "publish_timestamp": current_iso(),
        }
        producer.send(CONTENT_METADATA_TOPIC, key=content_id, value=metadata)
    producer.flush()


def healthcheck():
    try:
        create_topics()
        producer = make_producer()
        producer.close()
        print("✓ Producer healthcheck passed")
        return 0
    except Exception as exc:
        print(f"✗ Producer healthcheck failed: {exc}")
        return 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--healthcheck", action="store_true")
    args = parser.parse_args()

    if args.healthcheck:
        raise SystemExit(healthcheck())

    create_topics()
    producer = make_producer()
    print("Sending initial content metadata...")
    send_metadata(producer)
    print("Metadata published successfully.")

    simulated_time = datetime.now(timezone.utc)
    event_counter = 0

    while True:
        simulated_time = datetime.now(timezone.utc) + timedelta(seconds=event_counter * SIMULATION_STEP_SECONDS)
        event = generate_event(simulated_time)
        producer.send(USER_EVENTS_TOPIC, value=event)
        event_counter += 1
        if event_counter % 10 == 0:
            producer.flush()
        time.sleep(0.4)


if __name__ == "__main__":
    main()
