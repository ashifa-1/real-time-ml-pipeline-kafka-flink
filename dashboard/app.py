import json
import os
import threading
from datetime import datetime, timezone

import streamlit as st
from kafka import KafkaConsumer

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FEATURE_TOPIC = "feature-store"
METRICS_TOPIC = ["pipeline-metrics"]
TEST_USER_ID = os.getenv("TEST_USER_ID", "u1")
TEST_CONTENT_ID = os.getenv("TEST_CONTENT_ID", "c1")

st.set_page_config(page_title="Real-Time Feature Dashboard", layout="wide")
st.title("Real-Time Feature Dashboard")

if "feature_store" not in st.session_state:
    st.session_state.feature_store = {}

if "metrics" not in st.session_state:
    st.session_state.metrics = {
        "late_event_count": 0,
        "watermark_ms": None,
        "wall_clock_ms": None,
        "generated_at": None,
    }


@st.cache_resource
def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                FEATURE_TOPIC,
                *METRICS_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                key_deserializer=lambda x: x.decode("utf-8") if x else None,
            )
        except Exception:
            time.sleep(2)


def poll_kafka():
    consumer = create_consumer()
    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    if msg.topic == FEATURE_TOPIC:
                        key = msg.key or f"{msg.value.get('entity_id')}:{msg.value.get('feature_name')}"
                        st.session_state.feature_store[key] = msg.value
                    elif msg.topic == "pipeline-metrics":
                        metrics = msg.value
                        st.session_state.metrics.update(metrics)
                        st.session_state.metrics["generated_at"] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            time.sleep(1)
        time.sleep(0.5)


if "kafka_thread" not in st.session_state:
    thread = threading.Thread(target=poll_kafka, daemon=True)
    thread.start()
    st.session_state.kafka_thread = True

with st.sidebar:
    st.header("Entity Viewer")
    selected_user = st.text_input("Enter user_id", value=TEST_USER_ID)
    selected_content = st.text_input("Enter content_id", value=TEST_CONTENT_ID)
    st.markdown("---")
    st.write("**Pipeline metrics will appear below as they are received.**")

selected_user = selected_user.strip()
selected_content = selected_content.strip()

feature_rows = []
if selected_user:
    for value in st.session_state.feature_store.values():
        if value.get("entity_id") == selected_user:
            feature_rows.append(value)

content_rows = []
if selected_content:
    for value in st.session_state.feature_store.values():
        if value.get("entity_id") == selected_content:
            content_rows.append(value)

st.subheader("User Feature Snapshot")
if feature_rows:
    st.dataframe(feature_rows)
else:
    st.info(f"No feature-store rows found for user_id '{selected_user}' yet.")

st.subheader("Content Feature Snapshot")
if content_rows:
    st.dataframe(content_rows)
else:
    st.info(f"No feature-store rows found for content_id '{selected_content}' yet.")

latest_click_rate = st.session_state.feature_store.get(f"{selected_user}:click_rate")
latest_engagement_rate = st.session_state.feature_store.get(f"{selected_user}:engagement_rate")

if latest_click_rate and "computed_at" in latest_click_rate:
    click_freshness = datetime.utcnow().replace(tzinfo=timezone.utc) - datetime.fromisoformat(latest_click_rate["computed_at"].replace("Z", "+00:00"))
    click_freshness_str = f"{int(click_freshness.total_seconds())} sec ago"
else:
    click_freshness_str = "N/A"

if latest_engagement_rate and "computed_at" in latest_engagement_rate:
    engagement_freshness = datetime.utcnow().replace(tzinfo=timezone.utc) - datetime.fromisoformat(latest_engagement_rate["computed_at"].replace("Z", "+00:00"))
    engagement_freshness_str = f"{int(engagement_freshness.total_seconds())} sec ago"
else:
    engagement_freshness_str = "N/A"

watermark_ms = st.session_state.metrics.get("watermark_ms")
wall_clock_ms = st.session_state.metrics.get("wall_clock_ms")
if watermark_ms is not None and wall_clock_ms is not None and watermark_ms >= 0:
    watermark_lag = int((wall_clock_ms - watermark_ms) / 1000)
    watermark_lag_str = f"{watermark_lag} sec"
else:
    watermark_lag_str = "N/A"

st.subheader("Operational Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Click Rate Freshness", click_freshness_str)
col2.metric("Engagement Rate Freshness", engagement_freshness_str)
col3.metric("Watermark Lag", watermark_lag_str)

st.markdown("---")
col4, col5 = st.columns(2)
col4.metric("Late Events Count", st.session_state.metrics.get("late_event_count", 0))
col5.metric("Last Metrics Update", st.session_state.metrics.get("generated_at", "waiting..."))

st.subheader("Raw Feature Store Sample")
sample_rows = list(st.session_state.feature_store.values())[-20:]
st.table(sample_rows)
