import json
import os
import threading
import time
from datetime import datetime, timezone
import streamlit as st
from kafka import KafkaConsumer

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FEATURE_TOPIC = "feature-store"
METRICS_TOPIC = "pipeline-metrics"
TEST_USER_ID = os.getenv("TEST_USER_ID", "u1")
TEST_CONTENT_ID = os.getenv("TEST_CONTENT_ID", "c1")

st.set_page_config(page_title="Real-Time Feature Dashboard", layout="wide")
st.title("🛡️ Real-Time Feature Engineering Observability Dashboard")

if "feature_store" not in st.session_state:
    st.session_state.feature_store = {}
if "metrics" not in st.session_state:
    st.session_state.metrics = {
        "late_event_count": 0,
        "watermark_ms": 0,
        "wall_clock_ms": 0,
        "generated_at": "N/A",
    }

@st.cache_resource
def get_consumer():
    while True:
        try:
            return KafkaConsumer(
                FEATURE_TOPIC,
                METRICS_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                key_deserializer=lambda x: x.decode("utf-8") if x else None,
            )
        except Exception:
            time.sleep(2)

def poll_kafka():
    consumer = get_consumer()
    while True:
        try:
            messages = consumer.poll(timeout_ms=500)
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    if msg.topic == FEATURE_TOPIC:
                        val = msg.value
                        if val and "entity_id" in val and "feature_name" in val:
                            comp_key = f"{val['entity_id']}:{val['feature_name']}"
                            st.session_state.feature_store[comp_key] = val
                    elif msg.topic == METRICS_TOPIC:
                        st.session_state.metrics.update(msg.value)
        except Exception:
            time.sleep(1)
        time.sleep(0.1)

if "kafka_started" not in st.session_state:
    thread = threading.Thread(target=poll_kafka, daemon=True)
    thread.start()
    st.session_state.kafka_started = True

with st.sidebar:
    st.header("🔍 Entity Lookup Engine")
    selected_user = st.text_input("Target User ID Verification", value=TEST_USER_ID).strip()
    selected_content = st.text_input("Target Content ID Verification", value=TEST_CONTENT_ID).strip()
    st.markdown("---")
    st.button("Manual Interface Refresh")

# Filter Real-Time States
user_rows = [v for v in st.session_state.feature_store.values() if v.get("entity_id") == selected_user]
content_rows = [v for v in st.session_state.feature_store.values() if v.get("entity_id") == selected_content]

col_u, col_c = st.columns(2)

with col_u:
    st.subheader("👤 User Features (1-Hour Tumbling Window)")
    if user_rows:
        st.dataframe(user_rows, use_container_width=True)
    else:
        st.info(f"No tumbling parameters generated yet for user: {selected_user}")

with col_c:
    st.subheader("🎬 Content Features (15-Min Sliding Window / Stream Join)")
    if content_rows:
        st.dataframe(content_rows, use_container_width=True)
    else:
        st.info(f"No dynamic statistics generated yet for content: {selected_content}")

# Calculate metrics freshness
def fetch_freshness(f_key):
    f_data = st.session_state.feature_store.get(f_key)
    if f_data and "computed_at" in f_data:
        try:
            ts_str = f_data["computed_at"].replace("Z", "+00:00")
            diff = datetime.now(timezone.utc) - datetime.fromisoformat(ts_str)
            return f"{max(0, int(diff.total_seconds()))}s ago"
        except Exception:
            return "Format Err"
    return "N/A"

click_fresh = fetch_freshness(f"{selected_user}:click_rate")
eng_fresh = fetch_freshness(f"{selected_content}:engagement_rate")

w_ms = st.session_state.metrics.get("watermark_ms", 0)
wc_ms = st.session_state.metrics.get("wall_clock_ms", 0)
lag_str = f"{max(0, int((wc_ms - w_ms) / 1000))}s" if w_ms and wc_ms else "N/A"

st.markdown("---")
st.subheader("📊 Operational System Health Metrics")
m_col1, m_col2, m_col3, m_col4 = st.columns(4)

m_col1.metric("Click Rate Freshness", click_fresh)
m_col2.metric("Engagement Rate Freshness", eng_fresh)
m_col3.metric("Current Watermark Lag", lag_str)
m_col4.metric("Late Events Dropped (Engine)", st.session_state.metrics.get("late_event_count", 0))

st.markdown("---")
st.subheader("📋 Raw Global Feature Log (Last 10 Records)")
st.table(list(st.session_state.feature_store.values())[-10:])

time.sleep(1)
st.rerun()