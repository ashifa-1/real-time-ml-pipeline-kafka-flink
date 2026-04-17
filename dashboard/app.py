import streamlit as st
from kafka import KafkaConsumer
import json
import time
import os

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

st.title("Real-Time Feature Dashboard")

def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                "feature-store",
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="latest"
            )
        except:
            time.sleep(2)

consumer = create_consumer()

data_placeholder = st.empty()
features = []

for message in consumer:
    features.append(message.value)
    data_placeholder.dataframe(features[-20:])