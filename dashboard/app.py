import streamlit as st
from kafka import KafkaConsumer
import json

st.title("Real-Time Feature Dashboard")

consumer = KafkaConsumer(
    "feature-store",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

data_placeholder = st.empty()

features = []

for message in consumer:
    features.append(message.value)
    data_placeholder.dataframe(features[-20:])