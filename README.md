# Real-Time ML Feature Pipeline

## Overview

The aim of this project is to design and implement a real-time machine learning feature pipeline using streaming data. The system ingests user interaction events and content metadata, processes them in real time, and generates meaningful features that can be used for downstream ML applications.

This project demonstrates how streaming systems can be used to compute user behavior metrics, content engagement, and personalized insights with low latency.

---

## Architecture

The system follows a streaming architecture:

* Kafka Producer generates user events and content metadata
* Kafka topics store streaming data
* A Python-based stream processor consumes and processes events
* Features are written to a feature-store topic
* A Streamlit dashboard visualizes the output in real time

---

## Features

### User Features

* avg_dwell_time_window
* click_rate_window

### Content Features

* engagement_rate

### Derived Features

* category_affinity per user

---

## The Flow

The complete data flow is:

Producer → Kafka (user-events, content-metadata) → Processor → Feature Store → Dashboard

1. Producer generates real-time user activity
2. Kafka stores and streams the data
3. Processor performs:

   * feature computation
   * metadata join
   * window-based aggregation
4. Results are stored in the feature-store topic
5. Dashboard displays live feature updates

---

## Technologies Used

* Apache Kafka
* Python
* Streamlit
* Docker

---

## How to Run

### Step 1: Clone the repository

git clone https://github.com/ashifa-1/real-time-ml-pipeline-kafka-flink.git
cd real-time-ml-pipeline-kafka-flink

### Step 2: Run the project

docker-compose up --build

### Step 3: Open dashboard

http://localhost:8501

---

## Conclusion

This project successfully demonstrates a real-time streaming pipeline for machine learning feature engineering. It covers key concepts such as event streaming, feature computation, joins, and windowing. The system provides a scalable and efficient approach to generate real-time insights that can be directly used in ML models.
