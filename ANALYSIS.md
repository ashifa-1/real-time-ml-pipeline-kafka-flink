# Analysis

## Batch vs Streaming Processing

Batch processing handles large volumes of data at fixed intervals, leading to higher latency. Streaming processing handles data in real-time, allowing immediate insights and updates. This project uses streaming to ensure low-latency feature computation.

## Handling Late Events

The producer simulates late events by introducing delayed timestamps. The processor handles them as part of the stream without strict time-based rejection, ensuring robustness in real-world scenarios.

## Windowing Strategy

A sliding window approach is implemented using a fixed-size buffer of recent events. This simulates real-time window-based aggregation similar to frameworks like Apache Flink.

## Join Strategy

A stream-table join is implemented by loading content metadata into memory and enriching incoming user events. This allows combining dynamic event data with static reference data.

## Scalability Considerations

* Kafka ensures distributed and scalable ingestion
* Stateless processing logic can be extended to distributed systems
* Feature-store topic allows multiple downstream consumers

## Limitations

* Windowing is event-count based instead of time-based
* No fault tolerance for processor state
* Python-based processor instead of native Flink

## Conclusion

The system successfully demonstrates a real-time feature pipeline with streaming ingestion, transformation, and visualization. It captures key concepts used in production-grade ML pipelines.
