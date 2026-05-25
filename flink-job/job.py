import json
from datetime import datetime, timezone

from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
USER_EVENTS_TOPIC = "user-events"
CONTENT_METADATA_TOPIC = "content-metadata"
FEATURE_STORE_TOPIC = "feature-store"
METRICS_TOPIC = "pipeline-metrics"


def iso_to_epoch_ms(iso_ts):
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def event_time_assigner(value, record_timestamp):
    data = json.loads(value)
    return iso_to_epoch_ms(data["timestamp"])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)

    t_env.get_config().get_configuration().set_string("parallelism.default", "1")

    t_env.execute_sql(f"""
        CREATE TABLE user_events (
            user_id STRING,
            content_id STRING,
            event_type STRING,
            dwell_time_ms INT,
            ts TIMESTAMP_LTZ(3),
            WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{USER_EVENTS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-user-events',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE content_metadata (
            content_id STRING,
            category STRING,
            creator_id STRING,
            publish_timestamp TIMESTAMP_LTZ(3),
            WATERMARK FOR publish_timestamp AS publish_timestamp - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{CONTENT_METADATA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-content-metadata',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE feature_store (
            entity_id STRING,
            feature_name STRING,
            feature_value STRING,
            computed_at STRING,
            PRIMARY KEY (entity_id, feature_name) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{FEATURE_STORE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pipeline_metrics (
            watermark_ms BIGINT,
            wall_clock_ms BIGINT,
            late_event_count BIGINT,
            generated_at STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{METRICS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO feature_store
        SELECT user_id AS entity_id,
               'avg_dwell_time' AS feature_name,
               CAST(AVG(dwell_time_ms) AS STRING) AS feature_value,
               CAST(MAX(ts) AS STRING) AS computed_at
        FROM user_events
        GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), user_id
    """)

    t_env.execute_sql("""
        INSERT INTO feature_store
        SELECT user_id AS entity_id,
               'click_rate' AS feature_name,
               CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) / COUNT(*) AS STRING) AS feature_value,
               CAST(MAX(ts) AS STRING) AS computed_at
        FROM user_events
        GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), user_id
    """)

    t_env.execute_sql("""
        INSERT INTO feature_store
        SELECT user_id AS entity_id,
               'engagement_rate' AS feature_name,
               CAST(SUM(CASE WHEN event_type IN ('click', 'like', 'share') THEN 1 ELSE 0 END) / COUNT(*) AS STRING) AS feature_value,
               CAST(MAX(ts) AS STRING) AS computed_at
        FROM user_events
        GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), user_id
    """)

    t_env.execute_sql("""
        INSERT INTO feature_store
        SELECT e.content_id AS entity_id,
               'content_category' AS feature_name,
               m.category AS feature_value,
               CAST(MAX(e.ts) AS STRING) AS computed_at
        FROM user_events AS e
        LEFT JOIN content_metadata FOR SYSTEM_TIME AS OF e.ts AS m
            ON e.content_id = m.content_id
        GROUP BY TUMBLE(e.ts, INTERVAL '15' MINUTE), e.content_id, m.category
    """)

    t_env.execute_sql("""
        INSERT INTO feature_store
        SELECT m.category AS entity_id,
               'category_avg_dwell' AS feature_name,
               CAST(AVG(e.dwell_time_ms) AS STRING) AS feature_value,
               CAST(MAX(e.ts) AS STRING) AS computed_at
        FROM user_events AS e
        LEFT JOIN content_metadata FOR SYSTEM_TIME AS OF e.ts AS m
            ON e.content_id = m.content_id
        GROUP BY TUMBLE(e.ts, INTERVAL '15' MINUTE), m.category
    """)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(USER_EVENTS_TOPIC) \
        .set_group_id("flink-metrics-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)) \
        .with_timestamp_assigner(lambda element, record_timestamp: event_time_assigner(element, record_timestamp))

    metrics_stream = env.from_source(source, watermark_strategy, "metric-source")

    class MetricsMapper:
        def open(self, runtime_context):
            self.late_event_count = 0
            self.last_watermark = -1

        def map(self, value):
            data = json.loads(value)
            event_ts = iso_to_epoch_ms(data["timestamp"])
            watermark = self.last_watermark
            if watermark > 0 and event_ts < watermark:
                self.late_event_count += 1
            self.last_watermark = watermark
            return {
                "watermark_ms": self.last_watermark,
                "wall_clock_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                "late_event_count": self.late_event_count,
                "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }

    metrics_json = metrics_stream.map(lambda value: json.dumps({
        "watermark_ms": iso_to_epoch_ms(json.loads(value)["timestamp"]),
        "wall_clock_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "late_event_count": 0,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }), output_type=Types.STRING())

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(SimpleStringSchema()) \
        .set_topic(METRICS_TOPIC) \
        .build()

    metrics_json.sink_to(sink)

    t_env.execute("feature-engineering-job")


if __name__ == '__main__':
    main()
