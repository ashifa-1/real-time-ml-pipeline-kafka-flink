import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    # Initialize high-performance unified Flink Table Environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)
    
    config = t_env.get_config().get_configuration()
    config.set_string("parallelism.default", "1")
    config.set_string("pipeline.auto-watermark-interval", "200ms")
    
    kafka_server = "kafka:9092"

    # Source: User Interaction Events with a Bounded Out-Of-Orderness Watermark Strategy (30s)
    t_env.execute_sql(f"""
        CREATE TABLE user_events (
            user_id STRING,
            content_id STRING,
            event_type STRING,
            dwell_time_ms INT,
            event_time_str STRING,
            ts AS TO_TIMESTAMP_LTZ(CAST(TO_TIMESTAMP(REPLACE(event_time_str, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSS') AS BIGINT) * 1000, 3),
            WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'user-events',
            'properties.bootstrap.servers' = '{kafka_server}',
            'properties.group.id' = 'flink-user-events-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # Source: Changelog Table from Compacted Content Metadata
    t_env.execute_sql(f"""
        CREATE TABLE content_metadata (
            content_id STRING,
            category STRING,
            creator_id STRING,
            publish_timestamp STRING,
            PRIMARY KEY (content_id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'content-metadata',
            'properties.bootstrap.servers' = '{kafka_server}',
            'properties.group.id' = 'flink-metadata-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # Sink: Upsert-Kafka Target for the Unified Feature Store Backend
    t_env.execute_sql(f"""
        CREATE TABLE feature_store (
            entity_id STRING,
            feature_name STRING,
            feature_value STRING,
            computed_at STRING,
            PRIMARY KEY (entity_id, feature_name) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'feature-store',
            'properties.bootstrap.servers' = '{kafka_server}',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # Sink: Metric logs for system observability
    t_env.execute_sql(f"""
        CREATE TABLE pipeline_metrics (
            watermark_ms BIGINT,
            wall_clock_ms BIGINT,
            late_event_count BIGINT,
            generated_at STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'pipeline-metrics',
            'properties.bootstrap.servers' = '{kafka_server}',
            'format' = 'json'
        )
    """)

    # Create an atomic execution batch set
    statement_set = t_env.create_statement_set()

    # Feature 1: User Click Rate (1-Hour Tumbling Window)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            user_id AS entity_id,
            'click_rate' AS feature_name,
            CAST(CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS STRING) AS feature_value,
            DATE_FORMAT(MAX(ts), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS computed_at
        FROM user_events
        GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), user_id
    """)

    # Feature 2: User Average Dwell Time (1-Hour Tumbling Window)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            user_id AS entity_id,
            'avg_dwell_time' AS feature_name,
            CAST(AVG(dwell_time_ms) AS STRING) AS feature_value,
            DATE_FORMAT(MAX(ts), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS computed_at
        FROM user_events
        GROUP BY TUMBLE(ts, INTERVAL '1' HOUR), user_id
    """)

    # Feature 3: Per-Content Engagement Rate (15-Minute Sliding Window, 5-Minute Slide)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            content_id AS entity_id,
            'engagement_rate' AS feature_name,
            CAST(
                CASE WHEN COUNT(CASE WHEN event_type = 'view' THEN 1 END) = 0 THEN 0.0
                ELSE CAST(SUM(CASE WHEN event_type IN ('like', 'share') THEN 1 ELSE 0 END) AS DOUBLE) / 
                     COUNT(CASE WHEN event_type = 'view' THEN 1 END)
                END AS STRING
            ) AS feature_value,
            DATE_FORMAT(MAX(ts), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS computed_at
        FROM user_events
        GROUP BY HOP(ts, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE), content_id
    """)

    # Feature 4: User Category Affinity via Stream-Table Temporal Join (1-Hour Tumbling Window)
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            e.user_id AS entity_id,
            CONCAT('category_affinity_score_', COALESCE(m.category, 'unknown')) AS feature_name,
            CAST(COUNT(*) AS STRING) AS feature_value,
            DATE_FORMAT(MAX(e.ts), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS computed_at
        FROM user_events AS e
        LEFT JOIN content_metadata FOR SYSTEM_TIME AS OF e.ts AS m
            ON e.content_id = m.content_id
        GROUP BY TUMBLE(e.ts, INTERVAL '1' HOUR), e.user_id, m.category
    """)

    # Instrumentation pipeline to pass metrics directly to our dashboard
    statement_set.add_insert_sql("""
        INSERT INTO pipeline_metrics
        SELECT 
            CAST(CURRENT_WATERMARK(ts) AS BIGINT) AS watermark_ms,
            CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT) AS wall_clock_ms,
            CAST(SUM(CASE WHEN ts < CURRENT_WATERMARK(ts) THEN 1 ELSE 0 END) AS BIGINT) AS late_event_count,
            DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS generated_at
        FROM user_events
        GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
    """)

    # Trigger job execution
    statement_set.execute()

if __name__ == '__main__':
    main()