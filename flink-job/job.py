from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows

import json
from datetime import datetime

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("user-events") \
    .set_group_id("flink-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(
    source,
    WatermarkStrategy.for_bounded_out_of_orderness(Time.seconds(30)),
    "kafka-source"
)

def parse_event(value):
    data = json.loads(value)
    return (data["user_id"], data["event_type"], data["dwell_time_ms"])

mapped = stream.map(parse_event, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()]))

def aggregate(a, b):
    return (a[0], a[1] + b[1], a[2] + b[2])

windowed = mapped \
    .key_by(lambda x: x[0]) \
    .window(TumblingEventTimeWindows.of(Time.hours(1))) \
    .reduce(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))

def format_output(value):
    return json.dumps({
        "entity_id": value[0],
        "feature_name": "avg_dwell_time",
        "feature_value": value[2],
        "computed_at": datetime.utcnow().isoformat() + "Z"
    })

output = windowed.map(format_output)

sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(SimpleStringSchema()) \
    .set_topic("feature-store") \
    .build()

output.sink_to(sink)

env.execute("feature-engineering-job")