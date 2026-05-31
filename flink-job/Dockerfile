FROM apache/flink:1.17-scala_2.12

USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.17.2

# Explicitly download compatible Kafka SQL Connectors
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

WORKDIR /opt/flink/job
COPY job.py /opt/flink/job/job.py

ENV PYFLINK_CLIENT_EXECUTABLE=python3
CMD ["flink", "run", "-m", "flink-jobmanager:8081", "-py", "/opt/flink/job/job.py"]