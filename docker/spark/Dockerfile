FROM apache/spark:3.5.3

USER root

# Kafka connector jars baked into the image so startup needs no internet.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

WORKDIR /opt/telestream

# The producer package provides the shared catalog the rules engine reads.
COPY pyproject.toml README.md ./
COPY producer/ producer/
RUN pip3 install --no-cache-dir . psycopg2-binary

COPY spark/ spark/
ENV PYTHONPATH=/opt/telestream/spark

RUN mkdir -p /data/lake /data/checkpoints

CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", \
     "--driver-memory", "1g", \
     "/opt/telestream/spark/streaming_job.py"]
