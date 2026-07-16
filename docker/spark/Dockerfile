# bookworm pin: Spark 3.5 needs Java 17, which Debian trixie no longer ships.
FROM python:3.12-slim-bookworm

# Spark needs a JVM and `ps` (procps) at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /opt/telestream

# The producer package provides the shared catalog the rules engine reads.
COPY pyproject.toml README.md ./
COPY producer/ producer/
RUN pip install --no-cache-dir . psycopg2-binary "pyspark==3.5.3"

# Kafka connector jars baked into pyspark's jar dir so startup needs no internet.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))"); \
    cd "$JARS"; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY spark/ spark/
ENV PYTHONPATH=/opt/telestream/spark

RUN mkdir -p /data/lake /data/checkpoints

CMD ["spark-submit", "--master", "local[2]", \
     "--driver-memory", "1g", \
     "/opt/telestream/spark/streaming_job.py"]
