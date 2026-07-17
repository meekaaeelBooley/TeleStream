"""TeleStream Spark Structured Streaming job.

One checkpointed streaming query consumes all domain topics; per micro-batch:
parse → schema validation → business rules → dedupe → Parquet lake append +
idempotent PostgreSQL upserts. Every rejection is published to the
failed-transactions topic. A second query persists that DLQ topic into the
warehouse for the review dashboard.

Scale note: validation and sinking run on the driver over collected batches —
the right trade at laptop volume (hundreds of events/batch). The seam for
scaling out is moving rules into a UDF and sinks into foreachPartition;
nothing in the batch contract changes.
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from telestream_spark import sinks
from telestream_spark.rules import apply_rules
from telestream_spark.schemas import (
    DLQ_TOPIC,
    EVENT_FIELDS,
    TOPIC_TO_EVENT_TYPE,
    schema_violations,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("telestream.spark")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://telestream:telestream@postgres:5432/telestream"
)
LAKE_PATH = os.environ.get("LAKE_PATH", "/data/lake")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "5 seconds")

# subscriber-created is processed first so same-batch facts find their dimension row.
TOPIC_ORDER = list(TOPIC_TO_EVENT_TYPE)

_TYPE_MAP: dict[type, DataType] = {
    str: StringType(),
    int: LongType(),
    float: DoubleType(),
    bool: BooleanType(),
}

# Deterministic DLQ event ids: replaying a batch reproduces the same id, so
# the warehouse dedupes republished rejections on event_id.
_DLQ_NAMESPACE = uuid.UUID("a7d0d0f0-5c6b-4c5e-9b3e-2f1c0000dead")

Event = dict[str, Any]


def _struct_type(event_type: str) -> StructType:
    return StructType(
        [
            StructField(name, _TYPE_MAP[py_type], nullable=False)
            for name, py_type in EVENT_FIELDS[event_type].items()
        ]
    )


def make_dlq_record(source_topic: str, reason: str, raw_payload: str) -> Event:
    return {
        "event_id": str(uuid.uuid5(_DLQ_NAMESPACE, f"{source_topic}|{reason}|{raw_payload}")),
        "event_type": "dlq_record",
        "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "schema_version": 1,
        "source_topic": source_topic,
        "rejection_reason": reason,
        "original_payload": raw_payload,
    }


class DlqPublisher:
    def __init__(self, bootstrap_servers: str) -> None:
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            acks="all",
        )

    def publish_all(self, records: list[Event]) -> None:
        for record in records:
            self._producer.send(DLQ_TOPIC, value=record)
        self._producer.flush()


def _dedupe(events: list[Event]) -> list[Event]:
    seen: set[str] = set()
    unique = []
    for event in events:
        if event["event_id"] not in seen:
            seen.add(event["event_id"])
            unique.append(event)
    return unique


def _event_ts(event: Event) -> datetime:
    ts = datetime.fromisoformat(str(event["timestamp"]))
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


class BatchProcessor:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.dlq = DlqPublisher(KAFKA_BOOTSTRAP)

    def _validate(self, topic: str, raw_values: list[str], dlq_out: list[Event]) -> list[Event]:
        event_type = TOPIC_TO_EVENT_TYPE[topic]
        valid: list[Event] = []
        for raw in raw_values:
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                dlq_out.append(make_dlq_record(topic, "PARSE_ERROR", raw))
                continue
            structural = schema_violations(event_type, event)
            if structural:
                dlq_out.append(make_dlq_record(topic, f"SCHEMA_VIOLATION:{structural[0]}", raw))
                continue
            rule_hits = apply_rules(event_type, event)
            if rule_hits:
                dlq_out.append(make_dlq_record(topic, f"RULE_VIOLATION:{rule_hits[0]}", raw))
                continue
            valid.append(event)
        return valid

    def _write_lake(self, event_type: str, events: list[Event]) -> None:
        fields = list(EVENT_FIELDS[event_type])
        rows = []
        for event in events:
            row = []
            for field in fields:
                value = event[field]
                if EVENT_FIELDS[event_type][field] is float:
                    value = float(value)
                row.append(value)
            row.append(str(event["timestamp"])[:10])
            rows.append(row)
        schema = _struct_type(event_type).add("date", StringType(), nullable=False)
        frame = self.spark.createDataFrame(rows, schema)
        frame.write.mode("append").partitionBy("date").parquet(f"{LAKE_PATH}/{event_type}")

    def process(self, batch: DataFrame, batch_id: int) -> None:
        collected = batch.select("topic", F.col("value").cast("string").alias("raw")).collect()
        by_topic: dict[str, list[str]] = {}
        for row in collected:
            by_topic.setdefault(row["topic"], []).append(row["raw"])

        dlq_records: list[Event] = []
        timestamps: list[datetime] = []
        counts: dict[str, int] = {}
        conn = sinks.connect(POSTGRES_DSN)
        try:
            for topic in TOPIC_ORDER:
                raws = by_topic.get(topic, [])
                if not raws:
                    continue
                event_type = TOPIC_TO_EVENT_TYPE[topic]
                events = _dedupe(self._validate(topic, raws, dlq_records))
                if not events:
                    continue
                self._write_lake(event_type, events)
                if event_type == "subscriber_created":
                    sinks.upsert_subscribers(conn, events)
                elif event_type == "tower_status":
                    sinks.update_tower_status(conn, events)
                else:
                    unknown = sinks.insert_facts(conn, event_type, events)
                    dlq_records.extend(
                        make_dlq_record(
                            topic,
                            "REFERENTIAL:subscriber_unknown",
                            json.dumps(event, separators=(",", ":")),
                        )
                        for event in unknown
                    )
                    events = [e for e in events if e not in unknown]
                timestamps.extend(_event_ts(event) for event in events)
                counts[event_type] = len(events)
            if timestamps:
                sinks.refresh_fact_rollups(conn, min(timestamps), max(timestamps))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        if dlq_records:
            self.dlq.publish_all(dlq_records)
        logger.info("batch=%d accepted=%s rejected=%d", batch_id, counts, len(dlq_records))


def process_dlq_batch(batch: DataFrame, batch_id: int) -> None:
    """Persist the failed-transactions topic into dlq_records for review."""
    collected = batch.select(F.col("value").cast("string").alias("raw")).collect()
    records: list[Event] = []
    for row in collected:
        try:
            event = json.loads(row["raw"])
        except json.JSONDecodeError:
            logger.warning("unparseable DLQ payload skipped: %.200s", row["raw"])
            continue
        if schema_violations("dlq_record", event):
            logger.warning("malformed DLQ record skipped: %.200s", row["raw"])
            continue
        records.append(event)
    if not records:
        return
    conn = sinks.connect(POSTGRES_DSN)
    try:
        sinks.insert_dlq(conn, _dedupe(records))
        stamps = [_event_ts(r) for r in records]
        sinks.refresh_dlq_rollups(conn, min(stamps), max(stamps))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("dlq batch=%d persisted=%d", batch_id, len(records))


def main() -> None:
    spark = (
        SparkSession.builder.appName("telestream-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    processor = BatchProcessor(spark)

    events_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(TOPIC_ORDER))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
        .writeStream.foreachBatch(processor.process)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/events")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    dlq_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", DLQ_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
        .writeStream.foreachBatch(process_dlq_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/dlq")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    logger.info("streaming queries started: %s, %s", events_stream.id, dlq_stream.id)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
