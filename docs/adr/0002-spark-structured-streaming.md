# ADR-0002: Spark Structured Streaming for Processing

**Status:** Accepted · **Date:** 2026-07-14

## Context

The processing layer must consume multiple Kafka topics, validate and enrich events,
compute windowed aggregates, and write to two sinks (PostgreSQL, Parquet) with
exactly-once-flavoured guarantees on restart.

## Decision

A single PySpark Structured Streaming application with per-topic pipelines, checkpointed
sinks, and `foreachBatch` for idempotent Postgres upserts.

## Alternatives Considered

- **Kafka Streams / Faust:** lighter, but JVM-Kafka-Streams means leaving Python, and
  Faust is effectively unmaintained. Spark is also the stronger portfolio signal for
  data-engineering roles.
- **Apache Flink:** arguably the better pure streaming engine, but PyFlink's ergonomics
  and the local-dev footprint are worse; Spark skills transfer more broadly (batch +
  streaming + the wider Databricks ecosystem).
- **Plain Python consumers:** fine at this volume, but demonstrates none of the
  distributed-processing concepts the project exists to showcase.

## Consequences

- Micro-batch latency (seconds) rather than event-at-a-time — acceptable for
  dashboard-grade "real time".
- Exactly-once to Postgres is achieved via checkpointing + upsert-on-`event_id`
  (effectively idempotent at-least-once), which must be tested explicitly.
- The Spark container is resource-hungry; executor memory is capped in Compose and the
  default event rate is sized for a laptop.
