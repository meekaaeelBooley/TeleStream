# TeleStream Documentation

TeleStream is an enterprise-inspired streaming data platform that simulates a telecom
operator's real-time subscriber events — ingested through Kafka, processed with Spark
Structured Streaming, quality-checked, stored in a PostgreSQL star-schema warehouse and
a Parquet data lake, and visualized in Grafana with Prometheus observability.

## Contents

- [Architecture](architecture.md) — system design, components, data flow, error handling
- [Delivery Plan](planning.md) — phased roadmap with acceptance criteria
- [Event Schemas](event-schemas.md) — canonical event contracts and Kafka topic map
- [Data Model](data-model.md) — warehouse star schema and KPI mapping
- Architecture Decision Records:
    - [ADR-0001: Kafka as the event backbone](adr/0001-kafka-as-event-backbone.md)
    - [ADR-0002: Spark Structured Streaming](adr/0002-spark-structured-streaming.md)
    - [ADR-0003: Dual storage — Postgres + Parquet](adr/0003-dual-storage-postgres-parquet.md)
    - [ADR-0004: Docker Compose deployment](adr/0004-docker-compose-single-machine.md)

## Reading Order for New Contributors

1. [Architecture](architecture.md) for the big picture.
2. [Event Schemas](event-schemas.md) and [Data Model](data-model.md) — the contracts all
   code must honour.
3. [Delivery Plan](planning.md) to see what's built and what's next.
