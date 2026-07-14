# ADR-0003: Dual Storage — PostgreSQL Warehouse + Parquet Data Lake

**Status:** Accepted · **Date:** 2026-07-14

## Context

The platform needs both (a) a fast, queryable serving layer for Grafana dashboards and
analytical SQL, and (b) a cheap, durable, replayable archive of raw validated events for
reprocessing and future batch analytics.

## Decision

Write every validated event to two sinks:

- **PostgreSQL** — star-schema warehouse (facts, dimensions, minute-level rollups) as the
  serving layer for dashboards and ad-hoc SQL.
- **Parquet files** — append-only lake partitioned by `event_type/date`, as the raw
  archive and replay source.

## Alternatives Considered

- **Postgres only:** no replay story, raw history bloats the serving DB, and the project
  loses the warehouse-vs-lake distinction that real platforms have.
- **Lake only (query via Spark/DuckDB):** Grafana wants a low-latency SQL endpoint;
  ad-hoc Spark queries are the wrong tool for dashboard refreshes.
- **A real OLAP store (ClickHouse, Druid):** stronger at time-series analytics but adds a
  service, and Postgres star-schema modeling is the more universally expected skill.

## Consequences

- Two sinks means two checkpointed write paths; idempotency is enforced independently
  (upsert-on-`event_id` for Postgres; append with dedupe-on-read for the lake).
- The lake is the natural seam for the Iceberg/Delta lakehouse stretch goal, and the
  warehouse for the dbt stretch goal.
- Storage duplication is deliberate and documented — it mirrors the serving/archive
  split of production platforms.
