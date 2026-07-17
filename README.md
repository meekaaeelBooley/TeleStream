# TeleStream

[![CI](https://github.com/meekaaeelBooley/TeleStream/actions/workflows/ci.yml/badge.svg)](https://github.com/meekaaeelBooley/TeleStream/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)

> An enterprise-inspired streaming data platform that simulates a telecom operator's
> real-time subscriber events — ingested through **Apache Kafka**, processed with
> **Spark Structured Streaming**, quality-checked with **Pandera** and
> **Great Expectations**, stored in a **PostgreSQL** star-schema warehouse and a
> **Parquet** data lake, and visualized through **Grafana** with **Prometheus**
> observability. The whole stack runs with one `docker compose up`.

**Status: v1 complete.** Event generation → Kafka → Spark validation/rules/dedup →
Postgres star schema + Parquet lake → five provisioned Grafana dashboards, with
Prometheus scraping Kafka lag and warehouse health. Verified by 55 unit tests and 18
integration tests against the live stack — including Great Expectations warehouse
suites and failure-mode tests (broker restart, Spark restart, warehouse outage).
What's next: [stretch goals](docs/planning.md).

## Why This Project

Telecom networks are one of the highest-volume event environments there is: every second
brings SIM activations, airtime recharges, bundle purchases, voice calls, SMS, data
sessions, and tower telemetry. TeleStream recreates that flow end-to-end at laptop scale —
with the same architectural decisions (keyed topics, dead letter queues, idempotent
sinks, star-schema modeling, lag-based alerting) that matter at production scale.

## Architecture

```mermaid
flowchart LR
    GEN[Event Generators] --> KAFKA[(Kafka)]
    KAFKA --> SPARK[Spark Structured Streaming<br/>validate · rules · dedupe]
    SPARK -->|invalid| DLQ[(Dead Letter Queue)]
    SPARK -->|upsert| PG[(PostgreSQL<br/>Star Schema)]
    SPARK -->|append| LAKE[(Parquet Lake)]
    PG --> GRAF[Grafana Dashboards]
    DLQ --> GRAF
    SPARK -.-> PROM[(Prometheus)] -.-> GRAF
```

Deep dive: [docs/architecture.md](docs/architecture.md)

## Key Design Features

- **Contract-first events** — every event carries `event_id`, `timestamp`, and
  `schema_version`; contracts are documented and enforced in-stream
  ([event schemas](docs/event-schemas.md)).
- **Nothing dropped silently** — parse errors, schema violations, and business-rule
  failures all route to a dead letter queue with machine-readable reasons, surfaced on a
  review dashboard.
- **Idempotent, restart-safe pipeline** — Spark checkpointing plus upsert-on-`event_id`
  means replays and restarts don't duplicate warehouse rows.
- **Warehouse + lake split** — Postgres star schema for serving, partitioned Parquet for
  archive/replay ([data model](docs/data-model.md)).
- **Everything as code** — Kafka topics, DB schema, Grafana dashboards, Prometheus
  configs, and alert rules are all provisioned from files in this repo. Zero click-ops.

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka |
| Processing | Spark Structured Streaming (PySpark) |
| Storage | PostgreSQL (warehouse) · Parquet (lake) |
| Data quality | Pandera (in-stream) · Great Expectations (at-rest) |
| Visualization | Grafana |
| Monitoring | Prometheus |
| Infrastructure | Docker Compose |
| CI/CD | GitHub Actions |
| Testing | pytest (unit + compose-based integration) |
| Docs | MkDocs |

## Documentation

| Doc | What's in it |
|---|---|
| [Architecture](docs/architecture.md) | Components, data flow, error handling, observability |
| [Delivery Plan](docs/planning.md) | Phases 0–7 with acceptance criteria, stretch goals, risks |
| [Event Schemas](docs/event-schemas.md) | Canonical contracts + Kafka topic map |
| [Data Model](docs/data-model.md) | Star schema, rollups, KPI mapping |
| [ADRs](docs/adr/) | Why Kafka, why Spark, why dual storage, why Compose |

## Quick Start

Requires Docker Desktop (or any Docker Engine with Compose v2) and ~4 GB free RAM.

```bash
git clone https://github.com/meekaaeelBooley/TeleStream.git
cd TeleStream
docker compose up -d --build
```

Within ~2 minutes:

- **Grafana** — http://localhost:3000 (admin / admin) → five live dashboards under the
  *TeleStream* folder
- **Prometheus** — http://localhost:9090 (Kafka lag, warehouse, pipeline targets)
- **Warehouse** — `postgresql://telestream:telestream@localhost:5432/telestream`
- **Kafka** — `localhost:29092` from the host

Tune the simulation with env vars: `EVENTS_PER_SECOND` (default 50), `ERROR_RATE`
(default 0.02 — fraction of deliberately corrupted events exercising the DLQ).

### Running the tests

```bash
python -m venv .venv && . .venv/Scripts/activate   # Windows; use bin/activate on Linux
pip install -e ".[dev]"
pytest tests/unit                    # fast, no services needed
pytest tests/integration             # requires the compose stack to be up
ruff check . && mypy                 # lint + strict typing
```

## Dashboards

Five Grafana dashboards are provisioned automatically (JSON generated from
[dashboards/generate_dashboards.py](dashboards/generate_dashboards.py) — dashboards are code here):

| Dashboard | Audience | Highlights |
|---|---|---|
| Executive Overview | Leadership | Subscribers, revenue/min, calls/min, rejected events |
| Network Operations | NOC | Live tower status, tower load, dropped calls, traffic by technology |
| Sales & Revenue | Commercial | Airtime vs bundle revenue, top products, revenue by province, failed payments |
| Customer Insights | CVM | Subscribers by province/plan, ARPU, top data users |
| Pipeline Operations | Data platform | DLQ breakdown by reason, latest dead letters, Kafka consumer lag |

**Executive Overview** — live subscriber, revenue, call, and rejection KPIs:

![Executive dashboard](docs/img/dashboard-executive.png)

**Network Operations** — tower health, load, dropped calls, traffic by technology:

![Network dashboard](docs/img/dashboard-network.png)

More: [Sales & Revenue](docs/img/dashboard-sales.png) ·
[Customer Insights](docs/img/dashboard-customer.png) ·
[Pipeline Operations](docs/img/dashboard-pipeline.png)

(Screenshots are reproducible: `docker compose -f docker-compose.yml -f docker-compose.render.yml up -d`
then `bash scripts/capture_dashboards.sh`.)

## Data Quality — Two Layers

- **In-stream (per event):** pandera contracts + a pure-Python business rules engine inside
  the Spark job. Violations are never dropped — they go to the `failed-transactions` DLQ
  with machine-readable reasons (`PARSE_ERROR`, `SCHEMA_VIOLATION:<field>`,
  `RULE_VIOLATION:<rule>`, `REFERENTIAL:<check>`, `PAYMENT_DECLINED`).
- **At-rest (warehouse):** Great Expectations suites ([quality/](quality/)) assert on what
  actually landed — uniqueness of event ids, value ranges, referential integrity, timestamp
  sanity. The integration tests prove the suites pass on real pipeline output *and* catch a
  deliberately planted bad row.

## Testing & CI

- **55 unit tests** — generators, contracts, business rules, schema-drift guards
  (producer contracts vs Spark schemas vs generated artifacts). No Docker needed.
- **Integration tests** — against the real compose stack: produce → Kafka → Spark →
  Postgres assertions, DLQ contents, rollups, no orphans/duplicates, Great Expectations
  suites, and failure-mode tests (broker restart, Spark restart, warehouse outage —
  verifying checkpoint recovery and idempotent sinks).
- **GitHub Actions** — lint (ruff) → types (mypy --strict) → unit tests → docs build,
  then a second job that builds the full Docker stack on the runner and runs the
  integration suite against it.

## Roadmap

Phases 0–7: foundations → producers + Kafka → Spark core → warehouse → dashboards →
observability → data quality & hardening → polish. Stretch goals include Schema
Registry/Avro, an Iceberg or Delta lakehouse, dbt, Debezium CDC, a FastAPI KPI service,
and ML anomaly detection. Details and acceptance criteria: [docs/planning.md](docs/planning.md).

## Data Disclaimer

All data is synthetic. MSISDNs, subscribers, transactions, and tower telemetry are
generated; no real personal or network data is used anywhere.
