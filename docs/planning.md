# TeleStream Delivery Plan

Phased plan for building TeleStream. Each phase ends in a **demoable, committed, CI-green
state** — the repo should look impressive at every stage, not only at the end.

Rule of thumb: don't start a phase until the previous phase's acceptance criteria all pass.

---

## Phase 0 — Foundations (repo, docs, CI skeleton)

Scope:

- Repository scaffolding per the layout in [architecture.md](architecture.md).
- `pyproject.toml` with pinned dev tooling (ruff, mypy, pytest).
- GitHub Actions workflow: lint + format check + unit tests (even if only a placeholder test).
- `.gitignore` covering venvs, checkpoints, Parquet output, GE artifacts.
- MkDocs config building this `docs/` tree.

Acceptance criteria:

- [x] Fresh clone → `pip install -e ".[dev]"` → `pytest` passes.
- [x] CI is green on `main`.
- [x] `mkdocs build --strict` succeeds.

## Phase 1 — Event Generation & Kafka

Scope:

- Docker Compose with Kafka (+ Zookeeper only if the image requires it) and topic
  auto-creation for the 8 topics in [event-schemas.md](event-schemas.md).
- Producer package: shared base generator, subscriber registry, and generators for all
  event domains. Configurable rate and deliberate error injection (`ERROR_RATE`).
- Pandera contracts for every event type, used by generator unit tests.

Acceptance criteria:

- [x] `docker compose up -d` starts Kafka + producer; events visible via console consumer
      on every topic.
- [x] Generators are unit-tested (valid events pass contracts; injected errors fail them).
- [x] Throughput and error rate configurable via env vars.

## Phase 2 — Spark Structured Streaming Core

Scope:

- Spark service in Compose; `spark/streaming_job.py` consuming all topics.
- Pipeline stages: parse → schema validate → business rules → dedupe → sink.
- Business rules as pure, unit-tested functions.
- DLQ path: every rejection published to `failed-transactions` with `rejection_reason`.
- Parquet sink partitioned by event type and date; checkpointing everywhere.

Acceptance criteria:

- [x] Valid events land in Parquet; malformed/rule-violating events land in the DLQ —
      demonstrated by an integration test.
- [x] Restarting the Spark container neither loses nor duplicates events (checkpoint test).
- [x] Business rules covered by unit tests without Spark.

## Phase 3 — Warehouse

Scope:

- `warehouse/schema.sql`: star schema from [data-model.md](data-model.md), applied on
  Postgres init; seed data for `dim_bundle`, `dim_tower`, `dim_date`.
- Spark `foreachBatch` upserts into facts (idempotent on `event_id`) and maintains
  `dim_subscriber` from `subscriber-created`.
- Minute-level rollup tables for dashboard performance.
- DLQ consumer writing `dlq_records`.

Acceptance criteria:

- [x] End-to-end integration test: produce N known events → assert exact warehouse rows.
- [x] Replaying the same events does not create duplicates (idempotency test).
- [x] Fact rows join cleanly to all dimensions (no orphan keys).

## Phase 4 — Dashboards

Scope:

- Grafana in Compose with datasources and dashboards provisioned from `dashboards/` (JSON
  in git, zero click-ops).
- Executive, Network, Sales, and Customer dashboards per the KPI list in
  [architecture.md](architecture.md) §2.7, plus a DLQ review panel.

Acceptance criteria:

- [x] Fresh `docker compose up -d` → Grafana shows live-updating panels within ~2 minutes,
      no manual setup.
- [x] Every KPI panel is backed by a documented query against the star schema/rollups.
- [x] Screenshots captured for the README.

## Phase 5 — Observability

Scope:

- Prometheus + kafka-exporter + postgres-exporter in Compose.
- Custom pipeline metrics (messages processed, failures, batch duration, end-to-end
  latency) exposed from the Spark job.
- Pipeline Ops dashboard in Grafana; alert rules as code (lag growth, DLQ spike, stalled
  pipeline).

Acceptance criteria:

- [x] Kafka consumer lag per topic visible in Grafana.
- [ ] Killing the Spark job triggers visible lag growth and the stalled-pipeline alert.

## Phase 6 — Data Quality & Hardening

Scope:

- Great Expectations suites against the warehouse (nulls, duplicates, ranges,
  referential checks per [architecture.md](architecture.md) §2.4); wired into CI.
- Full CI pipeline: lint → types → unit → docker build → compose up → integration +
  GE checks → teardown.
- Failure-mode testing: broker restart, Postgres outage during streaming.

Acceptance criteria:

- [x] GE suite passes against a freshly populated warehouse and fails when seeded with a
      known-bad row (verified by a test).
- [x] Full CI pipeline green, including the compose-based integration job.

## Phase 7 — Polish & Publication

Scope:

- README with architecture diagram, dashboard screenshots/GIF, quick start, tech stack,
  data model summary, testing and CI sections, future improvements.
- MkDocs site (optionally published via GitHub Pages).
- Repo hygiene: badges (CI, license), LICENSE, tagged `v1.0.0` release.

Acceptance criteria:

- [x] A stranger can go from clone to live dashboards using only the README.
- [x] README claims match reality exactly.

---

## Stretch Phases (post-v1, pick by payoff)

| Stretch | Demonstrates | Builds on |
|---|---|---|
| Schema Registry + Avro | Schema evolution discipline | Phase 1–2 seam |
| Iceberg/Delta lakehouse | Modern table formats | Parquet sink |
| dbt on warehouse | Analytics engineering | Rollup tables |
| Debezium CDC from Postgres | Reverse data flow | Warehouse |
| FastAPI KPI service | Serving layer / APIs | Warehouse |
| ML anomaly detection (tower/fraud) | Applied ML on streams | Lake + warehouse |
| Terraform cloud deploy | IaC | Compose topology |
| High-rate synthetic scaling + OpenTelemetry | Performance & tracing | Producer + Spark |

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Spark + Kafka on one laptop is memory-heavy | Cap executor memory in Compose; keep default event rate modest; document minimum RAM |
| Integration tests flaky in CI (service startup races) | Health-check gating, generous but bounded waits, retry-once policy |
| Scope creep before v1 | Stretch items are locked behind Phase 7 completion |
| Windows/Linux path and line-ending friction | `.gitattributes` with LF for scripts; everything runs inside containers |
