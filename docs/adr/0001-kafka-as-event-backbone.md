# ADR-0001: Apache Kafka as the Event Backbone

**Status:** Accepted · **Date:** 2026-07-14

## Context

TeleStream needs a durable, replayable, high-throughput transport between event
producers and the stream processor, with per-entity ordering (all events for one
subscriber processed in order) and support for multiple independent consumers
(stream processor now, DLQ consumer, potential future consumers).

## Decision

Use Apache Kafka with one topic per event domain, keyed by subscriber/tower ID.
Prefer KRaft mode to avoid running Zookeeper.

## Alternatives Considered

- **RabbitMQ / classic queues:** great for task distribution, but no log semantics —
  no replay, weaker fit for stream analytics and consumer lag monitoring.
- **Redpanda:** Kafka-compatible and lighter, tempting for laptop use — but plain Kafka
  is the industry-standard line item this portfolio should demonstrate; Redpanda remains
  a drop-in swap if resources demand it.
- **Direct producer → Spark socket/files:** removes the most important architectural
  component and all durability/replay properties.

## Consequences

- Consumer lag becomes the primary pipeline-health metric (exported to Prometheus).
- Keyed partitioning gives per-subscriber ordering within a partition; cross-partition
  ordering is explicitly not guaranteed and downstream logic must not assume it.
- Kafka is the heaviest Compose service; memory limits must be tuned for laptop use.
