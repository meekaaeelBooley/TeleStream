# ADR-0004: Docker Compose as the Deployment Target

**Status:** Accepted · **Date:** 2026-07-14

## Context

The project's core promise is that anyone — a recruiter, a reviewer, the author on a new
machine — can go from `git clone` to live dashboards with one command. The stack has 8+
services (Kafka, Spark, Postgres, Grafana, Prometheus, exporters, producer).

## Decision

Docker Compose is the only supported deployment for v1. All provisioning (topics, DB
schema, Grafana dashboards/datasources, Prometheus scrape configs) is file-based and
applied automatically at startup. Health checks gate startup ordering — no sleep-based
orchestration.

## Alternatives Considered

- **Kubernetes (kind/minikube):** more "production", but triples setup friction and
  debugging surface for zero analytical payoff; a recruiter will not `kubectl` their way
  into a demo. Terraform/cloud deploy is an explicit stretch goal instead.
- **Local installs (no containers):** unreproducible across machines and OSes;
  Kafka/Spark version drift would dominate the work.

## Consequences

- Resource budgeting matters: services get memory limits and the default event rate is
  laptop-sized. Minimum host RAM is documented in the README.
- CI reuses the same Compose file (with a CI profile) for integration tests, so the demo
  environment and the test environment cannot drift apart.
- Anything that cannot be provisioned from a file in the repo does not ship.
