# gokraft — a mini observability platform, built from scratch in Go

[![CI](https://github.com/1morshed1/gokraft/actions/workflows/ci.yml/badge.svg)](https://github.com/1morshed1/gokraft/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

gokraft is a teaching-grade clone of the core of Datadog / Grafana Cloud /
Elastic Stack. It ingests **logs, metrics, and traces** over the OpenTelemetry
protocol (OTLP), buffers them through Kafka, stores each signal in a database
chosen for that signal's access pattern, and serves queries and a dashboard on
top. It also runs a small alerting engine.

The goal is **learning**, not production. Every non-obvious decision is written
down in [`docs/`](docs/) so you can understand *why* the system is shaped the
way it is, not just *what* the code does.

## The big picture

```
 instrumented apps ──OTLP/gRPC──▶ Collector ──▶ Kafka ──▶ Writers ──▶ Databases ──▶ Query API ──▶ Dashboard
 (OpenTelemetry SDK)              (enrich,      (logs,    (one per    (ClickHouse,   (Gin, REST     (HTML +
                                   batch)        metrics,  signal)     TimescaleDB,   + WebSocket)   Chart.js)
                                                 traces)               Postgres)
                                                                                          │
                                                                                          ▼
                                                                                   Alerting engine
```


## Why one database per signal?

The single most important idea in gokraft: **logs, metrics, and traces have
completely different shapes and query patterns, so they get different storage.**

| Signal  | Shape                            | Store        | Why |
|---------|----------------------------------|--------------|-----|
| Logs    | High-volume, append-only, text   | ClickHouse   | Columnar + compression + bloom-filter text search |
| Metrics | High-volume, numeric time-series | TimescaleDB  | Hypertables + continuous aggregates (pre-rolled) |
| Traces  | Lower-volume, relational         | PostgreSQL   | Joins/tree reconstruction, JSONB for span attrs |

This is explained in depth in [`docs/learning/02-storage.md`](docs/learning/02-storage.md).

## Repository layout

```
cmd/            one main() per deployable binary
  collector/      OTLP gRPC receiver → Kafka
  logwriter/      Kafka → ClickHouse
  metricwriter/   Kafka → TimescaleDB
  tracewriter/    Kafka → PostgreSQL
  queryapi/       REST + WebSocket query service + alerting
  loadgen/        synthetic telemetry generator for load testing
internal/       all the reusable logic (not importable by other repos)
  model/          normalized signal structs (the system's shared vocabulary)
  config/         12-factor env-var configuration
  logging/        structured slog logger
  collector/      OTLP receiver + OTLP→model conversion
  enrich/         Kubernetes metadata enrichment
  batch/          generic size/time batcher
  kafkabus/       Kafka producer/consumer wrappers
  storage/        one sub-package per database
  query/          query builders and handlers
  alert/          alert rule evaluation engine
migrations/     SQL schemas for each database
web/            the dashboard (static HTML + JS)
deploy/k8s/     Kubernetes manifests
docs/           the learning material — start here
```

## Getting started

Prerequisites: Go 1.24+, Docker + Docker Compose.

```bash
# 1. Start the infrastructure (Kafka, ClickHouse, TimescaleDB, Postgres)
make infra-up

# 2. Apply database schemas
make migrate

# 3. Build everything
make build

# 4. Run the pipeline (in separate terminals, or via compose)
make run-collector
make run-writers
make run-queryapi

# 5. Generate synthetic load
make loadgen

# 6. Open the dashboard
open http://localhost:8080
```

If you just want to explore the code and run the unit tests (no Docker needed
for the pure-logic packages):

```bash
go test ./...
```

## Where to learn

Start with [`docs/learning/00-overview.md`](docs/learning/00-overview.md) and
read the numbered docs in order:

- [00 — Overview](docs/learning/00-overview.md)
- [01 — The Collector](docs/learning/01-collector.md)
- [02 — Storage (one DB per signal)](docs/learning/02-storage.md)
- [03 — The Query API + correlation](docs/learning/03-query-api.md)
- [04 — The Alerting engine](docs/learning/04-alerting.md)
- [05 — Running + load testing](docs/learning/05-run-and-loadtest.md)
- [06 — Debugging journey (four real integration bugs)](docs/learning/06-debugging-journey.md)

Architectural decisions with trade-offs are recorded as ADRs in
[`docs/decisions/`](docs/decisions/).

## Status

Built and **verified end to end**: the docker-compose stack runs, and a load
run populates ClickHouse, TimescaleDB, and PostgreSQL with consistent data while
the query API serves search, metrics, trace waterfalls, live tail, and
cross-signal correlation. All pure-logic packages have unit tests
(`go test ./...`).

## License

[MIT](LICENSE)
