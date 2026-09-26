# Architecture Decision Records (ADRs)

An ADR captures **one significant decision**: the context that forced a choice,
the options, what we picked, and the consequences we accepted. They are short on
purpose. Read them to understand *why* gokraft is shaped the way it is; the
numbered learning docs in `../learning/` explain *how* each part works.

- [ADR-0001](0001-normalized-model-at-the-boundary.md) — Convert OTLP to a normalized model at the boundary
- [ADR-0002](0002-one-database-per-signal.md) — One database per signal
- [ADR-0003](0003-kafka-buffer-and-at-least-once.md) — Kafka buffer + at-least-once delivery
- [ADR-0004](0004-structured-alert-rules.md) — Structured alert rules instead of a query DSL
- [ADR-0005](0005-json-on-kafka.md) — JSON (not protobuf) on the Kafka wire
