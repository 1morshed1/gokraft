# ADR-0003 — Kafka buffer + at-least-once delivery

**Status:** Accepted

## Context
Ingest rate (bursty, external) rarely matches persist rate (bounded by the
databases). Without a buffer, a slow database forces us to drop telemetry or
block the sending apps.

## Decision
Put Kafka between the collector and the writers, with three topics (one per
signal). Writers commit their Kafka offset **only after** a successful database
write (at-least-once). Every store's write is idempotent so redelivered messages
are harmless (ClickHouse tolerates dup log rows; Postgres traces use
`ON CONFLICT DO NOTHING`).

## Consequences
- **+** Bursts are absorbed; writers drain at their own pace.
- **+** Signals scale and fail independently; writers scale by consumer group.
- **+** Crash-safe: uncommitted messages are redelivered, not lost.
- **−** Duplicates are possible on retry (mitigated by idempotent writes).
- **−** More infrastructure and end-to-end latency (bounded by batch windows).

We choose at-least-once over at-most-once because for observability, losing
telemetry is worse than storing a rare duplicate.
