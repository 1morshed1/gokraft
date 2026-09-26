# ADR-0001 — Convert OTLP to a normalized model at the boundary

**Status:** Accepted

## Context
Applications send telemetry as OTLP protobuf — a deeply nested format optimized
for wire transport (resource → scope → record), not for downstream code. We have
to accept OTLP, but the writers, query layer, and tests all need to manipulate
telemetry too.

## Decision
Convert OTLP into a flat, purpose-built set of Go structs (`internal/model`)
exactly once, at the collector's edge. Everything after the collector — Kafka,
writers, queries, tests — speaks only `model.Log`/`Metric`/`Span`.

## Consequences
- **+** The whole system is decoupled from the wire format. If OTLP changes, only
  the collector's conversion code changes.
- **+** Constructing test data is trivial (building valid OTLP by hand is not).
- **+** Storage rows map almost 1:1 to model structs, so writers stay simple.
- **−** A conversion step exists and must be maintained; some OTLP richness
  (typed attributes, histogram buckets) is flattened/simplified. Documented as
  extension points.

This is the single most important structural decision in the codebase:
*translate foreign formats to your own model at the boundary.*
