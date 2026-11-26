# ADR-0002 — One database per signal

**Status:** Accepted

## Context
Logs, metrics, and traces have fundamentally different shapes and query patterns.
Forcing all three into one database means at least one is served badly.

## Decision
Use a purpose-fit store per signal:
- **Logs → ClickHouse** — columnar OLAP, compression, bloom-filter text search.
- **Metrics → TimescaleDB** — hypertables + continuous-aggregate rollups.
- **Traces → PostgreSQL** — relational tree reconstruction, JSONB attributes.

## Consequences
- **+** Each signal gets storage matched to its access pattern (fast queries,
  efficient disk).
- **+** Failures are isolated: a slow ClickHouse doesn't affect metric queries.
- **−** Operational complexity: three databases to run, back up, and monitor.
- **−** Cross-signal queries must be done in the application layer (the
  correlation endpoint), not with a SQL join.

Trade accepted because the query-performance and isolation wins are exactly the
point of the project. At small scale you *could* collapse metrics into ClickHouse
too (noted in the spec) for one fewer database.
