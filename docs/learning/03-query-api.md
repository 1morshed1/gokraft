# 03 — The Query API: turning stored signals into answers

> Packages: `internal/query`; binary `cmd/queryapi`.

The query API is the read side. It is the only process that talks to all three
databases, because its headline feature — **cross-signal correlation** —
inherently spans them. It's built on Gin and exposes REST plus one WebSocket.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/v1/logs/search` | Full-text log search with filters + pagination |
| GET | `/api/v1/logs/tail` | WebSocket live tail |
| GET | `/api/v1/metrics/query` | Time-bucketed metric series (auto rollup) |
| GET | `/api/v1/traces/search` | Trace list (by service/duration/errors) |
| GET | `/api/v1/traces/:trace_id` | One trace as a waterfall tree |
| GET | `/api/v1/correlate/:trace_id` | Trace **and** its logs together |

## Depend on interfaces, not databases

`handlers.go` defines three small interfaces — `LogStore`, `MetricStore`,
`TraceStore` — describing *only* the methods the API uses. The handlers depend on
those, and the concrete ClickHouse/Timescale/Postgres stores satisfy them
implicitly (Go's structural interfaces). Two payoffs:

- The HTTP layer never imports a database driver or writes SQL.
- Handler logic is unit-tested with in-memory fakes — no Docker, no DB. See
  `handlers_test.go`, which drives the correlation endpoint with fake stores and
  asserts both the joined result and the *shape of the query* the handler made.

This is the same dependency-inversion lesson as the collector's `Enricher`,
applied on the read side.

## Trace tree reconstruction (`tree.go`)

The trace store returns a flat list of spans. `BuildTrace` turns it into the
tree a waterfall UI needs. It is pure in-memory logic (hence heavily tested in
`tree_test.go`) and handles two real-world messes:

- **Orphan spans** — a span whose parent never arrived (dropped, in-flight, or
  from a service that didn't export). We *promote* orphans to roots instead of
  hiding them, so a trace can legitimately have several roots. Losing spans
  silently would be worse than showing a slightly ragged tree.
- **Determinism** — children are sorted by start time and a DFS assigns each
  node a `Depth`, so the UI can indent without re-walking the tree.

## Cross-signal correlation — the "killer feature"

`correlate` is why the whole platform is worth more than three separate tools.
Given a `trace_id` it returns the assembled trace tree **and** every log carrying
that same `trace_id`, bounded to the trace's own time window (padded ±1s). From
an error log the UI jumps straight to the full request that produced it, logs and
spans side by side.

This works only because the `trace_id` was preserved end to end: the app puts it
on both its logs and spans, the collector's conversion copies it onto every
`model.Log` and `model.Span`, ClickHouse and Postgres both store it, and here we
join on it. That single shared field, threaded through every layer, is the whole
mechanism. Correlation is also **best-effort**: if the log join fails we still
return the trace (with the error noted) rather than failing the request — a
partial answer beats none during an incident.

## Live tail over WebSocket (`tail.go`)

The dashboard's `tail -f`. The browser opens a socket with a filter; the server
streams matching logs as they arrive.

- **Polling, not streaming.** Every second we ask ClickHouse for logs newer than
  a high-water mark and forward them. Simple, no extra infrastructure, and the
  mechanism is obvious. The documented lower-latency alternative is a dedicated
  Kafka consumer forwarding straight to the socket.
- **No goroutine leaks.** A reader loop exists solely to notice the client
  disappearing; on any read error it cancels the context and the writer stops.
  Forgetting this is the classic WebSocket bug — the server keeps querying for a
  browser tab that closed minutes ago.
- **Correctness at the boundary.** We only send logs strictly newer than the
  high-water mark and advance it as we go, so a log on a poll boundary is never
  sent twice.

## Time-range parsing

`parseTimeRange` accepts either an RFC3339 timestamp or a relative offset like
`-15m`/`-2h` (what a "last 15 minutes" picker sends), defaulting to the last
hour. Small, but it's the ergonomic glue that makes every endpoint pleasant to
call from a dashboard.

## The binary (`cmd/queryapi`)

The composition root of the read side: open the three stores, build the alert
engine (next doc) and run it in a goroutine, register query + alert routes, serve
the static dashboard, and shut down gracefully on SIGTERM. Note the custom JSON
`requestLogger` middleware so access logs match the rest of gokraft's structured
output — fitting for an observability tool to be observable itself.

Next: [`04-alerting.md`](04-alerting.md).
