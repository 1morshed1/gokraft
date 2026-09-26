# 02 — Storage: one database per signal, and the writers that feed them

> Packages: `internal/storage/clickhouse`, `internal/storage/timescale`,
> `internal/storage/tracestore`, `internal/writer`; binaries `cmd/logwriter`,
> `cmd/metricwriter`, `cmd/tracewriter`; SQL in `migrations/`.

This is the intellectual heart of gokraft. The central claim: **logs, metrics,
and traces have different shapes and different query patterns, so forcing them
into one database means one of them is always badly served.** We give each its
own store, chosen for its access pattern.

| Signal  | Shape                          | Store       | Chosen because |
|---------|--------------------------------|-------------|----------------|
| Logs    | High-volume, append-only, text | ClickHouse  | Columnar OLAP + compression + bloom-filter text search |
| Metrics | High-volume, numeric series    | TimescaleDB | Hypertables + continuous aggregates (pre-rolled) |
| Traces  | Lower-volume, relational       | PostgreSQL  | Tree reconstruction via joins, JSONB for span attrs |

## Logs → ClickHouse (`migrations/clickhouse.sql`, `storage/clickhouse`)

Logs are the firehose: highest volume, written once, never updated, and queried
by "time range + service + severity + a text needle". That's a textbook OLAP
workload, and ClickHouse is a column store built for it.

Key schema decisions and *why*:

- **Columnar storage** means a query reading 3 columns reads only those 3 off
  disk. Combined with per-column compression, ClickHouse is routinely 10–50×
  faster and 5–10× smaller than a row store / Elasticsearch for logs.
- **`LowCardinality(String)`** on `severity`, `service_name`, and the K8s
  columns dictionary-encodes them (a handful of distinct values → tiny integer
  codes), shrinking them 90%+ and speeding up filters/groupings.
- **`PARTITION BY toYYYYMMDD(timestamp)`** — one partition per day. Deleting old
  data is an instant partition `DROP`, not a slow row-by-row `DELETE`.
- **`ORDER BY (service_name, severity, timestamp)`** encodes the dominant query
  ("ERROR logs from service X in the last hour"), letting ClickHouse skip whole
  granules that can't match.
- **`TTL ... INTERVAL 30 DAY`** expires old logs automatically.
- **`tokenbf_v1` bloom-filter index** on `body` accelerates substring search
  without a full inverted index. It can yield false positives (which ClickHouse
  then verifies exactly) but never false negatives — results stay correct.

The store's most important method is `InsertBatch`, which uses ClickHouse's
**native columnar batch API** (`PrepareBatch` → `Append` → `Send`). Row-at-a-time
`INSERT`s are pathologically slow on a column store; batching is mandatory, not
an optimization.

`Search` builds a dynamic WHERE clause but **always uses bound parameters** —
never string-concatenates user input — so it is injection-safe. Text search uses
`positionCaseInsensitive(body, ?)`, which the bloom index can accelerate.

## Metrics → TimescaleDB (`migrations/timescale.sql`, `storage/timescale`)

Metrics are numeric time-series. You almost never want one raw sample; you want
"average CPU per minute over 6 hours". Two TimescaleDB features fit:

1. **Hypertables** — a Postgres table transparently partitioned by time into
   "chunks". Inserts and time-range queries touch only relevant chunks, so
   performance holds as the table grows into the billions.
2. **Continuous aggregates** — materialized, incrementally-refreshed rollups.
   We pre-compute `metrics_1m` (1-minute buckets) and `metrics_1h` (1-hour),
   each maintained by a background policy.

### The rollup-selection trick (the part to really understand)

A dashboard might ask for the last 5 minutes or the last 30 days. Scanning raw
samples for 30 days would read millions of rows. Instead, `selectTable` routes
by range:

```
range ≤ 30m   → metrics      (raw, full resolution)
range ≤ 24h   → metrics_1m   (pre-aggregated per minute)
otherwise     → metrics_1h   (pre-aggregated per hour)
```

Because each coarser view has ~60× fewer rows than the one below it, the number
of rows a query scans stays roughly constant no matter the time range. That is
how real observability backends keep "zoom out to last quarter" instant. See
`store_test.go` for the pinned heuristic, and note the subtlety flagged in the
SQL: averaging averages is only exact when buckets are equally weighted; at
production scale you'd weight by `sample_count`.

Inserts use Postgres **COPY** (`pgx.CopyFrom`) — the binary bulk-load path,
far faster than multi-row `INSERT` for large batches.

`Scalar` evaluates a single aggregate over a window; the alerting engine (doc
04) uses it to test rule conditions.

## Traces → PostgreSQL (`migrations/postgres.sql`, `storage/tracestore`)

Traces are lower-volume (you sample a fraction of requests) and relational:
"fetch every span with this `trace_id`, then rebuild the parent/child tree".
A well-indexed Postgres table with JSONB for the flexible bits is the pragmatic,
correct choice; you'd only reach for ClickHouse/ScyllaDB at much higher scale.

- **`PRIMARY KEY (trace_id, span_id)`** gives idempotent writes for free.
  Combined with `ON CONFLICT DO NOTHING`, a span redelivered by Kafka after a
  crash becomes a harmless no-op — this is what makes at-least-once delivery
  *safe* rather than duplicate-prone.
- Indexes cover the three real queries: by service (newest first), recent
  overall, and children-of-a-span (for tree rebuild).
- `Search` aggregates per `trace_id` to produce the list view (root service/op,
  total duration, span count, error presence). `GetSpans` returns the flat list
  the API turns into a waterfall tree.

## The writers and at-least-once delivery (`internal/writer`)

All three writers share one loop, written once with generics in
`internal/writer` and specialized only by record type and store:

```
loop:
  batch, rawMsgs := kafka.FetchBatch()
  insert(batch)          // with bounded exponential-backoff retry
  kafka.Commit(rawMsgs)  // ONLY after a successful insert
```

**The order is the whole correctness argument:**

- Insert *then* commit ⇒ **at-least-once**. A crash after insert but before
  commit causes redelivery and a duplicate insert — which every store absorbs
  idempotently. Nothing is lost.
- Commit *then* insert would be **at-most-once** — a crash loses the batch.

For an observability platform, a rare duplicate is far better than lost
telemetry, so we choose at-least-once and pay for it with idempotent writes. If
the database is down, the writer retries the same batch forever (loudly, in the
logs) rather than skipping data — the correct failure mode here.

Run multiple replicas of any writer with the same `KAFKA_GROUP` and Kafka
splits the partitions among them: instant horizontal scaling.

Next: [`03-query-api.md`](03-query-api.md) — turning stored signals into
answers, and correlating across all three.
