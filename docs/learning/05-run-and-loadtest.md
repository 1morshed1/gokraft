# 05 — Running gokraft, and load-testing it

> Files: `docker-compose.yml`, `Dockerfile`, `Makefile`, `web/`,
> `cmd/loadgen`, `deploy/k8s/`.

Now we run the whole thing and push traffic through it.

## The fastest path: docker-compose

```bash
make up                       # build images + start infra and all services
# wait ~30s for databases to become healthy and migrations to apply
make loadgen                  # push synthetic OTLP traffic through the collector
open http://localhost:8080    # the dashboard
make logs                     # follow the gokraft services' logs
make clean                    # tear everything down and wipe volumes
```

`docker-compose.yml` starts four infrastructure containers (Kafka in KRaft mode,
ClickHouse, TimescaleDB, Postgres) and the six gokraft binaries. Migrations are
applied automatically: ClickHouse and Postgres both execute `*.sql` mounted into
their `docker-entrypoint-initdb.d`. Health checks and `depends_on` ordering mean
writers don't start hammering a database that isn't ready.

## Running pieces locally

For a tight edit-run loop you often want just the infra in Docker and the Go
process on your host:

```bash
make infra-up          # only Kafka + the three databases
make run-collector     # in one terminal
make run-logwriter     # in another (repeat for metric/tracewriter)
make run-queryapi      # serves the dashboard on :8080
make run-loadgen       # generate traffic
```

Every binary is configured entirely by environment variables (see
`internal/config`), so overriding a port or DSN is a one-liner.

## The load generator (`cmd/loadgen`)

`loadgen` is not a mock — it is a **real OTLP/gRPC client** built on the same
generated stubs a genuine OpenTelemetry SDK uses. If loadgen's data reaches the
dashboard, any real instrumented app will too. Each simulated service emits, per
request:

- a **trace** (a root span + a child `db.query` span),
- two **logs** carrying that trace's `trace_id` (so correlation has something to
  join),
- two **metrics** (`http_request_duration_ms`, `http_error_rate`).

About 4% of requests are errors (slower, `ERROR` logs, error-status spans), which
drives the seeded error-rate alert into FIRING — so you can watch the whole
alerting state machine work against live data.

Tune it with env vars: `SERVICES` (up to 10), `RATE` (requests/sec per service),
`DURATION` (`0` = run until stopped).

## The dashboard (`web/`)

Deliberately plain — vanilla JS + Chart.js, one HTML file, no build step. It
exercises every read path:

- **Logs** — full-text search with service/severity/time filters; each log with a
  `trace_id` has a `[trace]` link that jumps to correlation.
- **Metrics** — line chart; change the range and watch it silently switch between
  raw data and the 1m/1h rollups (the `selectTable` heuristic from doc 02).
- **Traces** — search + a **waterfall** view, and beneath it the **correlated
  logs** for that trace (the killer feature from doc 03), fetched from
  `/correlate/:trace_id`.
- **Live Tail** — a WebSocket stream of new logs matching a filter.
- **Alerts** — the seeded rules with their live `ok`/`pending`/`firing` state.

## Load-testing targets (from the plan)

The plan sets these goals; here's how to measure them with the tools in the repo:

| Target | How to measure |
|--------|----------------|
| 50,000+ logs/sec sustained | Raise `RATE`/`SERVICES`, scale `logwriter` replicas, watch Kafka consumer lag not grow unbounded |
| Full-text search < 2s over 1B rows | Backfill ClickHouse, then time `/logs/search?q=…`; the `tokenbf_v1` index is what keeps it fast |
| End-to-end latency < 3s (emit → visible) | Compare a log's `timestamp` to when it appears in live tail; the batcher's `BATCH_DELAY` (500ms) is the main tunable |
| Storage efficiency (GB per 1B logs) | `SELECT formatReadableSize(sum(bytes)) FROM system.parts WHERE table='logs'` — observe ClickHouse's columnar compression |

Where the numbers come from in the design:

- **Ingestion throughput** is bounded by the collector's batching (doc 01) and
  the number of writer replicas draining Kafka (doc 02). Both scale horizontally.
- **Query latency** comes from the storage choices: ClickHouse's columnar layout
  + bloom index for logs, and TimescaleDB's continuous-aggregate rollups for
  metrics (doc 02).
- **End-to-end latency** is dominated by the batching delays (collector batch
  window + writer batch window). Shrink them to trade throughput for freshness.

## Kubernetes (`deploy/k8s/`)

The manifests deploy the stateless gokraft services and show the two scaling
stories: the **collector autoscales on CPU** (HPA) because ingestion is the
bursty part, and **writers scale by Kafka consumer group** (add replicas, Kafka
rebalances partitions). The collector wires the **Downward API** into the
`StaticEnricher`, so K8s enrichment is real, not theoretical. See
`deploy/k8s/README.md`.

That's the whole platform. Read the ADRs in [`../decisions/`](../decisions/) for
the reasoning behind the big trade-offs, condensed.
