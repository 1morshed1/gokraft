## Project 5: Distributed Log Aggregation & Observability Platform (Mini-Datadog)

### Concept & Why It Matters

You are building a simplified observability platform that ingests logs, metrics, and traces from dozens of services, stores them efficiently, and lets users query and visualize them through a dashboard. Think of it as a miniature Datadog, Grafana Cloud, or Elastic Stack built from scratch.

This project matters because observability is no longer optional — it is a core infrastructure concern at every serious company. Building the platform itself (rather than just using Prometheus/Grafana) proves you understand what happens beneath the tools. You will deal with extreme write-heavy workloads, columnar storage, time-series compression, and query optimization — problems that most engineers never touch directly.

It also fills a gap in the other projects: they all *use* observability tools, but this one *is* the observability tool.

### Architecture Diagram

```
  Application Services                                                        
  (instrumented with OpenTelemetry SDK)                                        
       │                                                                       
       │  OTLP/gRPC (logs, metrics, traces)                                   
       ▼                                                                       
┌──────────────────────────────────────────────────────────────────────────────┐
│                        KUBERNETES CLUSTER                                    │
│                                                                              │
│  ┌─────────────────────────────────────┐                                     │
│  │  Go Collector Service               │                                    │
│  │  (OpenTelemetry Collector clone)    │                                    │
│  │                                     │                                    │
│  │  - Receives OTLP/gRPC              │                                    │
│  │  - Validates & enriches            │                                    │
│  │    (adds K8s metadata: pod,        │                                    │
│  │     namespace, node)               │                                    │
│  │  - Routes by signal type           │                                    │
│  │  - Batches for downstream          │                                    │
│  │  - HPA scaled (CPU-based)          │                                    │
│  └──────────┬──────────────────────────┘                                     │
│             │                                                                │
│     ┌───────┴────────┬─────────────────┐                                     │
│     ▼                ▼                 ▼                                     │
│  ┌────────┐    ┌──────────┐    ┌────────────┐                                │
│  │ Kafka  │    │  Kafka   │    │   Kafka    │                                │
│  │ topic: │    │  topic:  │    │   topic:   │                                │
│  │ logs   │    │ metrics  │    │   traces   │                                │
│  └───┬────┘    └────┬─────┘    └─────┬──────┘                                │
│      │              │                │                                       │
│      ▼              ▼                ▼                                       │
│  ┌────────┐    ┌──────────┐    ┌────────────┐                                │
│  │ Log    │    │ Metrics  │    │  Trace     │                                │
│  │ Writer │    │ Writer   │    │  Writer    │                                │
│  │ (Go)   │    │ (Go)     │    │  (Go)      │                                │
│  └───┬────┘    └────┬─────┘    └─────┬──────┘                                │
│      │              │                │                                       │
│      ▼              ▼                ▼                                       │
│  ┌────────┐    ┌──────────┐    ┌────────────┐                                │
│  │Click-  │    │Timescale │    │ PostgreSQL │                                │
│  │House   │    │DB        │    │ (trace     │                                │
│  │(logs)  │    │(metrics) │    │  spans +   │                                │
│  │        │    │          │    │  indexes)  │                                │
│  └───┬────┘    └────┬─────┘    └─────┬──────┘                                │
│      │              │                │                                       │
│      └──────────────┴────────────────┘                                       │
│                     │                                                        │
│                     ▼                                                        │
│  ┌──────────────────────────────────────────────────┐                        │
│  │  Go Query API                                    │                       │
│  │                                                  │                       │
│  │  GET /api/v1/logs/search?q=...&from=...&to=...  │                       │
│  │  GET /api/v1/metrics/query?metric=...&agg=...   │                       │
│  │  GET /api/v1/traces/:trace_id                   │                       │
│  │  WS  /api/v1/logs/tail  (live tail)             │                       │
│  └──────────────────────────────────────────────────┘                        │
│                     │                                                        │
│                     ▼                                                        │
│  ┌──────────────────────────────────────────────────┐                        │
│  │  Dashboard (React or plain HTML)                 │                       │
│  │                                                  │                       │
│  │  - Log explorer with full-text search            │                       │
│  │  - Metrics charts (line, bar, heatmap)           │                       │
│  │  - Trace waterfall (Jaeger-style)                │                       │
│  │  - Live log tail with filters                    │                       │
│  │  - Alert rule configuration                      │                       │
│  └──────────────────────────────────────────────────┘                        │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Tech Stack Breakdown

| Layer | Technology | Why This Choice |
|---|---|---|
| Signal Ingestion | Go + OTLP/gRPC | OpenTelemetry's OTLP is the emerging standard for telemetry transport. Accepting OTLP means any app instrumented with the OpenTelemetry SDK can send data to your platform with zero custom integration. |
| Buffering | Apache Kafka (3 topics) | Decouples ingestion from storage. The collector can accept data at 100K events/sec even if the database writers are temporarily slow. Separate topics per signal type allow independent scaling. |
| Log Storage | ClickHouse | Column-oriented database designed for analytical queries over massive datasets. Logs are append-only and queried by time range + text search — exactly what ClickHouse excels at. 10-50x faster than Elasticsearch for log queries at scale, and uses 5-10x less disk due to columnar compression. |
| Metrics Storage | TimescaleDB | Same reasoning as Project 3. Time-series metrics with continuous aggregates for pre-computed rollups. Alternatively, you could use ClickHouse for metrics too (simpler stack, one fewer database to manage). |
| Trace Storage | PostgreSQL | Traces are relatively low-volume compared to logs and metrics. A well-indexed Postgres table with JSONB for span attributes is sufficient. For higher scale, you would move to ClickHouse or ScyllaDB. |
| Query API | Go + Gin | Translates user queries into optimized SQL for each backend. Handles cross-signal correlation (e.g., "show me the trace for this error log"). |
| Dashboard | React or plain HTML + Chart.js | Keep it simple. The backend is the showcase, not the frontend. |

### Phase-by-Phase Build Plan

#### Phase 1: The Collector (Go OTLP Receiver)

The collector is a gRPC server that implements the OpenTelemetry Collector's receiver interface. It accepts three signal types over a single gRPC connection.

**Protobuf (use the official OpenTelemetry proto definitions):**

```protobuf
// You do not need to write these — import from
// opentelemetry-proto/opentelemetry/proto/collector/
service LogsService {
    rpc Export(ExportLogsServiceRequest) returns (ExportLogsServiceResponse);
}
service MetricsService {
    rpc Export(ExportMetricsServiceRequest) returns (ExportMetricsServiceResponse);
}
service TraceService {
    rpc Export(ExportTraceServiceRequest) returns (ExportTraceServiceResponse);
}
```

**Enrichment Pipeline:**

Before forwarding to Kafka, the collector enriches each signal with Kubernetes metadata. When running inside K8s, it can query the Kubernetes API to resolve the sending pod's name, namespace, node, and labels. This is done via the Kubernetes Downward API (injected as environment variables) or by querying the K8s API server with the pod's IP.

```go
type Enricher struct {
    kubeClient kubernetes.Interface
    podCache   *cache.Expiring  // cache pod metadata for 60s
}

func (e *Enricher) Enrich(signal Signal, sourceIP string) Signal {
    pod, found := e.podCache.Get(sourceIP)
    if !found {
        pod = e.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
            FieldSelector: "status.podIP=" + sourceIP,
        })
        e.podCache.Set(sourceIP, pod, 60*time.Second)
    }
    signal.Attributes["k8s.pod.name"] = pod.Name
    signal.Attributes["k8s.namespace"] = pod.Namespace
    signal.Attributes["k8s.node.name"] = pod.Spec.NodeName
    return signal
}
```

**Batching:**

Do not produce one Kafka message per log line. Buffer incoming signals and flush to Kafka in batches of 1,000 records or every 500ms, whichever comes first. This reduces Kafka overhead by orders of magnitude.

#### Phase 2: Storage Layer — One Database Per Signal

**ClickHouse Schema for Logs:**

```sql
CREATE TABLE logs (
    timestamp     DateTime64(3, 'UTC'),
    trace_id      String,
    span_id       String,
    severity      LowCardinality(String),    -- INFO, WARN, ERROR, FATAL
    service_name  LowCardinality(String),
    body          String,                     -- the actual log message
    attributes    Map(String, String),        -- arbitrary key-value pairs
    k8s_pod       LowCardinality(String),
    k8s_namespace LowCardinality(String),
    k8s_node      LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, severity, timestamp)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Full-text index for log body search
ALTER TABLE logs ADD INDEX idx_body body TYPE tokenbf_v1(30720, 2, 0) GRANULARITY 1;
```

Why these choices:
- `LowCardinality(String)` for fields with few distinct values (severity, service name). ClickHouse uses dictionary encoding, reducing storage by 90%+ for these columns.
- `PARTITION BY toYYYYMMDD` — one partition per day. Dropping old data is instant (drop the partition, do not delete row by row).
- `ORDER BY (service_name, severity, timestamp)` — optimizes the most common query pattern: "show me ERROR logs from service X in the last hour."
- `TTL` — automatic data expiration. Logs older than 30 days are deleted automatically.
- `tokenbf_v1` — a token bloom filter index that accelerates full-text search on the log body without the overhead of a full inverted index.

**TimescaleDB Schema for Metrics:**

```sql
CREATE TABLE metrics (
    time            TIMESTAMPTZ NOT NULL,
    metric_name     TEXT NOT NULL,
    service_name    TEXT NOT NULL,
    tags            JSONB,
    value           DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('metrics', 'time');

CREATE INDEX idx_metrics_name_time ON metrics (metric_name, time DESC);
CREATE INDEX idx_metrics_service ON metrics (service_name, time DESC);

-- Pre-compute 1-minute rollups
CREATE MATERIALIZED VIEW metrics_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    metric_name,
    service_name,
    AVG(value) AS avg_val,
    MAX(value) AS max_val,
    MIN(value) AS min_val,
    COUNT(*) AS sample_count
FROM metrics
GROUP BY bucket, metric_name, service_name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_1m',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute'
);

-- 1-hour rollups for longer time ranges
CREATE MATERIALIZED VIEW metrics_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    metric_name,
    service_name,
    AVG(avg_val) AS avg_val,
    MAX(max_val) AS max_val,
    MIN(min_val) AS min_val,
    SUM(sample_count) AS sample_count
FROM metrics_1m
GROUP BY time_bucket('1 hour', bucket), metric_name, service_name
WITH NO DATA;
```

**Query Resolution Strategy:**

When a user queries metrics for the last 5 minutes, read from the raw `metrics` table (full resolution). For the last 24 hours, read from `metrics_1m`. For the last 7 days, read from `metrics_1h`. This keeps dashboard queries fast regardless of time range:

```go
func (q *MetricsQuerier) selectTable(timeRange time.Duration) string {
    switch {
    case timeRange <= 30*time.Minute:
        return "metrics"
    case timeRange <= 24*time.Hour:
        return "metrics_1m"
    default:
        return "metrics_1h"
    }
}
```

**PostgreSQL Schema for Traces:**

```sql
CREATE TABLE trace_spans (
    trace_id        TEXT NOT NULL,
    span_id         TEXT NOT NULL,
    parent_span_id  TEXT,
    service_name    TEXT NOT NULL,
    operation_name  TEXT NOT NULL,
    start_time      TIMESTAMPTZ NOT NULL,
    duration_us     BIGINT NOT NULL,          -- microseconds
    status_code     SMALLINT NOT NULL,        -- 0=unset, 1=ok, 2=error
    attributes      JSONB,
    events          JSONB,                    -- span events (logs attached to span)
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX idx_spans_service ON trace_spans (service_name, start_time DESC);
CREATE INDEX idx_spans_time ON trace_spans (start_time DESC);
CREATE INDEX idx_spans_parent ON trace_spans (trace_id, parent_span_id);
```

To reconstruct a full trace waterfall, query all spans with the same `trace_id`, sort by `start_time`, and build the parent-child tree using `parent_span_id`.

#### Phase 3: The Query API

**Endpoints:**

- `GET /api/v1/logs/search` — Full-text search with filters. Parameters: `q` (search text), `service` (filter by service), `severity` (filter by level), `from`/`to` (time range), `limit`, `offset`. Translates to a ClickHouse query.
- `GET /api/v1/logs/tail` (WebSocket) — Live tail. The server continuously polls ClickHouse for new logs matching the filter and pushes them to the client. Alternatively, consume directly from the Kafka `logs` topic for true real-time.
- `GET /api/v1/metrics/query` — Metric queries. Parameters: `metric` (metric name), `service`, `from`/`to`, `aggregation` (avg, max, min, sum, p99), `group_by`, `interval` (resolution). Returns time-series data formatted for charting.
- `GET /api/v1/traces/:trace_id` — Returns all spans for a trace, structured as a tree for waterfall rendering.
- `GET /api/v1/traces/search` — Search traces by service, operation, duration, or status. Returns a list of trace summaries.

**Cross-Signal Correlation:**

The killer feature. When a user clicks on an error log, the dashboard can show the full trace that generated it (using the `trace_id` field embedded in the log). When viewing a trace, clicking on a span shows all logs emitted during that span's execution. This is what makes Datadog worth $40B — connecting logs, metrics, and traces into a single coherent picture.

```go
func (h *Handler) GetCorrelatedData(c *gin.Context) {
    traceID := c.Param("trace_id")

    // Fetch trace spans
    spans := h.traceStore.GetSpans(ctx, traceID)

    // Fetch logs with matching trace_id
    logs := h.logStore.Search(ctx, LogQuery{
        TraceID: traceID,
        From:    spans.EarliestStart().Add(-1 * time.Second),
        To:      spans.LatestEnd().Add(1 * time.Second),
    })

    // Fetch metrics for the services involved
    services := spans.UniqueServices()
    metrics := h.metricsStore.Query(ctx, MetricQuery{
        Services: services,
        From:     spans.EarliestStart().Add(-5 * time.Minute),
        To:       spans.LatestEnd().Add(5 * time.Minute),
        Metrics:  []string{"http_request_duration_seconds", "error_rate"},
    })

    c.JSON(200, CorrelatedView{
        Trace:   spans,
        Logs:    logs,
        Metrics: metrics,
    })
}
```

#### Phase 4: Alerting Engine

Build a simple alert evaluation loop:

```go
type AlertRule struct {
    ID          string
    Name        string
    MetricQuery string        // e.g., "avg(http_error_rate{service='api'}) > 0.05"
    Duration    time.Duration // condition must be true for this long
    Severity    string        // warning, critical
    NotifyURL   string        // Slack webhook or email
}

// Runs every 30 seconds
func (e *AlertEvaluator) EvaluateAll(ctx context.Context) {
    for _, rule := range e.rules {
        value := e.metricsStore.EvaluateExpression(ctx, rule.MetricQuery)
        if value.IsBreaching() {
            if e.hasBeenBreachingFor(rule.ID, rule.Duration) {
                e.fire(rule)
            }
        } else {
            e.clearBreachTimer(rule.ID)
        }
    }
}
```

### Load Testing

Instrument a dummy application (10 Go microservices, each emitting 1,000 log lines/sec, 50 metrics, and generating traces) and point them all at your collector. Document:
- Total log ingestion rate (target: 50,000+ logs/sec sustained).
- ClickHouse query latency for a full-text search over 1 billion log lines (target: under 2 seconds).
- End-to-end latency from log emission to dashboard visibility (target: under 3 seconds).
- Storage efficiency: how many GB per billion log lines after ClickHouse compression.

### Cost Management

- ClickHouse runs well in Docker locally. Use the official `clickhouse/clickhouse-server` image.
- For cloud deployment, ClickHouse on a single `r6g.large` instance (8GB RAM) can handle tens of thousands of logs/sec. You do not need a cluster for a portfolio project.
- Set aggressive TTLs (7 days for dev, 30 days for "prod") to keep disk usage low.

---
---