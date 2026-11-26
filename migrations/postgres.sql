-- PostgreSQL schema for TRACE storage.
--
-- Why plain Postgres for traces? Traces are comparatively low-volume (you
-- sample a fraction of requests, and each trace is a handful of spans) and the
-- access pattern is relational: "give me every span with this trace_id, then
-- rebuild the parent/child tree". That is exactly what a well-indexed relational
-- table with JSONB for the flexible bits does best. At much higher scale you'd
-- move traces to ClickHouse or ScyllaDB, but for a demo-scale platform Postgres
-- is the pragmatic, boring, correct choice.
--
-- Run: psql "$POSTGRES_DSN" -f migrations/postgres.sql

CREATE TABLE IF NOT EXISTS trace_spans (
    trace_id       TEXT        NOT NULL,
    span_id        TEXT        NOT NULL,
    parent_span_id TEXT,                          -- NULL for the root span
    service_name   TEXT        NOT NULL,
    operation_name TEXT        NOT NULL,
    start_time     TIMESTAMPTZ NOT NULL,
    duration_us    BIGINT      NOT NULL,          -- microseconds
    status_code    SMALLINT    NOT NULL,          -- 0=unset, 1=ok, 2=error
    attributes     JSONB,                         -- arbitrary span attributes
    events         JSONB,                         -- span events (e.g. exceptions)

    -- (trace_id, span_id) is globally unique. Making it the PRIMARY KEY gives us
    -- idempotent writes for free: Kafka may redeliver a span after a writer
    -- crash, and `ON CONFLICT DO NOTHING` (see the writer) turns the duplicate
    -- into a no-op. This is how we make at-least-once delivery safe.
    PRIMARY KEY (trace_id, span_id)
);

-- "Search traces from service X, newest first" and "recent traces overall".
CREATE INDEX IF NOT EXISTS idx_spans_service ON trace_spans (service_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_spans_time    ON trace_spans (start_time DESC);

-- Speeds up rebuilding the tree: find a span's children within a trace.
CREATE INDEX IF NOT EXISTS idx_spans_parent  ON trace_spans (trace_id, parent_span_id);

-- To render a waterfall: SELECT * FROM trace_spans WHERE trace_id = $1
-- ORDER BY start_time; then link children to parents by parent_span_id in code.
