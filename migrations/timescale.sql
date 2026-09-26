-- TimescaleDB schema for METRIC storage.
--
-- Why TimescaleDB for metrics? Metrics are numeric time-series: you rarely want
-- a single sample, you want "avg CPU per minute over the last 6 hours". Two
-- TimescaleDB features fit this perfectly:
--   1. Hypertables: a regular Postgres table transparently partitioned by time
--      into "chunks". Inserts and time-range queries hit only the relevant
--      chunks, keeping both fast as the table grows to billions of rows.
--   2. Continuous aggregates: materialized, incrementally-refreshed rollups.
--      We pre-compute 1-minute and 1-hour summaries so dashboard queries over
--      wide time ranges read a few thousand pre-aggregated rows instead of
--      scanning millions of raw samples.
--
-- Run: psql "$TIMESCALE_DSN" -f migrations/timescale.sql

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw samples. This is the full-resolution source of truth.
CREATE TABLE IF NOT EXISTS metrics (
    time         TIMESTAMPTZ      NOT NULL,
    metric_name  TEXT             NOT NULL,
    service_name TEXT             NOT NULL,
    tags         JSONB,
    value        DOUBLE PRECISION NOT NULL
);

-- Turn it into a hypertable partitioned on `time`. Idempotent via if_not_exists.
SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);

-- Indexes for the two dominant filters: by metric name and by service, both
-- newest-first (time DESC) because dashboards almost always ask for "recent".
CREATE INDEX IF NOT EXISTS idx_metrics_name_time    ON metrics (metric_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_service_time ON metrics (service_name, time DESC);

-- 1-minute rollups. `WITH NO DATA` creates the view empty; the policy below
-- fills and maintains it incrementally in the background.
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    metric_name,
    service_name,
    avg(value)  AS avg_val,
    max(value)  AS max_val,
    min(value)  AS min_val,
    sum(value)  AS sum_val,
    count(*)    AS sample_count
FROM metrics
GROUP BY bucket, metric_name, service_name
WITH NO DATA;

-- Refresh policy: every minute, roll up data between 1 hour ago and 1 minute
-- ago. The end_offset keeps the most recent (still-arriving) minute out of the
-- aggregate so we never publish a half-filled bucket.
SELECT add_continuous_aggregate_policy('metrics_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE);

-- 1-hour rollups, computed FROM the 1-minute rollups (cheaper than re-scanning
-- raw data). Note we must re-aggregate correctly: avg-of-avgs is only valid
-- here because bucket sizes are uniform; for exactness at scale you'd weight by
-- sample_count. Kept simple for learning.
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    metric_name,
    service_name,
    avg(avg_val)      AS avg_val,
    max(max_val)      AS max_val,
    min(min_val)      AS min_val,
    sum(sum_val)      AS sum_val,
    sum(sample_count) AS sample_count
FROM metrics_1m
GROUP BY time_bucket('1 hour', bucket), metric_name, service_name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('metrics_1h',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE);

-- Data retention: drop raw samples older than 7 days (the rollups live longer
-- and carry the history). This keeps the high-volume raw table small.
SELECT add_retention_policy('metrics', INTERVAL '7 days', if_not_exists => TRUE);
