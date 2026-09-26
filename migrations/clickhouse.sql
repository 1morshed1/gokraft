-- ClickHouse schema for LOG storage.
--
-- Why ClickHouse for logs? Logs are high-volume, append-only, and queried by
-- "time range + service + severity + text". That is an analytical (OLAP) access
-- pattern, and ClickHouse is a column-oriented OLAP database purpose-built for
-- it. Column storage means a query touching 3 columns reads only those 3
-- columns off disk, not whole rows. Combined with per-column compression it is
-- routinely 10-50x faster and 5-10x smaller on disk than a row store or
-- Elasticsearch for this workload.
--
-- Run: clickhouse-client --multiquery < migrations/clickhouse.sql
--       (docker-compose mounts this file to auto-apply on first boot)

CREATE DATABASE IF NOT EXISTS gokraft;

CREATE TABLE IF NOT EXISTS gokraft.logs
(
    timestamp     DateTime64(3, 'UTC'),                 -- millisecond precision, UTC
    trace_id      String,                               -- links a log to its trace
    span_id       String,
    severity      LowCardinality(String),               -- INFO/WARN/ERROR/FATAL
    service_name  LowCardinality(String),
    body          String,                               -- the log message
    attributes    Map(String, String),                  -- arbitrary key/value
    k8s_pod       LowCardinality(String),
    k8s_namespace LowCardinality(String),
    k8s_node      LowCardinality(String),

    -- A token bloom-filter index accelerates full-text `LIKE '%needle%'` search
    -- on the body without the storage cost of a full inverted index. It can
    -- produce false positives (which ClickHouse then filters exactly) but never
    -- false negatives, so results stay correct.
    INDEX idx_body body TYPE tokenbf_v1(30720, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree()
-- One partition per day. Dropping old data becomes an instant partition DROP
-- rather than a row-by-row DELETE.
PARTITION BY toYYYYMMDD(timestamp)
-- The sort key encodes the dominant query: "ERROR logs from service X in the
-- last hour". Ordering by (service, severity, timestamp) lets ClickHouse skip
-- whole granules that can't match.
ORDER BY (service_name, severity, timestamp)
-- Automatic expiry: rows older than 30 days are dropped by the background merge.
-- Lower this to 7 days for a dev box to save disk.
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Design notes:
--  * LowCardinality(String) dictionary-encodes columns with few distinct values
--    (severity has ~4, service_name maybe dozens). This shrinks them by 90%+ and
--    speeds up filtering/grouping on them.
--  * We do NOT create a per-row primary key; MergeTree's ORDER BY is the sparse
--    primary index. There is no uniqueness constraint — logs are naturally
--    append-only and duplicates from Kafka redelivery are acceptable and rare.
