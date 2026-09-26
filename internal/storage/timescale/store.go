// Package timescale is the storage adapter for metrics. It bulk-inserts raw
// samples and answers time-series queries, transparently choosing the raw table
// or a pre-computed rollup based on the requested time range.
package timescale

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/1morshed1/gokraft/internal/model"
)

// Store wraps a pgx connection pool to TimescaleDB (which is just Postgres with
// an extension, so we use the ordinary Postgres driver).
type Store struct {
	pool *pgxpool.Pool
}

// Open dials TimescaleDB using a standard Postgres DSN, e.g.
// "postgres://user:pass@host:5432/metrics".
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("timescale pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("timescale ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// InsertBatch bulk-loads samples using Postgres COPY (pgx CopyFrom), which is
// dramatically faster than multi-row INSERT for large batches because it uses
// the binary COPY protocol and avoids per-row statement parsing.
func (s *Store) InsertBatch(ctx context.Context, metrics []model.Metric) error {
	if len(metrics) == 0 {
		return nil
	}
	rows := make([][]any, 0, len(metrics))
	for _, m := range metrics {
		var tags any
		if len(m.Tags) > 0 {
			b, err := json.Marshal(m.Tags)
			if err != nil {
				continue
			}
			tags = b
		}
		rows = append(rows, []any{m.Timestamp, m.Name, m.ServiceName, tags, m.Value})
	}
	_, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"metrics"},
		[]string{"time", "metric_name", "service_name", "tags", "value"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy metrics: %w", err)
	}
	return nil
}

// QueryParams describes a metric query.
type QueryParams struct {
	Metric      string
	Service     string
	From, To    time.Time
	Aggregation string        // avg | max | min | sum (defaults to avg)
	Interval    time.Duration // bucket width; 0 => auto from range
}

// Point is one time bucket of a series.
type Point struct {
	Bucket time.Time `json:"bucket"`
	Value  float64   `json:"value"`
}

// selectTable is the rollup-selection heuristic — the crux of keeping dashboard
// queries fast regardless of range. Short ranges read full-resolution raw data;
// wider ranges read progressively coarser pre-aggregated views so the row count
// scanned stays roughly constant.
//
// Returns the table/view name and the column expression for the requested
// aggregation. Rollup views store pre-aggregated columns (avg_val, max_val, …)
// while the raw table stores individual samples we aggregate on the fly.
func selectTable(rng time.Duration, agg string) (table, valueExpr, timeCol string) {
	switch {
	case rng <= 30*time.Minute:
		return "metrics", rawAgg(agg) + "(value)", "time"
	case rng <= 24*time.Hour:
		return "metrics_1m", rollupCol(agg), "bucket"
	default:
		return "metrics_1h", rollupCol(agg), "bucket"
	}
}

func rawAgg(agg string) string {
	switch agg {
	case "max", "min", "sum":
		return agg
	default:
		return "avg"
	}
}

// rollupCol maps an aggregation to the matching pre-computed column, wrapped so
// re-bucketing (a coarser interval than the view's native bucket) still works.
func rollupCol(agg string) string {
	switch agg {
	case "max":
		return "max(max_val)"
	case "min":
		return "min(min_val)"
	case "sum":
		return "sum(sum_val)"
	default:
		return "avg(avg_val)"
	}
}

// Query returns a time-bucketed series. It always parameterizes user input.
func (s *Store) Query(ctx context.Context, p QueryParams) ([]Point, error) {
	if p.From.IsZero() || p.To.IsZero() || !p.To.After(p.From) {
		return nil, fmt.Errorf("invalid time range")
	}
	rng := p.To.Sub(p.From)
	table, valueExpr, timeCol := selectTable(rng, p.Aggregation)

	interval := p.Interval
	if interval <= 0 {
		interval = autoInterval(rng)
	}

	conds := []string{fmt.Sprintf("%s >= $1", timeCol), fmt.Sprintf("%s <= $2", timeCol)}
	args := []any{p.From, p.To}
	if p.Metric != "" {
		args = append(args, p.Metric)
		conds = append(conds, fmt.Sprintf("metric_name = $%d", len(args)))
	}
	if p.Service != "" {
		args = append(args, p.Service)
		conds = append(conds, fmt.Sprintf("service_name = $%d", len(args)))
	}

	// time_bucket is a TimescaleDB function; the interval literal is derived
	// from our own enum-like autoInterval / validated duration, never user text.
	sql := fmt.Sprintf(`
		SELECT time_bucket('%s', %s) AS b, %s AS v
		FROM %s
		WHERE %s
		GROUP BY b
		ORDER BY b`,
		pgInterval(interval), timeCol, valueExpr, table, strings.Join(conds, " AND "))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("metric query: %w", err)
	}
	defer rows.Close()

	var out []Point
	for rows.Next() {
		var pt Point
		if err := rows.Scan(&pt.Bucket, &pt.Value); err != nil {
			return nil, err
		}
		out = append(out, pt)
	}
	return out, rows.Err()
}

// autoInterval picks a bucket width that yields ~100-300 points for the range,
// which is roughly how many a chart can usefully render.
func autoInterval(rng time.Duration) time.Duration {
	switch {
	case rng <= 30*time.Minute:
		return 10 * time.Second
	case rng <= 6*time.Hour:
		return time.Minute
	case rng <= 24*time.Hour:
		return 5 * time.Minute
	case rng <= 7*24*time.Hour:
		return time.Hour
	default:
		return 6 * time.Hour
	}
}

// pgInterval renders a duration as a Postgres interval literal like "10 seconds".
func pgInterval(d time.Duration) string {
	if d%time.Hour == 0 {
		return fmt.Sprintf("%d hours", int(d/time.Hour))
	}
	if d%time.Minute == 0 {
		return fmt.Sprintf("%d minutes", int(d/time.Minute))
	}
	return fmt.Sprintf("%d seconds", int(d/time.Second))
}

// Scalar evaluates a single aggregate value over a window — used by the alerting
// engine to test rule conditions like "avg(error_rate) over 5m".
func (s *Store) Scalar(ctx context.Context, metric, service, agg string, from, to time.Time) (float64, bool, error) {
	args := []any{from, to}
	conds := []string{"time >= $1", "time <= $2"}
	if metric != "" {
		args = append(args, metric)
		conds = append(conds, fmt.Sprintf("metric_name = $%d", len(args)))
	}
	if service != "" {
		args = append(args, service)
		conds = append(conds, fmt.Sprintf("service_name = $%d", len(args)))
	}
	sql := fmt.Sprintf(`SELECT %s(value) FROM metrics WHERE %s`,
		rawAgg(agg), strings.Join(conds, " AND "))

	var v *float64
	if err := s.pool.QueryRow(ctx, sql, args...).Scan(&v); err != nil {
		return 0, false, err
	}
	if v == nil { // no rows matched -> NULL
		return 0, false, nil
	}
	return *v, true, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }
