// Package tracestore is the storage adapter for traces, backed by PostgreSQL.
// It bulk-inserts spans idempotently and reconstructs whole traces (as a span
// tree) for waterfall rendering, plus a trace-search for the UI.
package tracestore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/1morshed1/gokraft/internal/model"
)

// Store wraps a pgx pool to Postgres.
type Store struct {
	pool *pgxpool.Pool
}

// Open dials Postgres with a standard DSN.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// InsertBatch writes spans. Because Kafka is at-least-once, the same span may be
// delivered twice after a writer crash. The primary key (trace_id, span_id) plus
// ON CONFLICT DO NOTHING makes re-inserts harmless no-ops — this is what makes
// the whole pipeline safe under redelivery.
//
// We send the batch inside a pgx.Batch (pipelined round-trip) rather than
// CopyFrom because CopyFrom cannot express ON CONFLICT. For trace volumes this
// is plenty fast.
func (s *Store) InsertBatch(ctx context.Context, spans []model.Span) error {
	if len(spans) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	const q = `
		INSERT INTO trace_spans
		    (trace_id, span_id, parent_span_id, service_name, operation_name,
		     start_time, duration_us, status_code, attributes, events)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (trace_id, span_id) DO NOTHING`
	for _, sp := range spans {
		attrs, _ := json.Marshal(orEmpty(sp.Attributes))
		events, _ := json.Marshal(sp.Events)
		var parent any
		if sp.ParentSpanID != "" {
			parent = sp.ParentSpanID
		}
		b.Queue(q, sp.TraceID, sp.SpanID, parent, sp.ServiceName, sp.Operation,
			sp.StartTime, sp.DurationUS, int16(sp.StatusCode), attrs, events)
	}
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()
	for range spans {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("insert span: %w", err)
		}
	}
	return nil
}

func orEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// GetSpans returns all spans of a trace, ordered by start time. The API layer
// turns this flat list into a tree using parent_span_id.
func (s *Store) GetSpans(ctx context.Context, traceID string) ([]model.Span, error) {
	const q = `
		SELECT trace_id, span_id, COALESCE(parent_span_id,''), service_name,
		       operation_name, start_time, duration_us, status_code, attributes, events
		FROM trace_spans
		WHERE trace_id = $1
		ORDER BY start_time`
	rows, err := s.pool.Query(ctx, q, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSpans(rows)
}

// TraceSummary is a compact row for the trace-search list view.
type TraceSummary struct {
	TraceID     string    `json:"trace_id"`
	RootService string    `json:"root_service"`
	RootOp      string    `json:"root_operation"`
	StartTime   time.Time `json:"start_time"`
	DurationUS  int64     `json:"duration_us"`
	SpanCount   int       `json:"span_count"`
	HasError    bool      `json:"has_error"`
}

// SearchParams filters the trace list.
type SearchParams struct {
	Service  string
	From, To time.Time
	MinDurUS int64
	OnlyErr  bool
	Limit    int
}

// Search returns recent traces matching the filters. It aggregates per trace_id:
// the root span (parent IS NULL) gives the display name and total duration, and
// we roll up span count and error presence.
func (s *Store) Search(ctx context.Context, p SearchParams) ([]TraceSummary, error) {
	conds := []string{"start_time >= $1", "start_time <= $2"}
	args := []any{defaultFrom(p.From), defaultTo(p.To)}
	if p.Service != "" {
		args = append(args, p.Service)
		conds = append(conds, fmt.Sprintf("service_name = $%d", len(args)))
	}
	limit := p.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	having := ""
	if p.OnlyErr {
		having = "HAVING bool_or(status_code = 2)"
	}
	if p.MinDurUS > 0 {
		if having == "" {
			having = "HAVING "
		} else {
			having += " AND "
		}
		args = append(args, p.MinDurUS)
		having += fmt.Sprintf("max(duration_us) >= $%d", len(args))
	}

	sql := fmt.Sprintf(`
		SELECT trace_id,
		       (array_agg(service_name   ORDER BY start_time))[1] AS root_service,
		       (array_agg(operation_name ORDER BY start_time))[1] AS root_op,
		       min(start_time)              AS start_time,
		       max(duration_us)             AS duration_us,
		       count(*)                     AS span_count,
		       bool_or(status_code = 2)     AS has_error
		FROM trace_spans
		WHERE %s
		GROUP BY trace_id
		%s
		ORDER BY start_time DESC
		LIMIT %d`, strings.Join(conds, " AND "), having, limit)

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TraceSummary
	for rows.Next() {
		var t TraceSummary
		if err := rows.Scan(&t.TraceID, &t.RootService, &t.RootOp, &t.StartTime,
			&t.DurationUS, &t.SpanCount, &t.HasError); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanSpans(rows pgx.Rows) ([]model.Span, error) {
	var out []model.Span
	for rows.Next() {
		var (
			sp          model.Span
			status      int16
			attrs, evts []byte
		)
		if err := rows.Scan(&sp.TraceID, &sp.SpanID, &sp.ParentSpanID, &sp.ServiceName,
			&sp.Operation, &sp.StartTime, &sp.DurationUS, &status, &attrs, &evts); err != nil {
			return nil, err
		}
		sp.StatusCode = model.StatusCode(status)
		_ = json.Unmarshal(attrs, &sp.Attributes)
		_ = json.Unmarshal(evts, &sp.Events)
		out = append(out, sp)
	}
	// Defensive: ensure deterministic order even if the driver reorders.
	sort.SliceStable(out, func(i, j int) bool { return out[i].StartTime.Before(out[j].StartTime) })
	return out, rows.Err()
}

func defaultFrom(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().Add(-1 * time.Hour)
	}
	return t
}
func defaultTo(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now()
	}
	return t
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }
