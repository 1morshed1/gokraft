// Package clickhouse is the storage adapter for logs. It owns the connection to
// ClickHouse, bulk-inserts batches of model.Log from the log writer, and serves
// the log search / live-tail / correlation queries the API needs.
//
// Design stance: keep all ClickHouse-specific SQL and driver quirks *in this
// package*. The rest of gokraft talks to a small Go API (InsertBatch, Search)
// and never sees a SQL string. If we ever swapped ClickHouse for something else,
// only this package would change.
package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/1morshed1/gokraft/internal/model"
)

// Store wraps a ClickHouse connection. The v2 driver's Conn is a connection
// pool and is safe for concurrent use, so one Store is shared process-wide.
type Store struct {
	conn driver.Conn
}

// Open dials ClickHouse. addr is host:port (native protocol, default 9000).
func Open(addr, database, username, password string) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database, Username: username, Password: password},
		// Compression cuts network bytes for the large batches we send.
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return &Store{conn: conn}, nil
}

// InsertBatch bulk-inserts logs using ClickHouse's native columnar batch API.
// PrepareBatch buffers rows column-by-column and Send() ships them in one shot —
// this is the single most important thing for ClickHouse write performance;
// row-at-a-time INSERTs are pathologically slow on a column store.
func (s *Store) InsertBatch(ctx context.Context, logs []model.Log) error {
	if len(logs) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO gokraft.logs")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, l := range logs {
		attrs := l.Attributes
		if attrs == nil {
			attrs = map[string]string{}
		}
		if err := batch.Append(
			l.Timestamp,
			l.TraceID,
			l.SpanID,
			l.Severity,
			l.ServiceName,
			l.Body,
			attrs,
			l.K8s.PodName,
			l.K8s.Namespace,
			l.K8s.NodeName,
		); err != nil {
			return fmt.Errorf("append row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

// SearchParams describes a log query from the API. Zero values mean "no filter".
type SearchParams struct {
	Query    string // substring match on body (uses the tokenbf index)
	Service  string
	Severity string
	TraceID  string
	From, To time.Time
	Limit    int
	Offset   int
}

// Search runs a filtered, paginated log query. It builds the WHERE clause
// dynamically but *always* uses bound parameters (never string concatenation of
// user input) so it is injection-safe. Results are newest-first.
func (s *Store) Search(ctx context.Context, p SearchParams) ([]model.Log, error) {
	var (
		conds []string
		args  []any
	)
	add := func(cond string, val any) {
		conds = append(conds, cond)
		args = append(args, val)
	}
	if !p.From.IsZero() {
		add("timestamp >= ?", p.From)
	}
	if !p.To.IsZero() {
		add("timestamp <= ?", p.To)
	}
	if p.Service != "" {
		add("service_name = ?", p.Service)
	}
	if p.Severity != "" {
		add("severity = ?", p.Severity)
	}
	if p.TraceID != "" {
		add("trace_id = ?", p.TraceID)
	}
	if p.Query != "" {
		// positionCaseInsensitive lets the tokenbf_v1 index prune granules while
		// still matching case-insensitively. The parameter is bound, not inlined.
		add("positionCaseInsensitive(body, ?) > 0", p.Query)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	limit := p.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100 // sane default / hard cap to protect the server
	}

	sql := fmt.Sprintf(`
		SELECT timestamp, trace_id, span_id, severity, service_name, body,
		       attributes, k8s_pod, k8s_namespace, k8s_node
		FROM gokraft.logs
		%s
		ORDER BY timestamp DESC
		LIMIT %d OFFSET %d`, where, limit, p.Offset)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []model.Log
	for rows.Next() {
		var (
			l     model.Log
			attrs map[string]string
		)
		if err := rows.Scan(
			&l.Timestamp, &l.TraceID, &l.SpanID, &l.Severity, &l.ServiceName,
			&l.Body, &attrs, &l.K8s.PodName, &l.K8s.Namespace, &l.K8s.NodeName,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		l.Attributes = attrs
		out = append(out, l)
	}
	return out, rows.Err()
}

// Close releases the connection pool.
func (s *Store) Close() error { return s.conn.Close() }
