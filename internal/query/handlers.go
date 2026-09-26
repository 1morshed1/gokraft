package query

// handlers.go is the HTTP surface of gokraft: the REST endpoints that turn user
// queries into store calls and JSON responses, plus the WebSocket live-tail and
// the cross-signal correlation endpoint.
//
// The handlers depend on three narrow *interfaces* (LogStore, MetricStore,
// TraceStore), not on the concrete ClickHouse/Timescale/Postgres stores. That
// inversion lets us unit-test handler logic with in-memory fakes and keeps the
// HTTP layer ignorant of any database. The concrete stores satisfy these
// interfaces structurally (Go's implicit interface satisfaction).

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/1morshed1/gokraft/internal/model"
	"github.com/1morshed1/gokraft/internal/storage/clickhouse"
	"github.com/1morshed1/gokraft/internal/storage/timescale"
	"github.com/1morshed1/gokraft/internal/storage/tracestore"
)

// LogStore is the subset of the ClickHouse store the API needs.
type LogStore interface {
	Search(ctx context.Context, p clickhouse.SearchParams) ([]model.Log, error)
}

// MetricStore is the subset of the TimescaleDB store the API needs.
type MetricStore interface {
	Query(ctx context.Context, p timescale.QueryParams) ([]timescale.Point, error)
}

// TraceStore is the subset of the Postgres trace store the API needs.
type TraceStore interface {
	GetSpans(ctx context.Context, traceID string) ([]model.Span, error)
	Search(ctx context.Context, p tracestore.SearchParams) ([]tracestore.TraceSummary, error)
}

// Handlers bundles the store dependencies for the route registrations.
type Handlers struct {
	Logs    LogStore
	Metrics MetricStore
	Traces  TraceStore
}

// Register wires all query routes onto the given router group.
func (h *Handlers) Register(r *gin.RouterGroup) {
	r.GET("/logs/search", h.searchLogs)
	r.GET("/logs/tail", h.tailLogs) // WebSocket
	r.GET("/metrics/query", h.queryMetrics)
	r.GET("/traces/search", h.searchTraces)
	r.GET("/traces/:trace_id", h.getTrace)
	r.GET("/correlate/:trace_id", h.correlate)
}

// --- time-range parsing shared by several handlers ---

// parseTimeRange reads from/to query params. Each accepts either an RFC3339
// timestamp or a relative "-15m"/"-2h" style offset from now, which is what a
// dashboard's "last 15 minutes" picker sends. Defaults to the last hour.
func parseTimeRange(c *gin.Context) (from, to time.Time) {
	now := time.Now().UTC()
	to = parseInstant(c.Query("to"), now)
	from = parseInstant(c.Query("from"), to.Add(-time.Hour))
	return from, to
}

func parseInstant(s string, def time.Time) time.Time {
	if s == "" {
		return def
	}
	if d, err := time.ParseDuration(s); err == nil {
		// Relative offset like "-15m".
		return time.Now().UTC().Add(d)
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC()
	}
	return def
}

func atoiDefault(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

// --- logs ---

func (h *Handlers) searchLogs(c *gin.Context) {
	from, to := parseTimeRange(c)
	p := clickhouse.SearchParams{
		Query:    c.Query("q"),
		Service:  c.Query("service"),
		Severity: c.Query("severity"),
		TraceID:  c.Query("trace_id"),
		From:     from,
		To:       to,
		Limit:    atoiDefault(c.Query("limit"), 100),
		Offset:   atoiDefault(c.Query("offset"), 0),
	}
	logs, err := h.Logs.Search(c.Request.Context(), p)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"logs": logs, "count": len(logs)})
}

// --- metrics ---

func (h *Handlers) queryMetrics(c *gin.Context) {
	from, to := parseTimeRange(c)
	var interval time.Duration
	if iv := c.Query("interval"); iv != "" {
		if d, err := time.ParseDuration(iv); err == nil {
			interval = d
		}
	}
	p := timescale.QueryParams{
		Metric:      c.Query("metric"),
		Service:     c.Query("service"),
		From:        from,
		To:          to,
		Aggregation: c.Query("aggregation"),
		Interval:    interval,
	}
	points, err := h.Metrics.Query(c.Request.Context(), p)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"metric": p.Metric, "service": p.Service, "aggregation": p.Aggregation,
		"points": points,
	})
}

// --- traces ---

func (h *Handlers) searchTraces(c *gin.Context) {
	from, to := parseTimeRange(c)
	p := tracestore.SearchParams{
		Service:  c.Query("service"),
		From:     from,
		To:       to,
		MinDurUS: int64(atoiDefault(c.Query("min_duration_us"), 0)),
		OnlyErr:  c.Query("only_errors") == "true",
		Limit:    atoiDefault(c.Query("limit"), 50),
	}
	traces, err := h.Traces.Search(c.Request.Context(), p)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"traces": traces, "count": len(traces)})
}

func (h *Handlers) getTrace(c *gin.Context) {
	traceID := c.Param("trace_id")
	spans, err := h.Traces.GetSpans(c.Request.Context(), traceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(spans) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "trace not found"})
		return
	}
	c.JSON(http.StatusOK, BuildTrace(traceID, spans))
}

// --- cross-signal correlation: the "killer feature" ---

// correlate assembles the single coherent picture that makes an observability
// platform valuable: given a trace_id, return the trace tree AND every log that
// carries that trace_id, bounded to the trace's own time window (padded a
// little). From an error log the UI can jump here and instantly see the whole
// request that produced it — logs and trace side by side. This is precisely the
// logs↔traces join that the shared trace_id field (set at conversion time in
// the collector) exists to enable.
func (h *Handlers) correlate(c *gin.Context) {
	traceID := c.Param("trace_id")
	ctx := c.Request.Context()

	spans, err := h.Traces.GetSpans(ctx, traceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(spans) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "trace not found"})
		return
	}
	trace := BuildTrace(traceID, spans)

	// Fetch logs tagged with this trace, within [start-1s, end+1s] so we catch
	// logs emitted just before/after the span boundaries.
	logs, err := h.Logs.Search(ctx, clickhouse.SearchParams{
		TraceID: traceID,
		From:    trace.StartTime.Add(-time.Second),
		To:      trace.EndTime.Add(time.Second),
		Limit:   1000,
	})
	if err != nil {
		// Correlation is best-effort: return the trace even if the log join
		// fails, with the error noted, rather than failing the whole request.
		c.JSON(http.StatusOK, gin.H{"trace": trace, "logs": []model.Log{}, "logs_error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"trace": trace, "logs": logs})
}
