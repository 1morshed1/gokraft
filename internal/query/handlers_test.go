package query

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/1morshed1/gokraft/internal/model"
	"github.com/1morshed1/gokraft/internal/storage/clickhouse"
	"github.com/1morshed1/gokraft/internal/storage/timescale"
	"github.com/1morshed1/gokraft/internal/storage/tracestore"
)

// --- in-memory fakes satisfying the store interfaces ---

type fakeLogs struct {
	logs       []model.Log
	lastParams clickhouse.SearchParams
}

func (f *fakeLogs) Search(_ context.Context, p clickhouse.SearchParams) ([]model.Log, error) {
	f.lastParams = p
	return f.logs, nil
}

type fakeTraces struct{ spans []model.Span }

func (f *fakeTraces) GetSpans(context.Context, string) ([]model.Span, error) { return f.spans, nil }
func (f *fakeTraces) Search(context.Context, tracestore.SearchParams) ([]tracestore.TraceSummary, error) {
	return nil, nil
}

type fakeMetrics struct{}

func (fakeMetrics) Query(context.Context, timescale.QueryParams) ([]timescale.Point, error) {
	return nil, nil
}

func newTestRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.Register(r.Group("/api/v1"))
	return r
}

// TestCorrelateJoinsLogsAndTrace is the key test: it proves the correlation
// endpoint returns both the assembled trace tree and the logs sharing the
// trace_id, and that it queries logs scoped to the trace's own time window.
func TestCorrelateJoinsLogsAndTrace(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	traces := &fakeTraces{spans: []model.Span{
		{TraceID: "abc", SpanID: "root", ServiceName: "api", Operation: "GET /", StartTime: base, DurationUS: 50_000},
	}}
	logs := &fakeLogs{logs: []model.Log{
		{TraceID: "abc", Severity: "ERROR", ServiceName: "api", Body: "boom", Timestamp: base.Add(10 * time.Millisecond)},
	}}
	h := &Handlers{Logs: logs, Metrics: fakeMetrics{}, Traces: traces}
	r := newTestRouter(h)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/correlate/abc", nil)
	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var resp struct {
		Trace Trace       `json:"trace"`
		Logs  []model.Log `json:"logs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Trace.TraceID != "abc" || len(resp.Trace.Roots) != 1 {
		t.Errorf("trace not assembled: %+v", resp.Trace)
	}
	if len(resp.Logs) != 1 || resp.Logs[0].Body != "boom" {
		t.Errorf("logs not correlated: %+v", resp.Logs)
	}
	// The log query must be filtered by the trace id and bounded near the
	// trace window (start-1s .. end+1s), not an unbounded scan.
	if logs.lastParams.TraceID != "abc" {
		t.Errorf("log query not filtered by trace_id: %+v", logs.lastParams)
	}
	if logs.lastParams.From.After(base) || logs.lastParams.To.Before(base.Add(50*time.Millisecond)) {
		t.Errorf("log query window does not cover the trace: %+v", logs.lastParams)
	}
}

// TestSearchLogsParsesRelativeTime checks that a "-15m" from param is turned
// into a real time window handed to the store.
func TestSearchLogsParsesRelativeTime(t *testing.T) {
	logs := &fakeLogs{}
	h := &Handlers{Logs: logs, Metrics: fakeMetrics{}, Traces: &fakeTraces{}}
	r := newTestRouter(h)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/logs/search?q=timeout&service=api&from=-15m", nil)
	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status = %d", w.Code)
	}
	if logs.lastParams.Query != "timeout" || logs.lastParams.Service != "api" {
		t.Errorf("filters not passed through: %+v", logs.lastParams)
	}
	window := logs.lastParams.To.Sub(logs.lastParams.From)
	if window < 14*time.Minute || window > 16*time.Minute {
		t.Errorf("expected ~15m window, got %v", window)
	}
}
