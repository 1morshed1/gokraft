package query

import (
	"testing"
	"time"

	"github.com/1morshed1/gokraft/internal/model"
)

func at(base time.Time, ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }

// TestBuildTraceTree verifies parent/child linking, depth assignment, and the
// overall duration window for a simple two-level trace.
func TestBuildTraceTree(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []model.Span{
		{TraceID: "t", SpanID: "root", ServiceName: "api", Operation: "GET /", StartTime: base, DurationUS: 100_000},
		{TraceID: "t", SpanID: "db", ParentSpanID: "root", ServiceName: "pg", Operation: "SELECT", StartTime: at(base, 10), DurationUS: 20_000},
		{TraceID: "t", SpanID: "cache", ParentSpanID: "root", ServiceName: "redis", Operation: "GET", StartTime: at(base, 5), DurationUS: 1_000},
	}

	tr := BuildTrace("t", spans)
	if len(tr.Roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(tr.Roots))
	}
	root := tr.Roots[0]
	if root.SpanID != "root" || root.Depth != 0 {
		t.Fatalf("root wrong: %+v", root)
	}
	if len(root.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(root.Children))
	}
	// Children sorted by start time: cache (t+5ms) before db (t+10ms).
	if root.Children[0].SpanID != "cache" || root.Children[1].SpanID != "db" {
		t.Errorf("children not sorted by start time: %s, %s", root.Children[0].SpanID, root.Children[1].SpanID)
	}
	if root.Children[0].Depth != 1 {
		t.Errorf("child depth = %d, want 1", root.Children[0].Depth)
	}
	// Root runs 100ms; that is the total window.
	if tr.DurationUS != 100_000 {
		t.Errorf("trace duration = %d us, want 100000", tr.DurationUS)
	}
	if len(tr.Services) != 3 {
		t.Errorf("expected 3 services, got %v", tr.Services)
	}
}

// TestOrphanPromotedToRoot verifies a span whose parent is missing is not
// dropped but surfaced as an additional root.
func TestOrphanPromotedToRoot(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	spans := []model.Span{
		{TraceID: "t", SpanID: "a", ParentSpanID: "ghost", ServiceName: "svc", StartTime: base, DurationUS: 1000},
	}
	tr := BuildTrace("t", spans)
	if len(tr.Roots) != 1 || tr.Roots[0].SpanID != "a" {
		t.Fatalf("orphan not promoted to root: %+v", tr.Roots)
	}
}

func TestBuildTraceEmpty(t *testing.T) {
	tr := BuildTrace("t", nil)
	if tr.SpanCount != 0 || len(tr.Roots) != 0 {
		t.Fatalf("empty trace should have no spans/roots: %+v", tr)
	}
}
