package query

// tree.go reconstructs a trace's parent/child structure from the flat list of
// spans the trace store returns. A distributed trace is logically a tree — the
// root span (an incoming HTTP request, say) has children (a DB call, an RPC to
// another service), which have their own children — but the database stores
// spans as independent rows linked only by parent_span_id. Rebuilding the tree
// is pure in-memory work, so it lives here, fully unit-tested, with no database.

import (
	"sort"
	"time"

	"github.com/1morshed1/gokraft/internal/model"
)

// SpanNode is a span plus its children, ready for waterfall rendering. Depth is
// pre-computed so the UI can indent without walking the tree itself.
type SpanNode struct {
	model.Span
	Depth    int         `json:"depth"`
	Children []*SpanNode `json:"children,omitempty"`
}

// Trace is the assembled view returned by the API for a single trace_id.
type Trace struct {
	TraceID    string      `json:"trace_id"`
	Roots      []*SpanNode `json:"roots"`
	SpanCount  int         `json:"span_count"`
	StartTime  time.Time   `json:"start_time"`
	EndTime    time.Time   `json:"end_time"`
	DurationUS int64       `json:"duration_us"`
	Services   []string    `json:"services"`
}

// BuildTrace turns a flat span slice into a Trace.
//
// Two real-world complications it handles:
//
//   - Orphan spans. A span may reference a parent that never arrived (dropped,
//     or still in flight, or from a service that didn't export). Rather than
//     drop such spans, we promote them to roots so nothing is hidden from the
//     user. A trace can therefore legitimately have several roots.
//
//   - Determinism. Children are sorted by start time so the waterfall reads
//     top-to-bottom in execution order.
func BuildTrace(traceID string, spans []model.Span) Trace {
	tr := Trace{TraceID: traceID, SpanCount: len(spans)}
	if len(spans) == 0 {
		return tr
	}

	// Index nodes by span id.
	nodes := make(map[string]*SpanNode, len(spans))
	for i := range spans {
		s := spans[i]
		nodes[s.SpanID] = &SpanNode{Span: s}
	}

	serviceSet := map[string]struct{}{}
	var roots []*SpanNode

	for _, n := range nodes {
		serviceSet[n.ServiceName] = struct{}{}
		parent, hasParent := nodes[n.ParentSpanID]
		if n.ParentSpanID == "" || !hasParent {
			// True root, or an orphan we promote to root.
			roots = append(roots, n)
			continue
		}
		parent.Children = append(parent.Children, n)
	}

	// Sort children (and roots) by start time, then assign depth via DFS.
	var assignDepth func(n *SpanNode, d int)
	assignDepth = func(n *SpanNode, d int) {
		n.Depth = d
		sort.SliceStable(n.Children, func(i, j int) bool {
			return n.Children[i].StartTime.Before(n.Children[j].StartTime)
		})
		for _, c := range n.Children {
			assignDepth(c, d+1)
		}
	}
	sort.SliceStable(roots, func(i, j int) bool {
		return roots[i].StartTime.Before(roots[j].StartTime)
	})
	for _, r := range roots {
		assignDepth(r, 0)
	}
	tr.Roots = roots

	// Compute overall trace window and service set.
	tr.StartTime = spans[0].StartTime
	tr.EndTime = spans[0].EndTime()
	for _, s := range spans {
		if s.StartTime.Before(tr.StartTime) {
			tr.StartTime = s.StartTime
		}
		if e := s.EndTime(); e.After(tr.EndTime) {
			tr.EndTime = e
		}
	}
	tr.DurationUS = tr.EndTime.Sub(tr.StartTime).Microseconds()

	tr.Services = make([]string, 0, len(serviceSet))
	for svc := range serviceSet {
		tr.Services = append(tr.Services, svc)
	}
	sort.Strings(tr.Services)

	return tr
}
