// Package model defines the normalized, storage-agnostic representation of the
// three telemetry signals gokraft handles: logs, metrics, and traces.
//
// Why a normalized model?
//
// The collector receives data in OpenTelemetry's OTLP protobuf format, which is
// deeply nested and optimized for wire transport, not for the ergonomics of our
// downstream code. Rather than passing OTLP structs all the way through Kafka
// and into every storage writer, we convert OTLP into these flat, purpose-built
// Go structs at the edge (in the collector). Every component after the collector
// — the Kafka bus, the writers, tests — speaks this vocabulary instead.
//
// This gives us three concrete benefits:
//  1. Decoupling: if the OTLP schema changes, only the collector's conversion
//     code changes. The rest of the system is insulated.
//  2. Serialization simplicity: these structs marshal to compact JSON, which is
//     what we push onto Kafka. JSON is easy to inspect while learning; a
//     production system might switch to protobuf or Avro for efficiency.
//  3. Testability: constructing a Log or Metric in a test is trivial, whereas
//     building a valid OTLP ExportLogsServiceRequest by hand is painful.
package model

import "time"

// SignalType identifies which of the three telemetry pillars a record belongs
// to. It is used to route records to the correct Kafka topic and writer.
type SignalType string

const (
	SignalLogs    SignalType = "logs"
	SignalMetrics SignalType = "metrics"
	SignalTraces  SignalType = "traces"
)

// K8sMetadata holds the Kubernetes context the enricher attaches to every
// signal. In a real cluster these are resolved from the sender's pod IP; in
// local/dev mode they may be empty or populated from environment variables via
// the Downward API.
type K8sMetadata struct {
	PodName   string `json:"pod_name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	NodeName  string `json:"node_name,omitempty"`
}

// Log is a single log record, normalized from OTLP LogRecord.
//
// The field set mirrors our ClickHouse `logs` table (see migrations) so the log
// writer is a near-direct mapping. TraceID/SpanID are the linchpin of
// cross-signal correlation: a log emitted inside a traced request carries the
// same TraceID as the trace's spans, letting the query API jump from "this
// error log" to "the trace that produced it".
type Log struct {
	Timestamp   time.Time         `json:"timestamp"`
	TraceID     string            `json:"trace_id,omitempty"`
	SpanID      string            `json:"span_id,omitempty"`
	Severity    string            `json:"severity"`     // INFO, WARN, ERROR, FATAL
	ServiceName string            `json:"service_name"` // logical service that emitted it
	Body        string            `json:"body"`         // the human-readable message
	Attributes  map[string]string `json:"attributes,omitempty"`
	K8s         K8sMetadata       `json:"k8s"`
}

// Metric is a single numeric measurement at a point in time.
//
// OTLP metrics are rich (gauges, sums, histograms, exponential histograms, each
// with many data points). We flatten every data point into one Metric row keyed
// by name + service + tags + timestamp. This is the classic "narrow table"
// time-series shape that TimescaleDB and its continuous aggregates work best
// with. Histograms are decomposed into multiple Metric rows (e.g. _count, _sum,
// and per-bucket) by the collector's conversion code.
type Metric struct {
	Timestamp   time.Time         `json:"timestamp"`
	Name        string            `json:"name"`
	ServiceName string            `json:"service_name"`
	Value       float64           `json:"value"`
	Tags        map[string]string `json:"tags,omitempty"`
	K8s         K8sMetadata       `json:"k8s"`
}

// StatusCode mirrors OTLP's span status: unset, ok, or error. Stored as a small
// integer in Postgres so it is cheap to filter on ("show me only error spans").
type StatusCode int16

const (
	StatusUnset StatusCode = 0
	StatusOK    StatusCode = 1
	StatusError StatusCode = 2
)

// SpanEvent is a timestamped annotation attached to a span (OTLP calls these
// "events"; they are effectively structured logs scoped to a span, e.g. an
// exception with a stack trace).
type SpanEvent struct {
	Timestamp  time.Time         `json:"timestamp"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Span is one node in a distributed trace. A trace is the set of all spans
// sharing a TraceID; the parent/child structure is reconstructed from
// ParentSpanID when rendering a waterfall.
type Span struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	ServiceName  string            `json:"service_name"`
	Operation    string            `json:"operation_name"`
	StartTime    time.Time         `json:"start_time"`
	DurationUS   int64             `json:"duration_us"` // microseconds
	StatusCode   StatusCode        `json:"status_code"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Events       []SpanEvent       `json:"events,omitempty"`
	K8s          K8sMetadata       `json:"k8s"`
}

// EndTime is a convenience derived from StartTime + duration. Handy for
// correlation windows and waterfall layout.
func (s Span) EndTime() time.Time {
	return s.StartTime.Add(time.Duration(s.DurationUS) * time.Microsecond)
}
