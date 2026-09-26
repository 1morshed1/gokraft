package collector

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/1morshed1/gokraft/internal/model"
)

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// TestLogsFromOTLP checks the resource→scope→record flattening, service-name
// extraction from the resource, severity normalization, and hex id encoding.
func TestLogsFromOTLP(t *testing.T) {
	req := []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", "checkout")}},
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano: 1_700_000_000_000_000_000,
				SeverityText: "warning", // should normalize to WARN
				Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "disk almost full"}},
				TraceId:      []byte{0xde, 0xad, 0xbe, 0xef},
				Attributes:   []*commonpb.KeyValue{strAttr("region", "us-east")},
			}},
		}},
	}}

	logs := LogsFromOTLP(req)
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}
	l := logs[0]
	if l.ServiceName != "checkout" {
		t.Errorf("service name = %q, want checkout", l.ServiceName)
	}
	if l.Severity != "WARN" {
		t.Errorf("severity = %q, want WARN (normalized from 'warning')", l.Severity)
	}
	if l.Body != "disk almost full" {
		t.Errorf("body = %q", l.Body)
	}
	if l.TraceID != "deadbeef" {
		t.Errorf("trace id = %q, want deadbeef", l.TraceID)
	}
	if l.Attributes["region"] != "us-east" {
		t.Errorf("attribute lost: %v", l.Attributes)
	}
}

// TestMetricsFromOTLP checks that a Gauge data point becomes one Metric row and
// that AsDouble values are read correctly.
func TestMetricsFromOTLP(t *testing.T) {
	req := []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", "api")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "cpu_usage",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{{
						TimeUnixNano: 1_700_000_000_000_000_000,
						Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 0.73},
						Attributes:   []*commonpb.KeyValue{strAttr("core", "0")},
					}},
				}},
			}},
		}},
	}}

	metrics := MetricsFromOTLP(req)
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}
	m := metrics[0]
	if m.Name != "cpu_usage" || m.ServiceName != "api" || m.Value != 0.73 {
		t.Errorf("unexpected metric: %+v", m)
	}
	if m.Tags["core"] != "0" {
		t.Errorf("tag lost: %v", m.Tags)
	}
}

// TestHistogramDecomposition checks that a histogram becomes _count and _sum
// companion series.
func TestHistogramDecomposition(t *testing.T) {
	sum := 12.5
	req := []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", "api")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Metrics: []*metricspb.Metric{{
				Name: "latency",
				Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
					DataPoints: []*metricspb.HistogramDataPoint{{
						TimeUnixNano: 1_700_000_000_000_000_000,
						Count:        4,
						Sum:          &sum,
					}},
				}},
			}},
		}},
	}}

	metrics := MetricsFromOTLP(req)
	names := map[string]float64{}
	for _, m := range metrics {
		names[m.Name] = m.Value
	}
	if names["latency_count"] != 4 {
		t.Errorf("latency_count = %v, want 4", names["latency_count"])
	}
	if names["latency_sum"] != 12.5 {
		t.Errorf("latency_sum = %v, want 12.5", names["latency_sum"])
	}
}

// TestTracesFromOTLP checks span flattening, duration computation, and status
// mapping.
func TestTracesFromOTLP(t *testing.T) {
	const start = uint64(1_700_000_000_000_000_000)
	const end = start + 2_000_000 // +2ms in nanoseconds
	req := []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", "db")}},
		ScopeSpans: []*tracepb.ScopeSpans{{
			Spans: []*tracepb.Span{{
				TraceId:           []byte{0x01, 0x02},
				SpanId:            []byte{0x0a},
				ParentSpanId:      []byte{0x0b},
				Name:              "SELECT",
				StartTimeUnixNano: start,
				EndTimeUnixNano:   end,
				Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR},
			}},
		}},
	}}

	spans := TracesFromOTLP(req)
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	s := spans[0]
	if s.DurationUS != 2000 {
		t.Errorf("duration = %d us, want 2000", s.DurationUS)
	}
	if s.StatusCode != model.StatusError {
		t.Errorf("status = %d, want StatusError", s.StatusCode)
	}
	if s.Operation != "SELECT" || s.ServiceName != "db" {
		t.Errorf("unexpected span: %+v", s)
	}
}
