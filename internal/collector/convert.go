package collector

// This file converts OpenTelemetry's OTLP protobuf structures into gokraft's
// normalized model types. It is the single boundary where the foreign wire
// format is translated into our own vocabulary; nothing downstream imports OTLP.
//
// OTLP is deeply nested for a reason: it amortizes repeated context. A single
// export request carries a list of "resources" (each identifying a service),
// each resource carries a list of "scopes" (instrumentation libraries), and
// each scope carries the actual records. The service name, K8s attributes, etc.
// live once on the resource rather than being repeated on every log line. Our
// job here is to *flatten* that tree: walk resource → scope → record and emit
// one flat model struct per record, copying the resource-level context down
// onto each one.

import (
	"encoding/hex"
	"strconv"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/1morshed1/gokraft/internal/model"
)

// serviceNameKey is the OTLP semantic-convention attribute that identifies the
// emitting service. It lives on the Resource.
const serviceNameKey = "service.name"

// anyValueToString renders an OTLP AnyValue (a oneof over string/bool/int/
// double/bytes/array/kvlist) as a plain string. We store attributes as
// map[string]string everywhere for simplicity; richer typing is a possible
// future refinement but not worth the complexity for a learning platform.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(val.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	default:
		// Arrays and kvlists are rare on the fields we care about; render a
		// stable placeholder rather than dropping the key entirely.
		return v.String()
	}
}

// attrsToMap flattens a slice of OTLP KeyValue into a Go map.
func attrsToMap(kvs []*commonpb.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		m[kv.Key] = anyValueToString(kv.Value)
	}
	return m
}

// serviceName pulls service.name out of a resource's attributes, defaulting to
// "unknown_service" (matching the OTel SDK convention) when absent.
func serviceName(attrs []*commonpb.KeyValue) string {
	for _, kv := range attrs {
		if kv.Key == serviceNameKey {
			return anyValueToString(kv.Value)
		}
	}
	return "unknown_service"
}

// unixNanoToTime converts an OTLP timestamp (nanoseconds since Unix epoch, the
// unit OTLP uses throughout) to a time.Time. Zero maps to the zero time so
// callers can detect "unset".
func unixNanoToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(ns)).UTC()
}

// hexID renders a trace/span id (raw bytes on the wire) as the lowercase hex
// string everyone expects to see in a UI and to join on. Empty stays empty.
func hexID(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

// LogsFromOTLP flattens ExportLogsServiceRequest resource logs into model.Log.
func LogsFromOTLP(resourceLogs []*logspb.ResourceLogs) []model.Log {
	var out []model.Log
	for _, rl := range resourceLogs {
		var svc string
		if rl.Resource != nil {
			svc = serviceName(rl.Resource.Attributes)
		}
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				// OTLP has both TimeUnixNano (event time) and
				// ObservedTimeUnixNano (collection time). Prefer event time;
				// fall back to observed if the app didn't set it.
				ts := unixNanoToTime(lr.TimeUnixNano)
				if ts.IsZero() {
					ts = unixNanoToTime(lr.ObservedTimeUnixNano)
				}
				out = append(out, model.Log{
					Timestamp:   ts,
					TraceID:     hexID(lr.TraceId),
					SpanID:      hexID(lr.SpanId),
					Severity:    normalizeSeverity(lr.SeverityText),
					ServiceName: svc,
					Body:        anyValueToString(lr.Body),
					Attributes:  attrsToMap(lr.Attributes),
				})
			}
		}
	}
	return out
}

// normalizeSeverity coerces the free-form OTLP SeverityText into our small
// fixed set so the ClickHouse LowCardinality(String) column stays low
// cardinality. Unknown values pass through uppercased.
func normalizeSeverity(s string) string {
	switch up := upper(s); up {
	case "TRACE", "DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL":
		if up == "WARNING" {
			return "WARN"
		}
		return up
	case "":
		return "INFO"
	default:
		return up
	}
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

// MetricsFromOTLP flattens OTLP metrics into model.Metric rows. OTLP metrics are
// a oneof over Gauge/Sum/Histogram/ExponentialHistogram/Summary. We handle the
// two overwhelmingly common types fully (Gauge and Sum, both NumberDataPoints)
// and decompose Histograms into _count and _sum companion series, which is
// enough to chart latency/throughput. Richer histogram bucket handling is left
// as a documented exercise.
func MetricsFromOTLP(resourceMetrics []*metricspb.ResourceMetrics) []model.Metric {
	var out []model.Metric
	for _, rm := range resourceMetrics {
		var svc string
		if rm.Resource != nil {
			svc = serviceName(rm.Resource.Attributes)
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				out = appendMetric(out, svc, m)
			}
		}
	}
	return out
}

func appendMetric(out []model.Metric, svc string, m *metricspb.Metric) []model.Metric {
	switch data := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range data.Gauge.DataPoints {
			out = append(out, numberPointToMetric(svc, m.Name, dp))
		}
	case *metricspb.Metric_Sum:
		for _, dp := range data.Sum.DataPoints {
			out = append(out, numberPointToMetric(svc, m.Name, dp))
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range data.Histogram.DataPoints {
			ts := unixNanoToTime(dp.TimeUnixNano)
			tags := attrsToMap(dp.Attributes)
			out = append(out,
				model.Metric{Timestamp: ts, Name: m.Name + "_count", ServiceName: svc, Value: float64(dp.Count), Tags: tags},
			)
			if dp.Sum != nil {
				out = append(out,
					model.Metric{Timestamp: ts, Name: m.Name + "_sum", ServiceName: svc, Value: *dp.Sum, Tags: tags},
				)
			}
		}
	}
	return out
}

func numberPointToMetric(svc, name string, dp *metricspb.NumberDataPoint) model.Metric {
	// A NumberDataPoint's value is itself a oneof: double or int.
	var v float64
	switch val := dp.Value.(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		v = val.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		v = float64(val.AsInt)
	}
	return model.Metric{
		Timestamp:   unixNanoToTime(dp.TimeUnixNano),
		Name:        name,
		ServiceName: svc,
		Value:       v,
		Tags:        attrsToMap(dp.Attributes),
	}
}

// TracesFromOTLP flattens OTLP spans into model.Span.
func TracesFromOTLP(resourceSpans []*tracepb.ResourceSpans) []model.Span {
	var out []model.Span
	for _, rs := range resourceSpans {
		var svc string
		if rs.Resource != nil {
			svc = serviceName(rs.Resource.Attributes)
		}
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				start := unixNanoToTime(sp.StartTimeUnixNano)
				end := unixNanoToTime(sp.EndTimeUnixNano)
				durUS := int64(0)
				if !start.IsZero() && !end.IsZero() {
					durUS = end.Sub(start).Microseconds()
				}
				out = append(out, model.Span{
					TraceID:      hexID(sp.TraceId),
					SpanID:       hexID(sp.SpanId),
					ParentSpanID: hexID(sp.ParentSpanId),
					ServiceName:  svc,
					Operation:    sp.Name,
					StartTime:    start,
					DurationUS:   durUS,
					StatusCode:   statusFromOTLP(sp.Status),
					Attributes:   attrsToMap(sp.Attributes),
					Events:       eventsFromOTLP(sp.Events),
				})
			}
		}
	}
	return out
}

func statusFromOTLP(st *tracepb.Status) model.StatusCode {
	if st == nil {
		return model.StatusUnset
	}
	switch st.Code {
	case tracepb.Status_STATUS_CODE_OK:
		return model.StatusOK
	case tracepb.Status_STATUS_CODE_ERROR:
		return model.StatusError
	default:
		return model.StatusUnset
	}
}

func eventsFromOTLP(evs []*tracepb.Span_Event) []model.SpanEvent {
	if len(evs) == 0 {
		return nil
	}
	out := make([]model.SpanEvent, 0, len(evs))
	for _, e := range evs {
		out = append(out, model.SpanEvent{
			Timestamp:  unixNanoToTime(e.TimeUnixNano),
			Name:       e.Name,
			Attributes: attrsToMap(e.Attributes),
		})
	}
	return out
}
