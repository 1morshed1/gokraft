// Command loadgen is a synthetic telemetry generator. It plays the role of the
// "instrumented applications" in the architecture: a fleet of fake microservices
// that emit logs, metrics, and traces over OTLP/gRPC to the collector. Use it to
// smoke-test the whole pipeline end to end and to load-test ingestion.
//
// Crucially, loadgen speaks the *real* OTLP protocol using the same generated
// client stubs a genuine OpenTelemetry SDK would — so if loadgen's data flows
// through to the dashboard, any real instrumented app will too. It also produces
// correlated data: each request generates a trace, and the logs for that request
// carry the trace's trace_id, so the correlation endpoint has something to join.
//
//	COLLECTOR_ADDR   default "localhost:4317"
//	SERVICES         number of fake services      (default 10)
//	RATE             requests/sec per service      (default 20)
//	DURATION         how long to run, 0 = forever  (default "0s")
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math"
	mrand "math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/1morshed1/gokraft/internal/config"
	"github.com/1morshed1/gokraft/internal/logging"
)

var serviceNames = []string{
	"api", "checkout", "cart", "payments", "inventory",
	"shipping", "auth", "search", "recommendations", "notifications",
}

func main() {
	log := logging.New("loadgen")

	addr := config.String("COLLECTOR_ADDR", "localhost:4317")
	numServices := config.Int("SERVICES", 10)
	rate := config.Int("RATE", 20)
	duration := config.Duration("DURATION", 0)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("dial collector", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	clients := otlpClients{
		logs:    collogspb.NewLogsServiceClient(conn),
		metrics: colmetricspb.NewMetricsServiceClient(conn),
		traces:  coltracepb.NewTraceServiceClient(conn),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}

	if numServices > len(serviceNames) {
		numServices = len(serviceNames)
	}
	log.Info("loadgen starting", "collector", addr, "services", numServices, "rate_per_service", rate)

	var wg sync.WaitGroup
	for i := 0; i < numServices; i++ {
		wg.Add(1)
		go func(svc string) {
			defer wg.Done()
			runService(ctx, log, clients, svc, rate)
		}(serviceNames[i])
	}
	wg.Wait()
	log.Info("loadgen stopped")
}

type otlpClients struct {
	logs    collogspb.LogsServiceClient
	metrics colmetricspb.MetricsServiceClient
	traces  coltracepb.TraceServiceClient
}

// runService emits telemetry for one fake service at the target rate until ctx
// ends. Each "request" produces: a root span + a child span, a couple of logs
// tagged with the trace id, and a latency metric. ~4% of requests are errors,
// which drives the error-rate alert.
func runService(ctx context.Context, log logger, c otlpClients, svc string, ratePerSec int) {
	if ratePerSec < 1 {
		ratePerSec = 1
	}
	interval := time.Second / time.Duration(ratePerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	rng := mrand.New(mrand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			isError := rng.Float64() < 0.04
			latencyMS := 20 + rng.Float64()*180
			if isError {
				latencyMS += 400 // errors are slow
			}
			traceID := randID(16)
			now := time.Now()

			// Fire the three signals. Errors are logged and swallowed so the
			// generator keeps running if the collector hiccups.
			sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if _, err := c.traces.Export(sctx, buildTrace(svc, traceID, now, latencyMS, isError)); err != nil {
				log.Warn("export traces", "svc", svc, "err", err)
			}
			if _, err := c.logs.Export(sctx, buildLogs(svc, traceID, now, isError)); err != nil {
				log.Warn("export logs", "svc", svc, "err", err)
			}
			if _, err := c.metrics.Export(sctx, buildMetrics(svc, now, latencyMS, isError)); err != nil {
				log.Warn("export metrics", "svc", svc, "err", err)
			}
			cancel()
		}
	}
}

// --- OTLP request builders ---

func resource(svc string) *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		strKV("service.name", svc),
	}}
}

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func buildLogs(svc, traceID string, now time.Time, isError bool) *collogspb.ExportLogsServiceRequest {
	sev, body := "INFO", "handled request ok"
	if isError {
		sev, body = "ERROR", "request failed: upstream timeout"
	}
	rec := &logspb.LogRecord{
		TimeUnixNano: uint64(now.UnixNano()),
		SeverityText: sev,
		Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
		TraceId:      mustHex(traceID),
		Attributes:   []*commonpb.KeyValue{strKV("http.method", "GET"), strKV("http.route", "/"+svc)},
	}
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource:  resource(svc),
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{rec}}},
	}}}
}

func buildMetrics(svc string, now time.Time, latencyMS float64, isError bool) *colmetricspb.ExportMetricsServiceRequest {
	errVal := 0.0
	if isError {
		errVal = 1.0
	}
	dp := func(name string, val float64) *metricspb.Metric {
		return &metricspb.Metric{
			Name: name,
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: uint64(now.UnixNano()),
				Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: val},
			}}}},
		}
	}
	return &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: resource(svc),
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{
			dp("http_request_duration_ms", math.Round(latencyMS*100)/100),
			dp("http_error_rate", errVal),
		}}},
	}}}
}

func buildTrace(svc, traceID string, now time.Time, latencyMS float64, isError bool) *coltracepb.ExportTraceServiceRequest {
	rootSpan := randID(8)
	childSpan := randID(8)
	status := tracepb.Status_STATUS_CODE_OK
	if isError {
		status = tracepb.Status_STATUS_CODE_ERROR
	}
	startNS := uint64(now.UnixNano())
	endNS := startNS + uint64(latencyMS*float64(time.Millisecond))
	childStart := startNS + uint64(5*float64(time.Millisecond))
	childEnd := childStart + uint64(latencyMS*0.6*float64(time.Millisecond))

	spans := []*tracepb.Span{
		{
			TraceId: mustHex(traceID), SpanId: mustHex(rootSpan),
			Name: "GET /" + svc, StartTimeUnixNano: startNS, EndTimeUnixNano: endNS,
			Status: &tracepb.Status{Code: status},
		},
		{
			TraceId: mustHex(traceID), SpanId: mustHex(childSpan), ParentSpanId: mustHex(rootSpan),
			Name: "db.query", StartTimeUnixNano: childStart, EndTimeUnixNano: childEnd,
			Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		},
	}
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   resource(svc),
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
}

// --- id helpers ---

// randID returns a random hex string of n bytes (16 bytes = a trace id, 8 = a
// span id, matching the OTLP conventions).
func randID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// mustHex decodes our hex id string back to the raw bytes OTLP expects on the
// wire. loadgen owns both ends so the strings are always valid hex.
func mustHex(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

// logger is the tiny subset of slog we use, so runService is easy to read.
type logger interface {
	Info(string, ...any)
	Warn(string, ...any)
}

var _ logger = logging.New("x")
