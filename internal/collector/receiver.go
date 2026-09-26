package collector

// receiver.go implements the gRPC server side of the OTLP protocol. The
// OpenTelemetry project ships the service definitions (LogsService,
// MetricsService, TraceService) as generated Go interfaces; we implement them.
// Any application instrumented with an OpenTelemetry SDK — in any language —
// can point its OTLP/gRPC exporter at us and it Just Works, because we speak the
// standard. That interoperability is the whole reason to accept OTLP rather than
// invent a bespoke ingestion API.
//
// Flow for every incoming export request:
//
//	OTLP request ──▶ convert to []model.X ──▶ enrich with K8s metadata
//	            ──▶ hand each record to the per-signal batcher ──▶ (batcher flushes to Kafka)
//
// The handlers return as soon as records are enqueued in the batcher; they do
// NOT wait for the Kafka write. This keeps ingest latency low and is safe
// because the batcher applies backpressure (Add blocks) if Kafka falls behind,
// which propagates back to the sender as slower Export calls — exactly the
// signal a well-behaved OTLP client uses to throttle.
//
// A Go subtlety worth noting: all three OTLP services declare a method literally
// named `Export`. Go has no method overloading, so one struct cannot implement
// all three — a type may have only a single method called `Export`. We therefore
// use three small receiver types, one per signal, each embedding the matching
// generated "Unimplemented*Server" for forward compatibility.

import (
	"context"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/peer"

	"github.com/1morshed1/gokraft/internal/batch"
	"github.com/1morshed1/gokraft/internal/enrich"
	"github.com/1morshed1/gokraft/internal/model"
)

// Sinks are the three per-signal batchers the receiver feeds. The receiver does
// not know or care that they flush to Kafka — it only calls Add. This keeps the
// gRPC layer decoupled from the transport layer (and makes it testable with
// in-memory batchers).
type Sinks struct {
	Logs    *batch.Batcher[model.Log]
	Metrics *batch.Batcher[model.Metric]
	Traces  *batch.Batcher[model.Span]
}

// sourceAddr extracts the caller's network address from the gRPC context. In a
// cluster this is the sending pod's IP, which an APIServerEnricher would use to
// resolve pod identity.
func sourceAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}

// --- Logs ---

type logsReceiver struct {
	collogspb.UnimplementedLogsServiceServer
	sink     *batch.Batcher[model.Log]
	enricher enrich.Enricher
}

func (r *logsReceiver) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	meta := r.enricher.Metadata(sourceAddr(ctx))
	logs := LogsFromOTLP(req.ResourceLogs)
	for i := range logs {
		logs[i].K8s = meta
		r.sink.Add(logs[i])
	}
	// OTLP has a partial-success field for rejecting some records; we accept
	// everything, so an empty response signals full success.
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// --- Metrics ---

type metricsReceiver struct {
	colmetricspb.UnimplementedMetricsServiceServer
	sink     *batch.Batcher[model.Metric]
	enricher enrich.Enricher
}

func (r *metricsReceiver) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	meta := r.enricher.Metadata(sourceAddr(ctx))
	metrics := MetricsFromOTLP(req.ResourceMetrics)
	for i := range metrics {
		metrics[i].K8s = meta
		r.sink.Add(metrics[i])
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// --- Traces ---

type tracesReceiver struct {
	coltracepb.UnimplementedTraceServiceServer
	sink     *batch.Batcher[model.Span]
	enricher enrich.Enricher
}

func (r *tracesReceiver) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	meta := r.enricher.Metadata(sourceAddr(ctx))
	spans := TracesFromOTLP(req.ResourceSpans)
	for i := range spans {
		spans[i].K8s = meta
		r.sink.Add(spans[i])
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}
