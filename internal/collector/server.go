package collector

// server.go assembles the collector: it constructs the three per-signal
// batchers (each flushing to a Kafka topic), registers the OTLP gRPC services,
// and manages a clean startup/shutdown lifecycle. This is the "composition root"
// of the collector binary — the one place where all the decoupled pieces
// (conversion, enrichment, batching, Kafka) are wired together.

import (
	"context"
	"log/slog"
	"net"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"

	"github.com/1morshed1/gokraft/internal/batch"
	"github.com/1morshed1/gokraft/internal/enrich"
	"github.com/1morshed1/gokraft/internal/kafkabus"
	"github.com/1morshed1/gokraft/internal/model"
)

// Config controls the collector's batching and connectivity. Sensible defaults
// come from the plan: flush at 1,000 records or every 500ms, whichever first.
type Config struct {
	ListenAddr  string        // gRPC bind address, e.g. ":4317" (the OTLP default port)
	Brokers     []string      // Kafka brokers
	BatchSize   int           // records per batch before a forced flush
	BatchDelay  time.Duration // max time a record waits before flush
	BatchBuffer int           // intake buffer size (backpressure threshold)
	Enricher    enrich.Enricher
	Logger      *slog.Logger
}

// Server owns the gRPC server, the Kafka producers, and the batchers.
type Server struct {
	cfg   Config
	log   *slog.Logger
	grpc  *grpc.Server
	sinks Sinks

	logProducer    *kafkabus.Producer
	metricProducer *kafkabus.Producer
	traceProducer  *kafkabus.Producer
}

// New constructs the collector server: producers, batchers wired to flush into
// those producers, and a gRPC server with the three OTLP services registered.
func New(cfg Config) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	logProd := kafkabus.NewProducer(cfg.Brokers, kafkabus.TopicLogs)
	metricProd := kafkabus.NewProducer(cfg.Brokers, kafkabus.TopicMetrics)
	traceProd := kafkabus.NewProducer(cfg.Brokers, kafkabus.TopicTraces)

	// Each batcher's flush function marshals the batch and publishes it to the
	// matching topic. We partition by service name so that all records from one
	// service land on the same partition and therefore stay time-ordered — which
	// matters for correctly rendering a service's logs and for trace assembly.
	logBatcher := batch.New(cfg.BatchSize, cfg.BatchDelay, cfg.BatchBuffer,
		func(ctx context.Context, recs []model.Log) {
			if err := kafkabus.WriteJSON(ctx, logProd, recs, func(l model.Log) string { return l.ServiceName }); err != nil {
				log.Error("flush logs to kafka", "err", err, "count", len(recs))
			}
		})
	metricBatcher := batch.New(cfg.BatchSize, cfg.BatchDelay, cfg.BatchBuffer,
		func(ctx context.Context, recs []model.Metric) {
			if err := kafkabus.WriteJSON(ctx, metricProd, recs, func(m model.Metric) string { return m.ServiceName }); err != nil {
				log.Error("flush metrics to kafka", "err", err, "count", len(recs))
			}
		})
	traceBatcher := batch.New(cfg.BatchSize, cfg.BatchDelay, cfg.BatchBuffer,
		func(ctx context.Context, recs []model.Span) {
			// Partition traces by trace_id so all spans of one trace stay
			// together and ordered — the writer and query layer assemble whole
			// traces, so co-locating their spans is the right call.
			if err := kafkabus.WriteJSON(ctx, traceProd, recs, func(s model.Span) string { return s.TraceID }); err != nil {
				log.Error("flush traces to kafka", "err", err, "count", len(recs))
			}
		})

	sinks := Sinks{Logs: logBatcher, Metrics: metricBatcher, Traces: traceBatcher}

	gs := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(gs, &logsReceiver{sink: logBatcher, enricher: cfg.Enricher})
	colmetricspb.RegisterMetricsServiceServer(gs, &metricsReceiver{sink: metricBatcher, enricher: cfg.Enricher})
	coltracepb.RegisterTraceServiceServer(gs, &tracesReceiver{sink: traceBatcher, enricher: cfg.Enricher})

	return &Server{
		cfg:            cfg,
		log:            log,
		grpc:           gs,
		sinks:          sinks,
		logProducer:    logProd,
		metricProducer: metricProd,
		traceProducer:  traceProd,
	}
}

// Serve binds the listener and blocks serving gRPC until the server is stopped.
func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	s.log.Info("collector listening for OTLP/gRPC", "addr", s.cfg.ListenAddr)
	return s.grpc.Serve(lis)
}

// Shutdown drains everything in the correct order so no telemetry is lost:
//  1. Stop accepting new gRPC requests (GracefulStop lets in-flight ones finish).
//  2. Close the batchers, which flushes any buffered records to Kafka.
//  3. Close the Kafka producers, which flushes their internal buffers.
//
// Order matters: if we closed producers before batchers, the final flush would
// have nowhere to go.
func (s *Server) Shutdown() {
	s.log.Info("collector shutting down: stopping gRPC")
	s.grpc.GracefulStop()

	s.log.Info("flushing batchers")
	s.sinks.Logs.Close()
	s.sinks.Metrics.Close()
	s.sinks.Traces.Close()

	s.log.Info("closing kafka producers")
	_ = s.logProducer.Close()
	_ = s.metricProducer.Close()
	_ = s.traceProducer.Close()
	s.log.Info("collector shutdown complete")
}
