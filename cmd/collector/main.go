// Command collector is the OTLP ingestion service. It listens for OpenTelemetry
// logs, metrics, and traces over gRPC, enriches them with Kubernetes metadata,
// batches them, and produces them to Kafka.
//
// Run it with defaults (talks to Kafka at localhost:9092, listens on :4317):
//
//	go run ./cmd/collector
//
// Every setting is an environment variable (see internal/config). The most
// useful ones:
//
//	COLLECTOR_ADDR   gRPC bind address        (default ":4317", the OTLP port)
//	KAFKA_BROKERS    comma-separated brokers  (default "localhost:9092")
//	BATCH_SIZE       records per flush        (default 1000)
//	BATCH_DELAY      max wait per batch        (default "500ms")
//	K8S_POD_NAME/K8S_NAMESPACE/K8S_NODE_NAME  Downward API enrichment
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/1morshed1/gokraft/internal/collector"
	"github.com/1morshed1/gokraft/internal/config"
	"github.com/1morshed1/gokraft/internal/enrich"
	"github.com/1morshed1/gokraft/internal/logging"
)

func main() {
	log := logging.New("collector")

	cfg := collector.Config{
		ListenAddr:  config.String("COLLECTOR_ADDR", ":4317"),
		Brokers:     config.StringSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		BatchSize:   config.Int("BATCH_SIZE", 1000),
		BatchDelay:  config.Duration("BATCH_DELAY", 500e6), // 500ms in ns
		BatchBuffer: config.Int("BATCH_BUFFER", 10000),
		Enricher:    enrich.NewStaticFromEnv(os.Getenv),
		Logger:      log,
	}

	srv := collector.New(cfg)

	// Serve in a goroutine so main can wait for a shutdown signal. Kubernetes
	// (and Ctrl-C locally) send SIGTERM/SIGINT; we translate that into a clean
	// drain so no buffered telemetry is lost on a rolling deploy.
	go func() {
		if err := srv.Serve(); err != nil {
			log.Error("gRPC server stopped", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	srv.Shutdown()
}
