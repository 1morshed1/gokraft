// Command metricwriter consumes the `metrics` topic and bulk-loads into
// TimescaleDB via COPY.
//
//	KAFKA_BROKERS   default "localhost:9092"
//	KAFKA_GROUP     default "metricwriter"
//	TIMESCALE_DSN   default "postgres://gokraft:gokraft@localhost:5433/metrics"
//	WRITE_BATCH_MAX default 5000
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/1morshed1/gokraft/internal/config"
	"github.com/1morshed1/gokraft/internal/kafkabus"
	"github.com/1morshed1/gokraft/internal/logging"
	"github.com/1morshed1/gokraft/internal/model"
	"github.com/1morshed1/gokraft/internal/storage/timescale"
	"github.com/1morshed1/gokraft/internal/writer"
)

func main() {
	log := logging.New("metricwriter")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := timescale.Open(ctx, config.String("TIMESCALE_DSN", "postgres://gokraft:gokraft@localhost:5433/metrics"))
	if err != nil {
		log.Error("connect timescale", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	consumer := kafkabus.NewConsumer(
		config.StringSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		kafkabus.TopicMetrics,
		config.String("KAFKA_GROUP", "metricwriter"),
	)
	defer consumer.Close()

	log.Info("metricwriter started")
	err = writer.Run[model.Metric](ctx, log, consumer, config.Int("WRITE_BATCH_MAX", 5000), store.InsertBatch)
	log.Info("metricwriter stopped", "reason", err)
}
