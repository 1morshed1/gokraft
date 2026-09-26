// Command tracewriter consumes the `traces` topic and inserts spans into
// PostgreSQL (idempotently, via ON CONFLICT DO NOTHING).
//
//	KAFKA_BROKERS   default "localhost:9092"
//	KAFKA_GROUP     default "tracewriter"
//	POSTGRES_DSN    default "postgres://gokraft:gokraft@localhost:5434/traces"
//	WRITE_BATCH_MAX default 2000
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
	"github.com/1morshed1/gokraft/internal/storage/tracestore"
	"github.com/1morshed1/gokraft/internal/writer"
)

func main() {
	log := logging.New("tracewriter")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := tracestore.Open(ctx, config.String("POSTGRES_DSN", "postgres://gokraft:gokraft@localhost:5434/traces"))
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	consumer := kafkabus.NewConsumer(
		config.StringSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		kafkabus.TopicTraces,
		config.String("KAFKA_GROUP", "tracewriter"),
	)
	defer consumer.Close()

	log.Info("tracewriter started")
	err = writer.Run[model.Span](ctx, log, consumer, config.Int("WRITE_BATCH_MAX", 2000), store.InsertBatch)
	log.Info("tracewriter stopped", "reason", err)
}
