// Command logwriter consumes the `logs` Kafka topic and bulk-inserts into
// ClickHouse. Run several replicas with the same consumer group to scale
// horizontally; Kafka divides the partitions among them.
//
//	KAFKA_BROKERS      default "localhost:9092"
//	KAFKA_GROUP        default "logwriter"
//	CLICKHOUSE_ADDR    default "localhost:9000"
//	CLICKHOUSE_DB      default "gokraft"
//	CLICKHOUSE_USER    default "default"
//	CLICKHOUSE_PASS    default ""
//	WRITE_BATCH_MAX    default 5000
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
	"github.com/1morshed1/gokraft/internal/storage/clickhouse"
	"github.com/1morshed1/gokraft/internal/writer"
)

func main() {
	log := logging.New("logwriter")

	store, err := clickhouse.Open(
		config.String("CLICKHOUSE_ADDR", "localhost:9000"),
		config.String("CLICKHOUSE_DB", "gokraft"),
		config.String("CLICKHOUSE_USER", "default"),
		config.String("CLICKHOUSE_PASS", ""),
	)
	if err != nil {
		log.Error("connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	consumer := kafkabus.NewConsumer(
		config.StringSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		kafkabus.TopicLogs,
		config.String("KAFKA_GROUP", "logwriter"),
	)
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("logwriter started")
	err = writer.Run[model.Log](ctx, log, consumer, config.Int("WRITE_BATCH_MAX", 5000), store.InsertBatch)
	log.Info("logwriter stopped", "reason", err)
}
