// Package kafkabus wraps the segmentio/kafka-go client with the small, opinionated
// surface gokraft actually needs: a Producer that publishes batches of model
// records as JSON, and a Consumer (in consumer.go) that reads them back.
//
// Why Kafka sits in the middle of the pipeline (the "Why Kafka?" lesson):
//
//   - Decoupling. The collector's job is to accept telemetry as fast as it
//     arrives. The writers' job is to persist it as fast as the databases allow.
//     Those two rates are almost never equal. Without a buffer, a slow database
//     would force the collector to drop data or block the sending apps. Kafka is
//     that buffer — a durable, replayable log that absorbs bursts.
//
//   - Independent scaling and isolation. Three topics (logs, metrics, traces)
//     mean a flood of logs cannot starve metric ingestion, and each writer can be
//     scaled (more consumer instances in the same group) independently.
//
//   - Replayability. Because Kafka retains messages, a writer can crash, be
//     fixed, and resume from its last committed offset without losing data. This
//     is the property that turns "at-most-once" into "at-least-once" delivery.
//
// We serialize records as JSON. JSON is inspectable (you can `kafka-console-
// consumer` a topic and read it), which is invaluable while learning. A
// production system would likely use protobuf/Avro with a schema registry for
// compactness and schema evolution; the trade-off is opacity.
package kafkabus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Topic names. Kept as constants so producer and consumer cannot disagree.
const (
	TopicLogs    = "logs"
	TopicMetrics = "metrics"
	TopicTraces  = "traces"
)

// Producer publishes JSON-encoded records to a single topic. kafka-go's Writer
// is safe for concurrent use and does its own internal batching/retrying, so we
// keep one Writer per topic for the process lifetime.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Producer for one topic. RequireOne acknowledgement
// balances durability against throughput: we wait for the leader to persist the
// message but not every replica. For a learning platform that is the right
// default; bump to RequireAll for stronger durability at the cost of latency.
func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // partition by key so a service's data is ordered
			RequiredAcks: kafka.RequireOne,
			Async:        false, // synchronous: WriteMessages returns after the broker acks
			BatchSize:    1000,  // kafka-go groups writes; our batcher already bulks upstream
			// kafka-go does NOT request topic creation by default, so even with
			// the broker's auto.create.topics.enable=true a first produce to a
			// missing topic fails with UnknownTopicOrPartition. Enabling this
			// makes the writer send the metadata that triggers creation — handy
			// for dev. In production you'd pre-create topics with an explicit
			// partition count and disable this.
			AllowAutoTopicCreation: true,
		},
	}
}

// WriteJSON marshals each record to JSON and writes them as one Kafka batch.
// keyOf derives the partition key for a record so related records land on the
// same partition (and thus stay ordered) — e.g. key by service name. A nil
// keyOf means round-robin-ish distribution.
func WriteJSON[T any](ctx context.Context, p *Producer, records []T, keyOf func(T) string) error {
	if len(records) == 0 {
		return nil
	}
	msgs := make([]kafka.Message, 0, len(records))
	for _, rec := range records {
		payload, err := json.Marshal(rec)
		if err != nil {
			// One bad record should not sink the whole batch; skip it. In a
			// real system this would increment a "dropped" metric.
			continue
		}
		var key []byte
		if keyOf != nil {
			key = []byte(keyOf(rec))
		}
		msgs = append(msgs, kafka.Message{Key: key, Value: payload})
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka write to %s: %w", p.writer.Topic, err)
	}
	return nil
}

// Close flushes and closes the underlying writer.
func (p *Producer) Close() error { return p.writer.Close() }
