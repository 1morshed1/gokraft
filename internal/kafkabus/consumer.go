package kafkabus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer reads JSON records from a topic as part of a consumer group.
//
// Consumer groups are the mechanism behind Kafka's horizontal scaling and
// at-least-once delivery:
//
//   - All consumers sharing a GroupID cooperatively divide the topic's
//     partitions among themselves. Run three logwriter replicas with the same
//     group and Kafka hands each ~1/3 of the partitions. Add a fourth and Kafka
//     rebalances automatically.
//
//   - Kafka tracks a committed offset per group per partition. We commit only
//     *after* a batch is durably written to the database (see the writers). If a
//     writer crashes mid-batch, the uncommitted messages are redelivered to
//     another group member — no data loss. The cost is possible duplicates on
//     retry, which is why our database writes are designed to be idempotent
//     (upserts / dedup keys).
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer joins groupID on topic. MinBytes/MaxBytes let the broker wait for
// a worthwhile chunk before responding, trading a little latency for far fewer
// round-trips under load. We disable auto-commit by committing explicitly after
// a successful write (CommitMessages), which is what makes at-least-once work.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 10e3, // 10KB: wait for at least this much
			MaxBytes: 10e6, // 10MB: cap a single fetch
		}),
	}
}

// FetchBatch reads up to max messages, decoding each JSON value into a T. It
// returns the decoded records alongside the raw kafka.Messages so the caller can
// commit their offsets after a successful downstream write. FetchMessage blocks
// until at least one message is available or ctx is cancelled; we then greedily
// pull more without blocking to assemble a batch for efficient bulk insert.
func FetchBatch[T any](ctx context.Context, c *Consumer, max int) ([]T, []kafka.Message, error) {
	// Block for the first message so we don't spin when the topic is idle.
	first, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, nil, err
	}

	raw := make([]kafka.Message, 0, max)
	recs := make([]T, 0, max)

	decode := func(m kafka.Message) {
		var rec T
		if err := json.Unmarshal(m.Value, &rec); err != nil {
			// Poison message: log-worthy in a real system. We still record the
			// raw message so its offset gets committed and we don't loop on it.
			raw = append(raw, m)
			return
		}
		recs = append(recs, rec)
		raw = append(raw, m)
	}
	decode(first)

	// Greedily assemble a batch: keep pulling messages, but give each follow-up
	// fetch only a short deadline. As long as messages keep arriving we keep
	// batching; the moment the topic goes quiet for `drainWindow` we stop and
	// return what we have. This bounds the extra latency a record can incur
	// waiting for batch-mates to at most drainWindow.
	const drainWindow = 5 * time.Millisecond
	for len(raw) < max {
		dctx, cancel := context.WithTimeout(ctx, drainWindow)
		m, err := c.reader.FetchMessage(dctx)
		cancel()
		if err != nil {
			break // deadline hit (topic quiet) or ctx cancelled
		}
		decode(m)
	}
	return recs, raw, nil
}

// Commit advances the group's committed offset past these messages. Call it only
// after the batch is safely persisted downstream.
func (c *Consumer) Commit(ctx context.Context, msgs []kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka commit: %w", err)
	}
	return nil
}

// Close closes the underlying reader (also commits any pending offsets).
func (c *Consumer) Close() error { return c.reader.Close() }
