// Package writer holds the loop every storage writer shares: pull a batch from
// Kafka, persist it, and only then commit the offset. The three writers
// (logwriter, metricwriter, tracewriter) differ only in the record type and the
// store they call, so we express the common loop once with generics and inject
// the insert function.
//
// The ordering — insert first, commit second — is the entire correctness story
// for at-least-once delivery:
//
//   - If the insert fails, we do NOT commit. The same messages are redelivered
//     and retried. Nothing is lost.
//   - If the insert succeeds but the process crashes before committing, the
//     messages are redelivered and inserted again. This is why every store's
//     insert is idempotent (ClickHouse tolerates dup log rows; Postgres traces
//     use ON CONFLICT DO NOTHING).
//
// Committing before inserting would give at-most-once (data loss on crash).
// Inserting before committing gives at-least-once (dup-on-crash, which we
// neutralize). We choose the latter because for observability, losing telemetry
// is worse than storing a rare duplicate.
package writer

import (
	"context"
	"log/slog"
	"time"

	"github.com/1morshed1/gokraft/internal/kafkabus"
)

// InsertFunc persists a batch of decoded records to a store.
type InsertFunc[T any] func(ctx context.Context, records []T) error

// Run consumes from the topic and writes batches until ctx is cancelled. It owns
// the retry policy for transient database errors: on failure it backs off and
// retries the *same* batch without committing, so no data is skipped.
func Run[T any](ctx context.Context, log *slog.Logger, c *kafkabus.Consumer, batchMax int, insert InsertFunc[T]) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		recs, raw, err := kafkabus.FetchBatch[T](ctx, c, batchMax)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // clean shutdown
			}
			log.Error("fetch batch", "err", err)
			time.Sleep(time.Second)
			continue
		}
		if len(raw) == 0 {
			continue
		}

		// Persist with bounded exponential backoff. We keep retrying a transient
		// failure rather than dropping the batch; a persistent failure will keep
		// the consumer stuck here (visible in logs) instead of silently losing
		// data — the right failure mode for telemetry.
		if err := insertWithRetry(ctx, log, insert, recs); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		// Only now is it safe to advance the offset.
		if err := c.Commit(ctx, raw); err != nil {
			log.Error("commit offsets", "err", err)
			// Not fatal: the batch is already persisted. Worst case those
			// messages are redelivered and idempotently re-inserted.
		}
	}
}

func insertWithRetry[T any](ctx context.Context, log *slog.Logger, insert InsertFunc[T], recs []T) error {
	if len(recs) == 0 {
		return nil
	}
	backoff := 500 * time.Millisecond
	const maxBackoff = 15 * time.Second
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := insert(ctx, recs)
		if err == nil {
			return nil
		}
		log.Error("insert batch failed; will retry", "attempt", attempt, "count", len(recs), "err", err, "backoff", backoff.String())
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
