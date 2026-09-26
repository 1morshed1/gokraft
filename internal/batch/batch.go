// Package batch implements a generic size-or-time batcher.
//
// The problem it solves: the collector receives telemetry one record (or one
// small OTLP request) at a time, but writing one Kafka message per log line is
// wildly inefficient — the per-message overhead (network round-trips, broker
// bookkeeping) dwarfs the payload. The fix is to accumulate records and flush
// them in bulk when *either* of two thresholds is hit:
//
//   - the batch reaches maxSize records, OR
//   - maxDelay has elapsed since the first record in the current batch.
//
// The size trigger keeps throughput high under load; the time trigger bounds
// latency when traffic is light (a lone log line should not sit in a buffer
// forever). This "size OR time, whichever comes first" pattern appears
// everywhere in data pipelines — Kafka producers, log shippers, database bulk
// loaders — so it is worth understanding deeply.
//
// The implementation is generic over the record type T (Go generics, 1.18+)
// and concurrency-safe: callers push from many goroutines while an internal
// goroutine owns the flush timing. Flushing is delegated to a caller-provided
// function, so this package knows nothing about Kafka — it is pure buffering
// logic and is unit-tested without any infrastructure.
package batch

import (
	"context"
	"sync"
	"time"
)

// FlushFunc is called with a non-empty slice of records to be flushed. It runs
// on the batcher's internal goroutine, so a slow flush applies natural
// backpressure (pushes block once the channel fills). The slice must not be
// retained by the callee after it returns; the batcher may reuse the backing
// array.
type FlushFunc[T any] func(ctx context.Context, records []T)

// Batcher accumulates records and flushes them by size or elapsed time.
type Batcher[T any] struct {
	maxSize  int
	maxDelay time.Duration
	flush    FlushFunc[T]

	in   chan T
	done chan struct{}
	wg   sync.WaitGroup
}

// New creates and starts a Batcher. maxSize is the record count that forces a
// flush; maxDelay is the maximum time a record waits before being flushed.
// bufferSize sizes the intake channel — when it fills (because flush can't keep
// up) Add blocks, which is the backpressure signal we want.
func New[T any](maxSize int, maxDelay time.Duration, bufferSize int, flush FlushFunc[T]) *Batcher[T] {
	b := &Batcher[T]{
		maxSize:  maxSize,
		maxDelay: maxDelay,
		flush:    flush,
		in:       make(chan T, bufferSize),
		done:     make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// Add enqueues a record. It blocks if the internal buffer is full (backpressure)
// or returns immediately once the batcher is closed (the record is dropped —
// callers should stop producing before Close).
func (b *Batcher[T]) Add(rec T) {
	select {
	case b.in <- rec:
	case <-b.done:
	}
}

// run is the single goroutine that owns the pending buffer and the timer. Owning
// this state in one goroutine means we need no locks around the buffer itself —
// a clean application of "don't communicate by sharing memory; share memory by
// communicating".
func (b *Batcher[T]) run() {
	defer b.wg.Done()

	pending := make([]T, 0, b.maxSize)
	timer := time.NewTimer(b.maxDelay)
	timer.Stop() // no timer until the first record arrives
	timerActive := false

	flushNow := func() {
		if len(pending) == 0 {
			return
		}
		b.flush(context.Background(), pending)
		pending = make([]T, 0, b.maxSize)
		if timerActive {
			timer.Stop()
			timerActive = false
		}
	}

	add := func(rec T) {
		if len(pending) == 0 && !timerActive {
			// First record of a new batch: start the latency clock.
			timer.Reset(b.maxDelay)
			timerActive = true
		}
		pending = append(pending, rec)
		if len(pending) >= b.maxSize {
			flushNow()
		}
	}

	for {
		select {
		case rec := <-b.in:
			add(rec)
		case <-timer.C:
			timerActive = false
			flushNow()
		case <-b.done:
			// Shutdown: drain anything already buffered in the channel (without
			// blocking), then do a final flush so no telemetry is lost.
			for {
				select {
				case rec := <-b.in:
					add(rec)
				default:
					flushNow()
					return
				}
			}
		}
	}
}

// Close signals shutdown, waits for the final flush, and returns. We deliberately
// never close the intake channel: closing it would race with concurrent Add
// calls and could panic ("send on closed channel"). Instead Close closes `done`,
// Add stops sending once it observes `done`, and run drains + flushes. Callers
// should stop producing before calling Close for a clean drain.
func (b *Batcher[T]) Close() {
	close(b.done)
	b.wg.Wait()
}
