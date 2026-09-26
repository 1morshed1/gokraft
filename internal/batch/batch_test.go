package batch

import (
	"context"
	"sync"
	"testing"
	"time"
)

// collector is a thread-safe flush sink used by the tests to record what the
// batcher emitted and how many times it flushed.
type collector struct {
	mu      sync.Mutex
	batches [][]int
}

func (c *collector) flush(_ context.Context, recs []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Copy: the batcher may reuse the backing array after flush returns.
	cp := append([]int(nil), recs...)
	c.batches = append(c.batches, cp)
}

func (c *collector) snapshot() [][]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]int(nil), c.batches...)
}

// TestFlushesOnSize verifies the size trigger: with maxSize=3 and a long delay,
// pushing 3 records flushes immediately without waiting for the timer.
func TestFlushesOnSize(t *testing.T) {
	c := &collector{}
	b := New[int](3, time.Hour, 16, c.flush)
	defer b.Close()

	b.Add(1)
	b.Add(2)
	b.Add(3)

	waitFor(t, func() bool { return len(c.snapshot()) == 1 })
	got := c.snapshot()
	if len(got[0]) != 3 {
		t.Fatalf("expected a batch of 3, got %v", got[0])
	}
}

// TestFlushesOnTime verifies the time trigger: fewer than maxSize records still
// flush once maxDelay elapses.
func TestFlushesOnTime(t *testing.T) {
	c := &collector{}
	b := New[int](100, 40*time.Millisecond, 16, c.flush)
	defer b.Close()

	b.Add(1)
	b.Add(2)

	waitFor(t, func() bool { return len(c.snapshot()) == 1 })
	got := c.snapshot()
	if len(got[0]) != 2 {
		t.Fatalf("expected a time-triggered batch of 2, got %v", got[0])
	}
}

// TestFlushesRemainderOnClose verifies graceful shutdown drains buffered records.
func TestFlushesRemainderOnClose(t *testing.T) {
	c := &collector{}
	b := New[int](100, time.Hour, 16, c.flush)
	b.Add(1)
	b.Add(2)
	b.Close() // should flush the remaining 2

	got := c.snapshot()
	if len(got) != 1 || len(got[0]) != 2 {
		t.Fatalf("expected a single flushed batch of 2 on close, got %v", got)
	}
}

// waitFor polls cond up to 2s so tests are not flaky under a busy scheduler.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
