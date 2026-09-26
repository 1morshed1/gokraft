package alert

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// recordingNotifier counts fire/resolve calls for assertions.
type recordingNotifier struct {
	fires    int
	resolves int
}

func (n *recordingNotifier) Fire(context.Context, Event)    { n.fires++ }
func (n *recordingNotifier) Resolve(context.Context, Event) { n.resolves++ }

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// buildEvaluator wires an evaluator whose clock and metric value we control.
func buildEvaluator(rule Rule, value *float64, ok *bool, now *time.Time) (*Evaluator, *recordingNotifier) {
	rules := NewRuleStore()
	_, _ = rules.Upsert(rule)
	// Grab the assigned ID back.
	stored := rules.List()[0]
	_ = stored

	scalar := func(context.Context, string, string, string, time.Time, time.Time) (float64, bool, error) {
		return *value, *ok, nil
	}
	n := &recordingNotifier{}
	e := NewEvaluator(rules, scalar, n, quietLog())
	e.now = func() time.Time { return *now }
	return e, n
}

// TestForDurationSuppressesSpike is the core test: a rule with For=2m must NOT
// fire on a single breaching tick, must fire once the breach has persisted past
// 2m, must not re-fire while it stays firing, and must resolve exactly once when
// the value recovers.
func TestForDurationSuppressesSpike(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	value := 0.0
	ok := true

	rule := Rule{
		Name: "high error rate", Metric: "error_rate", Aggregation: "avg",
		Comparator: GT, Threshold: 0.05, Window: 5 * time.Minute, For: 2 * time.Minute,
		Severity: Critical,
	}
	e, n := buildEvaluator(rule, &value, &ok, &now)
	id := e.rules.List()[0].ID

	// t=0: breaching begins, but For hasn't elapsed -> PENDING, no fire.
	value = 0.10
	e.EvaluateOnce(context.Background())
	if got := e.StateOf(id); got != StatePending {
		t.Fatalf("t=0 state = %s, want pending", got)
	}
	if n.fires != 0 {
		t.Fatalf("fired too early: %d", n.fires)
	}

	// t=1m: still breaching, still under For -> PENDING.
	now = base.Add(time.Minute)
	e.EvaluateOnce(context.Background())
	if got := e.StateOf(id); got != StatePending {
		t.Fatalf("t=1m state = %s, want pending", got)
	}
	if n.fires != 0 {
		t.Fatalf("fired too early at 1m: %d", n.fires)
	}

	// t=2m: breach has persisted for For -> FIRING, exactly one fire.
	now = base.Add(2 * time.Minute)
	e.EvaluateOnce(context.Background())
	if got := e.StateOf(id); got != StateFiring {
		t.Fatalf("t=2m state = %s, want firing", got)
	}
	if n.fires != 1 {
		t.Fatalf("expected 1 fire, got %d", n.fires)
	}

	// t=3m: still breaching -> stays FIRING, no repeat notification.
	now = base.Add(3 * time.Minute)
	e.EvaluateOnce(context.Background())
	if n.fires != 1 {
		t.Fatalf("re-fired while already firing: %d", n.fires)
	}

	// t=4m: recovers -> OK, exactly one resolve.
	now = base.Add(4 * time.Minute)
	value = 0.01
	e.EvaluateOnce(context.Background())
	if got := e.StateOf(id); got != StateOK {
		t.Fatalf("t=4m state = %s, want ok", got)
	}
	if n.resolves != 1 {
		t.Fatalf("expected 1 resolve, got %d", n.resolves)
	}
}

// TestBreachResetsIfConditionClearsBeforeFor verifies a breach that clears
// before For elapses never fires, and the timer resets for the next episode.
func TestBreachResetsIfConditionClearsBeforeFor(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := base
	value := 0.10
	ok := true
	rule := Rule{Name: "r", Metric: "m", Aggregation: "avg", Comparator: GT, Threshold: 0.05, Window: time.Minute, For: 2 * time.Minute}
	e, n := buildEvaluator(rule, &value, &ok, &now)

	e.EvaluateOnce(context.Background()) // t=0 pending
	now = base.Add(time.Minute)
	value = 0.0 // clears before For
	e.EvaluateOnce(context.Background())
	if n.fires != 0 || n.resolves != 0 {
		t.Fatalf("should neither fire nor resolve: fires=%d resolves=%d", n.fires, n.resolves)
	}
	if got := e.StateOf(e.rules.List()[0].ID); got != StateOK {
		t.Fatalf("state = %s, want ok after clearing", got)
	}
}

func TestComparatorBreaches(t *testing.T) {
	cases := []struct {
		c        Comparator
		v, thr   float64
		breaches bool
	}{
		{GT, 6, 5, true}, {GT, 5, 5, false},
		{GTE, 5, 5, true}, {LT, 4, 5, true},
		{LTE, 5, 5, true}, {LTE, 6, 5, false},
	}
	for _, c := range cases {
		if got := c.c.Breaches(c.v, c.thr); got != c.breaches {
			t.Errorf("%s %v vs %v = %v, want %v", c.c, c.v, c.thr, got, c.breaches)
		}
	}
}
