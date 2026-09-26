package alert

// evaluator.go holds the alert state machine — the genuinely interesting part of
// alerting. Its whole job is to turn a stream of point-in-time measurements into
// stable fire/resolve events, suppressing the noise of momentary spikes.
//
// Per rule we track one of three states:
//
//	OK       — condition not breaching.
//	PENDING  — condition is breaching but has not yet breached for the rule's
//	           `For` duration. No notification sent.
//	FIRING   — condition has breached continuously for at least `For`. A single
//	           "firing" notification is sent on the OK/PENDING → FIRING edge.
//
// When the condition clears we return to OK and send one "resolved"
// notification (only if we had actually fired). Sending on *edges*, not on every
// tick, is what prevents alert storms.
//
// Everything here is deterministic and injectable: the metric value comes from
// a ScalarFunc, "now" comes from a clock func, and notifications go through the
// Notifier interface. That makes the state machine unit-testable with zero
// infrastructure and fake time (see evaluator_test.go).

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// State is the lifecycle position of a rule.
type State string

const (
	StateOK      State = "ok"
	StatePending State = "pending"
	StateFiring  State = "firing"
)

// ScalarFunc evaluates a rule's aggregate over [from, to]. It returns the value
// and whether any data existed (no data → we can't say it's breaching). This is
// exactly the signature TimescaleDB's Store.Scalar provides, but the evaluator
// depends only on the func so it never imports the storage layer.
type ScalarFunc func(ctx context.Context, metric, service, agg string, from, to time.Time) (value float64, ok bool, err error)

// Notifier receives fire and resolve events. Implementations: webhook, logger.
type Notifier interface {
	Fire(ctx context.Context, ev Event)
	Resolve(ctx context.Context, ev Event)
}

// Event is what a notifier receives.
type Event struct {
	Rule      Rule      `json:"rule"`
	Value     float64   `json:"value"`
	State     State     `json:"state"`
	Timestamp time.Time `json:"timestamp"`
}

type ruleState struct {
	state          State
	breachingSince time.Time
}

// Evaluator runs the state machine across all rules.
type Evaluator struct {
	rules  *RuleStore
	scalar ScalarFunc
	notify Notifier
	log    *slog.Logger
	now    func() time.Time // injectable clock (defaults to time.Now)

	mu    sync.Mutex
	state map[string]*ruleState
}

// NewEvaluator wires the dependencies. A nil clock defaults to time.Now.
func NewEvaluator(rules *RuleStore, scalar ScalarFunc, notify Notifier, log *slog.Logger) *Evaluator {
	return &Evaluator{
		rules:  rules,
		scalar: scalar,
		notify: notify,
		log:    log,
		now:    time.Now,
		state:  make(map[string]*ruleState),
	}
}

// EvaluateOnce runs one evaluation pass over all rules. It is exported so tests
// can drive it deterministically tick by tick; Run calls it on a ticker.
func (e *Evaluator) EvaluateOnce(ctx context.Context) {
	now := e.now().UTC()
	for _, rule := range e.rules.List() {
		e.evaluateRule(ctx, rule, now)
	}
}

func (e *Evaluator) evaluateRule(ctx context.Context, rule Rule, now time.Time) {
	value, ok, err := e.scalar(ctx, rule.Metric, rule.Service, rule.Aggregation, now.Add(-rule.Window), now)
	if err != nil {
		e.log.Warn("alert eval error", "rule", rule.ID, "err", err)
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.state[rule.ID]
	if st == nil {
		st = &ruleState{state: StateOK}
		e.state[rule.ID] = st
	}

	// No data is treated as "not breaching" — we never alert on absence here.
	breaching := ok && rule.Comparator.Breaches(value, rule.Threshold)

	if !breaching {
		if st.state == StateFiring {
			// Was firing, now clear: emit a single resolve.
			e.notify.Resolve(ctx, Event{Rule: rule, Value: value, State: StateOK, Timestamp: now})
			e.log.Info("alert resolved", "rule", rule.ID, "value", value)
		}
		st.state = StateOK
		st.breachingSince = time.Time{}
		return
	}

	// Breaching.
	if st.breachingSince.IsZero() {
		st.breachingSince = now // start of a breach episode
	}
	breachedFor := now.Sub(st.breachingSince)

	switch st.state {
	case StateFiring:
		// Already firing; stay firing, no repeat notification.
	default:
		if breachedFor >= rule.For {
			st.state = StateFiring
			e.notify.Fire(ctx, Event{Rule: rule, Value: value, State: StateFiring, Timestamp: now})
			e.log.Warn("alert firing", "rule", rule.ID, "value", value, "threshold", rule.Threshold, "breached_for", breachedFor.String())
		} else {
			st.state = StatePending
		}
	}
}

// StateOf returns the current state of a rule (for the API / tests).
func (e *Evaluator) StateOf(id string) State {
	e.mu.Lock()
	defer e.mu.Unlock()
	if st, ok := e.state[id]; ok {
		return st.state
	}
	return StateOK
}

// Run evaluates on `interval` until ctx is cancelled. Call it in a goroutine
// from the query API process.
func (e *Evaluator) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	e.log.Info("alert evaluator started", "interval", interval.String())
	for {
		select {
		case <-ctx.Done():
			e.log.Info("alert evaluator stopped")
			return
		case <-ticker.C:
			e.EvaluateOnce(ctx)
		}
	}
}
