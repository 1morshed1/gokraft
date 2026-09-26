// Package alert is gokraft's alerting engine. It periodically evaluates rules
// like "average error_rate for service api over the last 5 minutes has been
// above 0.05 for at least 2 minutes" and fires a notification when the condition
// holds long enough — then fires a resolution when it recovers.
//
// Two deliberate design choices:
//
//   - Structured rules instead of an expression language. A real system (Datadog,
//     Prometheus) parses a query DSL like `avg(rate(errors[5m])) > 0.05`. Writing
//     a parser is a project of its own and would obscure the alerting *logic*,
//     which is the actual lesson here. So a Rule is a struct of typed fields
//     (metric, service, aggregation, comparator, threshold, windows). The DSL is
//     left as a documented extension point.
//
//   - The "for duration" semantics. Naively firing the instant a threshold is
//     crossed produces alert storms from momentary spikes. Real alerting requires
//     the condition to hold continuously for a configured Duration before firing.
//     That state machine (ok → pending → firing → ok) is the interesting part and
//     is implemented in evaluator.go, fully unit-tested with an injected clock.
package alert

import (
	"fmt"
	"sync"
	"time"
)

// Comparator is how a measured value is tested against a threshold.
type Comparator string

const (
	GT  Comparator = ">"
	GTE Comparator = ">="
	LT  Comparator = "<"
	LTE Comparator = "<="
)

// Breaches reports whether value violates the threshold under this comparator.
func (c Comparator) Breaches(value, threshold float64) bool {
	switch c {
	case GT:
		return value > threshold
	case GTE:
		return value >= threshold
	case LT:
		return value < threshold
	case LTE:
		return value <= threshold
	default:
		return false
	}
}

// Severity of an alert.
type Severity string

const (
	Warning  Severity = "warning"
	Critical Severity = "critical"
)

// Rule is one alerting condition.
type Rule struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Metric      string        `json:"metric"`               // e.g. "http_error_rate"
	Service     string        `json:"service,omitempty"`    // optional service filter
	Aggregation string        `json:"aggregation"`          // avg | max | min | sum
	Comparator  Comparator    `json:"comparator"`           // > >= < <=
	Threshold   float64       `json:"threshold"`            // the value to compare against
	Window      time.Duration `json:"window"`               // lookback for the aggregate (e.g. 5m)
	For         time.Duration `json:"for"`                  // must breach this long before firing
	Severity    Severity      `json:"severity"`             // warning | critical
	NotifyURL   string        `json:"notify_url,omitempty"` // webhook to POST on fire/resolve
}

// Validate checks a rule is well-formed before it is accepted.
func (r Rule) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("name is required")
	}
	if r.Metric == "" {
		return fmt.Errorf("metric is required")
	}
	switch r.Comparator {
	case GT, GTE, LT, LTE:
	default:
		return fmt.Errorf("invalid comparator %q", r.Comparator)
	}
	if r.Window <= 0 {
		return fmt.Errorf("window must be positive")
	}
	if r.For < 0 {
		return fmt.Errorf("for must be non-negative")
	}
	return nil
}

// RuleStore is a concurrency-safe in-memory registry of rules, exposed via the
// API for CRUD. In-memory is fine for a learning platform (rules are few and a
// restart reloading defaults is acceptable); persisting them to Postgres is a
// documented extension. All methods are safe for concurrent use because the API
// handlers and the evaluator goroutine both touch it.
type RuleStore struct {
	mu    sync.RWMutex
	rules map[string]Rule
	seq   int
}

// NewRuleStore creates an empty store.
func NewRuleStore() *RuleStore {
	return &RuleStore{rules: make(map[string]Rule)}
}

// Upsert validates and stores a rule, assigning an ID if absent. Returns the
// stored rule.
func (s *RuleStore) Upsert(r Rule) (Rule, error) {
	if err := r.Validate(); err != nil {
		return Rule{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ID == "" {
		s.seq++
		r.ID = fmt.Sprintf("rule-%d", s.seq)
	}
	s.rules[r.ID] = r
	return r, nil
}

// Delete removes a rule by ID; reports whether it existed.
func (s *RuleStore) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rules[id]; !ok {
		return false
	}
	delete(s.rules, id)
	return true
}

// List returns a snapshot copy of all rules.
func (s *RuleStore) List() []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Rule, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, r)
	}
	return out
}
