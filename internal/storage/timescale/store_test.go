package timescale

import (
	"testing"
	"time"
)

// TestSelectTable pins the rollup-selection heuristic: short ranges hit the raw
// table, medium the 1-minute rollup, long the 1-hour rollup. This is the logic
// that keeps dashboard queries fast across any time range, so it is worth a
// regression test even though it is just a switch.
func TestSelectTable(t *testing.T) {
	cases := []struct {
		rng       time.Duration
		wantTable string
	}{
		{5 * time.Minute, "metrics"},
		{30 * time.Minute, "metrics"},
		{2 * time.Hour, "metrics_1m"},
		{24 * time.Hour, "metrics_1m"},
		{72 * time.Hour, "metrics_1h"},
		{30 * 24 * time.Hour, "metrics_1h"},
	}
	for _, c := range cases {
		gotTable, _, _ := selectTable(c.rng, "avg")
		if gotTable != c.wantTable {
			t.Errorf("selectTable(%v) table = %q, want %q", c.rng, gotTable, c.wantTable)
		}
	}
}

// TestRollupColMatchesAggregation ensures each aggregation reads the correct
// pre-computed column from a rollup view (a max query must read max_val, etc.).
func TestRollupCol(t *testing.T) {
	cases := map[string]string{
		"max": "max(max_val)",
		"min": "min(min_val)",
		"sum": "sum(sum_val)",
		"avg": "avg(avg_val)",
		"":    "avg(avg_val)", // default
	}
	for agg, want := range cases {
		if got := rollupCol(agg); got != want {
			t.Errorf("rollupCol(%q) = %q, want %q", agg, got, want)
		}
	}
}

func TestPgInterval(t *testing.T) {
	cases := map[time.Duration]string{
		10 * time.Second: "10 seconds",
		5 * time.Minute:  "5 minutes",
		2 * time.Hour:    "2 hours",
	}
	for d, want := range cases {
		if got := pgInterval(d); got != want {
			t.Errorf("pgInterval(%v) = %q, want %q", d, got, want)
		}
	}
}
