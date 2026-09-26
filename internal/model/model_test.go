package model

import (
	"testing"
	"time"
)

// TestSpanEndTime verifies the derived EndTime helper. It looks trivial, but it
// is the basis for correlation time-windows and waterfall layout, so a bug here
// would quietly corrupt trace rendering.
func TestSpanEndTime(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := Span{StartTime: start, DurationUS: 1_500_000} // 1.5 seconds
	want := start.Add(1500 * time.Millisecond)
	if got := s.EndTime(); !got.Equal(want) {
		t.Fatalf("EndTime() = %v, want %v", got, want)
	}
}

func TestStatusCodeValues(t *testing.T) {
	// These integer values are persisted to Postgres and must not drift, or
	// old rows would be reinterpreted. Pin them with a test.
	if StatusUnset != 0 || StatusOK != 1 || StatusError != 2 {
		t.Fatalf("status code constants changed: unset=%d ok=%d error=%d",
			StatusUnset, StatusOK, StatusError)
	}
}
