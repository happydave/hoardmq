package otel

import (
	"net/http"
	"testing"
	"time"
)

// TestExportRetryDecision: only 429, 502, 503, 504 and a failed transport are
// retried, and not past the last attempt; the wait is the larger of the
// backoff and Retry-After capped at the flush interval.
func TestExportRetryDecision(t *testing.T) {
	flush := time.Second
	cases := []struct {
		name       string
		status     int
		transport  bool
		attempt    int
		retryAfter time.Duration
		want       outcome
		wait       time.Duration
	}{
		{"200", 200, false, 1, 0, outcomeDone, 0},
		{"204", 204, false, 1, 0, outcomeDone, 0},
		{"400", 400, false, 1, 0, outcomeDrop, 0},
		{"404", 404, false, 1, 0, outcomeDrop, 0},
		{"413", 413, false, 1, 0, outcomeDrop, 0},
		{"500", 500, false, 1, 0, outcomeDrop, 0},
		{"429 with Retry-After 2 s", 429, false, 1, 2 * time.Second, outcomeRetry, flush},
		{"502, second attempt", 502, false, 2, 0, outcomeRetry, 200 * time.Millisecond},
		{"503 without Retry-After", 503, false, 1, 0, outcomeRetry, 100 * time.Millisecond},
		{"504 at the last attempt", 504, false, maxAttempts, 0, outcomeDrop, 0},
		{"transport failed", 0, true, 1, 0, outcomeRetry, 100 * time.Millisecond},
		{"transport failed at the last attempt", 0, true, maxAttempts, 0, outcomeDrop, 0},
		{"Retry-After below the backoff", 503, false, 2, 50 * time.Millisecond, outcomeRetry, 200 * time.Millisecond},
	}
	for _, tc := range cases {
		got, wait := exportDecision(tc.status, tc.transport, tc.attempt, tc.retryAfter, flush)
		if got != tc.want || wait != tc.wait {
			t.Errorf("%s: %v after %v, want %v after %v", tc.name, got, wait, tc.want, tc.wait)
		}
	}
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	for v, want := range map[string]time.Duration{
		"3": 3 * time.Second, "": 0, "soon": 0,
		now.Add(5 * time.Second).Format(http.TimeFormat): 5 * time.Second,
	} {
		if got := parseRetryAfter(v, now); got != want {
			t.Errorf("Retry-After %q: %v, want %v", v, got, want)
		}
	}
}
