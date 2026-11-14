package sim

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

// lockBody is a /metrics body holding a find-wait histogram at the given
// cumulative counts over the bounds 1e-06, 0.000128, 0.004096 and +Inf.
func lockBody(counts [4]int, sum string) string {
	les := []string{"1e-06", "0.000128", "0.004096", "+Inf"}
	var b strings.Builder
	// In label order, as /metrics writes them: "+Inf" sorts first as text.
	for _, i := range []int{3, 1, 2, 0} {
		le := les[i]
		b.WriteString(`hoardmq_replication_lock_wait_seconds_bucket{kind="find",le="` + le + `"} `)
		b.WriteString(strconv.Itoa(counts[i]) + "\n")
	}
	b.WriteString(`hoardmq_replication_lock_wait_seconds_sum{kind="find"} ` + sum + "\n")
	b.WriteString(`hoardmq_replication_lock_wait_seconds_count{kind="find"} ` + strconv.Itoa(counts[3]) + "\n")
	b.WriteString("# TYPE hoardmq_sessions_active gauge\nhoardmq_sessions_active{namespace=\"default\"} 7\n")
	return b.String()
}

// TestScaleLockHistogramsWindow: the run reads a node's histograms by family
// and kind, and the storm window is the later reading less the earlier: its
// count, and its 99th percentile's bucket bound read from the difference, not
// from either reading alone.
func TestScaleLockHistogramsWindow(t *testing.T) {
	before := parseHistograms(lockBody([4]int{1000, 1000, 1000, 1000}, "0.01"))
	after := parseHistograms(lockBody([4]int{1000, 1000, 1004, 1005}, "0.5"))
	key := "hoardmq_replication_lock_wait_seconds/find"
	h := after[key]
	if h == nil || len(after) != 1 {
		t.Fatalf("parsed %v; want the find waits alone", after)
	}
	if h.Count != 1005 || h.Sum != 0.5 || len(h.Le) != 4 || h.Le[0] != "1e-06" || h.Le[3] != "+Inf" || h.Counts[2] != 1004 {
		t.Fatalf("find waits %+v", h)
	}
	// Five samples in the window, the fifth past the last finite bound: the
	// later reading alone, with the earlier's thousand short waits, would put
	// the p99 at 1 µs.
	count, p99, ok := histWindow(before[key], after[key])
	if !ok || count != 5 || !math.IsInf(p99, 1) {
		t.Fatalf("window: count %v, p99 %v, ok %v; want 5, +Inf", count, p99, ok)
	}
	mid := parseHistograms(lockBody([4]int{1000, 1000, 1100, 1100}, "0.2"))
	if count, p99, ok := histWindow(before[key], mid[key]); !ok || count != 100 || p99 != 0.004096 {
		t.Fatalf("window: count %v, p99 %v; want 100, 0.004096", count, p99)
	}
	if _, _, ok := histWindow(after[key], before[key]); ok {
		t.Fatal("a reading counting fewer than the one before it (a restart) read as a window")
	}
	if got := parseHistograms("# TYPE hoardmq_sessions_active gauge\nhoardmq_sessions_active 3\n"); len(got) != 0 {
		t.Fatalf("a body without the families parsed as %v", got)
	}
	report := lockReport([]lockReading{
		{Point: "storm-start", Node: "n2", Hist: before},
		{Point: "storm-end", Node: "n2", Hist: mid},
		{Point: "storm-start", Node: "n1", Hist: before},
		{Point: "storm-end", Node: "n1", Error: "connection refused"},
	})
	if !strings.Contains(report, "| n2 | 100 | 4.096 |") || strings.Contains(report, "| n1 |") {
		t.Fatalf("report:\n%s", report)
	}
}
