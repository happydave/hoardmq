package replication

import (
	"testing"
	"time"
)

// histP99 applies p99Of's rule to a read histogram.
func histP99(h Histogram) int64 {
	if h.Count == 0 {
		return 0
	}
	want := (h.Count*99 + 99) / 100
	var seen int64
	for i, n := range h.Buckets {
		if seen += n; seen >= want {
			return int64(1) << i * 1000
		}
	}
	return -1
}

// TestHistogramsReadTheTrace: the histograms read for export are the lock
// trace's and the fetch timing's own buckets, counts and sums, kind by kind,
// and their 99th percentile is the trace's.
func TestHistogramsReadTheTrace(t *testing.T) {
	r := &Replicator{}
	r.follower = &follower{r: r}
	c := &r.follower.lockHolds
	// Bucket 0 (below 1 µs), 2 ([2, 4) µs), 17 ([65.5, 131) ms), 22 ([2.1,
	// 4.2) s) and 23 (past the last bound /metrics writes).
	durations := []time.Duration{500 * time.Nanosecond, 3 * time.Microsecond, 100 * time.Millisecond, 3 * time.Second, 5 * time.Second}
	wantBuckets := []int{0, 2, 17, 22, 23}
	var sum int64
	for _, d := range durations {
		c.waited(holdFind, d)
		r.fetches.requested(int64(d))
		sum += int64(d)
	}
	c.waited(holdApply, 7*time.Microsecond)
	now := time.Now()
	c.held(holdFind, now) // the trace reports a kind's waits beside its holds
	c.held(holdApply, now.Add(-3*time.Second))
	c.held(holdApply, now.Add(-100*time.Millisecond))

	holds, waits := r.LockHistograms()
	if len(holds) != holdKinds {
		t.Fatalf("holds read for %d kinds; want all %d", len(holds), holdKinds)
	}
	if len(waits) != 3 || waits["apply"].Count != 1 || waits["apply-other"].Count != 0 {
		t.Fatalf("waits read %+v; want apply, apply-other and find only", waits)
	}
	find := waits["find"]
	for i, b := range wantBuckets {
		if find.Buckets[b] != 1 {
			t.Errorf("the %v wait is not in bucket %d: %v", durations[i], b, find.Buckets)
		}
	}
	if find.Count != int64(len(durations)) || find.SumNs != sum {
		t.Errorf("find waits: count %d, sum %d ns; want %d and %d", find.Count, find.SumNs, len(durations), sum)
	}
	st := c.stats()
	if got, want := histP99(find), st.ByKind["find"].WaitP99Ns; got != want || want == 0 {
		t.Errorf("find wait p99 from the read buckets %d; the trace's %d", got, want)
	}
	if find.SumNs != st.ByKind["find"].WaitTotalNs {
		t.Errorf("find wait sum %d; the trace's %d", find.SumNs, st.ByKind["find"].WaitTotalNs)
	}
	apply := holds["apply"]
	if apply.Count != 2 || apply.Count != st.ByKind["apply"].Count || apply.SumNs != st.ByKind["apply"].TotalNs {
		t.Errorf("apply holds %+v; the trace's %+v", apply, st.ByKind["apply"])
	}
	if apply.Buckets[22] != 1 || apply.Buckets[17] != 1 {
		t.Errorf("apply holds of 3 s and 100 ms not in buckets 22 and 17: %v", apply.Buckets)
	}
	if got, want := histP99(apply), st.ByKind["apply"].HoldP99Ns; got != want {
		t.Errorf("apply hold p99 from the read buckets %d; the trace's %d", got, want)
	}
	if holds["checkpoint"].Count != 0 {
		t.Errorf("checkpoint holds %+v; none was recorded", holds["checkpoint"])
	}

	fetch := r.FetchRequestHistogram()
	fs := r.FetchStats()
	for i, b := range wantBuckets {
		if fetch.Buckets[b] != 1 {
			t.Errorf("the %v request is not in bucket %d: %v", durations[i], b, fetch.Buckets)
		}
	}
	if fetch.Count != int64(len(durations)) || fetch.SumNs != sum || float64(fetch.SumNs)/1e6 != fs.RequestMs {
		t.Errorf("fetch requests: count %d, sum %d ns; want %d and %d (the trace's %v ms)", fetch.Count, fetch.SumNs, len(durations), sum, fs.RequestMs)
	}
	if got := histP99(fetch); got != fs.RequestP99Ns || got == 0 {
		t.Errorf("fetch request p99 from the read buckets %d; the trace's %d", got, fs.RequestP99Ns)
	}
}
