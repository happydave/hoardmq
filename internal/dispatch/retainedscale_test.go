package dispatch

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
)

// TestRetainedScale measures a wildcard subscribe's retained lookup and a
// retained write made while such lookups run, at each count of retained
// topics HOARDMQ_RETAINED_SCALE lists (comma-separated). It only measures:
// it fails only when a precondition or a result's size is wrong.
//
// Topics are t/<i/100>/<i%100>, so t/42/+ matches 100 of them at any count
// from 4,300, and t/# matches all.
func TestRetainedScale(t *testing.T) {
	spec := os.Getenv("HOARDMQ_RETAINED_SCALE")
	if spec == "" {
		t.Skip("HOARDMQ_RETAINED_SCALE is not set")
	}
	for _, f := range strings.Split(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 4300 {
			t.Fatalf("HOARDMQ_RETAINED_SCALE %q: each count is a number of at least 4300", spec)
		}
		retainedScale(t, n)
	}
}

func retainedScale(t *testing.T, n int) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	r := New(RoundRobin)
	start := time.Now()
	for i := 0; i < n; i++ {
		r.Retain(&message.Message{Namespace: "default", Topic: fmt.Sprintf("t/%d/%d", i/100, i%100), Payload: []byte("x"), Retain: true})
	}
	fill := time.Since(start)
	if got := r.RetainedCount(); got != n {
		t.Fatalf("n=%d: %d retained after the fill", n, got)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("retained-scale n=%d fill=%s heap=%d bytes (%.0f a topic) gomaxprocs=%d", n, fill, after.HeapAlloc-before.HeapAlloc, float64(after.HeapAlloc-before.HeapAlloc)/float64(n), runtime.GOMAXPROCS(0))
	now := time.Now()

	lookup := func(filter string, want, runs int) []time.Duration {
		var ds []time.Duration
		for k := 0; k < runs; k++ {
			s := time.Now()
			got := r.RetainedFor("default", filter, now)
			ds = append(ds, time.Since(s))
			if len(got) != want {
				t.Fatalf("n=%d: %s matched %d, want %d", n, filter, len(got), want)
			}
		}
		return ds
	}
	t.Logf("retained-scale n=%d subscribe t/42/+ (100 matches) alone: %s", n, summary(lookup("t/42/+", 100, 20)))
	t.Logf("retained-scale n=%d subscribe t/# (%d matches) alone: %s", n, n, summary(lookup("t/#", n, 5)))

	write := func(k int) time.Duration {
		s := time.Now()
		r.Retain(&message.Message{Namespace: "default", Topic: fmt.Sprintf("t/%d/%d", (k%n)/100, k%100), Payload: []byte("y"), Retain: true})
		return time.Since(s)
	}
	var alone []time.Duration
	for k := 0; k < 2000; k++ {
		alone = append(alone, write(k))
	}
	t.Logf("retained-scale n=%d write alone: %s", n, summary(alone))

	const readers = 4
	window := 3 * time.Second
	var stop atomic.Bool
	var scans atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < readers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if got := r.RetainedFor("default", "t/42/+", now); len(got) != 100 {
					t.Errorf("n=%d: a concurrent t/42/+ matched %d", n, len(got))
					return
				}
				scans.Add(1)
			}
		}()
	}
	var during []time.Duration
	end := time.Now().Add(window)
	for k := 0; time.Now().Before(end); k++ {
		during = append(during, write(k))
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	if got := r.RetainedCount(); got != n {
		t.Fatalf("n=%d: %d retained after the writes", n, got)
	}
	t.Logf("retained-scale n=%d write during %d readers of t/42/+ for %s: %s; the readers made %d lookups", n, readers, window, summary(during), scans.Load())
}

// summary is the count, median, 99th percentile and maximum of ds.
func summary(ds []time.Duration) string {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(q float64) time.Duration { return s[int(q*float64(len(s)-1))] }
	return fmt.Sprintf("count=%d median=%s p99=%s max=%s", len(s), at(0.5), at(0.99), s[len(s)-1])
}
