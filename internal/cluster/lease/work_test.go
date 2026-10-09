package lease

import (
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A pass sees every payload attached to its key since the last pass, and a
// key queued twice before it runs is served once — the property that turns
// a client's retries on two survivors into one Detach/Take and two acks.
func TestWorkQueueCoalescesPerKey(t *testing.T) {
	var mu sync.Mutex
	passes := map[key][][]any{}
	q := newWorkQueue("t", 2, slog.Default(), func(k key, payloads []any) {
		mu.Lock()
		passes[k] = append(passes[k], payloads)
		mu.Unlock()
	})
	k := key{"ns", "c1"}
	q.enqueue(k, "req-1")
	q.enqueue(k, "req-2")
	q.enqueue(k, nil) // a bare notice for the same key rides the same pass
	q.enqueue(key{"ns", "c2"}, nil)
	if got := q.stats(); got.Depth != 2 || got.Coalesced != 2 {
		t.Fatalf("before start: %+v, want depth 2 coalesced 2", got)
	}
	q.start()
	defer q.stopAndWait()
	waitUntil(t, func() bool { return q.stats().Processed == 2 })
	mu.Lock()
	defer mu.Unlock()
	if len(passes[k]) != 1 || len(passes[k][0]) != 2 {
		t.Fatalf("c1 passes = %v, want one pass with two payloads", passes[k])
	}
	if len(passes[key{"ns", "c2"}]) != 1 {
		t.Fatalf("c2 passes = %v, want one", passes[key{"ns", "c2"}])
	}
}

// The queue bounds goroutines to its workers whatever the arrival count.
// The negative control below runs the shape this replaced — a goroutine per
// event, each waiting on one lock — and the same measurement must see the
// pile-up, or the assertion here proves nothing.
func TestWorkQueueBoundsGoroutines(t *testing.T) {
	const n = 2000
	var lock sync.Mutex
	var served atomic.Int64
	// Hold the lock for a few microseconds of real work: long enough that
	// arrivals outrun service, short enough that 2,000 serialized passes
	// finish well inside the test's patience (time.Sleep would round each
	// hold up to the timer resolution).
	work := func(key, []any) {
		lock.Lock()
		until := time.Now().Add(5 * time.Microsecond)
		for time.Now().Before(until) {
		}
		lock.Unlock()
		served.Add(1)
	}

	t.Run("queued", func(t *testing.T) {
		q := newWorkQueue("bound", 4, slog.Default(), work)
		q.start()
		defer q.stopAndWait()
		before := runtime.NumGoroutine()
		for i := 0; i < n; i++ {
			q.enqueue(key{"ns", "c" + itoa(i)}, nil)
		}
		peak := runtime.NumGoroutine()
		if peak-before > 8 {
			t.Fatalf("goroutines grew by %d during %d enqueues; the queue must not fan out", peak-before, n)
		}
		waitUntil(t, func() bool { return served.Load() == n })
	})

	t.Run("negative control: goroutine per event piles up", func(t *testing.T) {
		served.Store(0)
		before := runtime.NumGoroutine()
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				work(key{}, nil)
			}()
		}
		peak := runtime.NumGoroutine()
		wg.Wait()
		if peak-before < n/10 {
			t.Fatalf("the control did not pile up (%d extra goroutines); the bound assertion would be vacuous", peak-before)
		}
	})
}

// A pass that panics is retried once; a second panic drops the key,
// counted, and never takes a worker down.
func TestWorkQueueRetriesOnceThenDrops(t *testing.T) {
	var calls atomic.Int64
	var events []string
	var mu sync.Mutex
	q := newWorkQueue("p", 1, slog.Default(), func(key, []any) {
		calls.Add(1)
		panic("boom")
	})
	q.onEvent = func(o string) { mu.Lock(); events = append(events, o); mu.Unlock() }
	q.start()
	defer q.stopAndWait()
	q.enqueue(key{"ns", "x"}, "payload")
	waitUntil(t, func() bool { return calls.Load() == 2 })
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e == "dropped" {
				return true
			}
		}
		return false
	})
	// The worker survived: a later key is still served.
	var ok atomic.Bool
	q.run = func(key, []any) { ok.Store(true) }
	q.enqueue(key{"ns", "y"}, nil)
	waitUntil(t, ok.Load)
}

func TestWorkQueueStopEndsWorkers(t *testing.T) {
	q := newWorkQueue("s", 3, slog.Default(), func(key, []any) {})
	before := runtime.NumGoroutine()
	q.start()
	waitUntil(t, func() bool { return runtime.NumGoroutine() >= before+3 })
	q.stopAndWait()
	waitUntil(t, func() bool { return runtime.NumGoroutine() <= before })
	q.enqueue(key{"ns", "late"}, nil) // after stop: ignored, never blocks
	q.stopAndWait()                   // idempotent
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in 5s")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
