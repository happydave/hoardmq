package replication

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// These measure how long a replica-store checkpoint holds the follower's
// mutex against the number of copies held, and what that hold does to a
// fetch. They are measurements, not regressions: each runs only with
// HOARDMQ_TEST_CHECKPOINT_HOLD set, and logs its figures as "measure" lines.

func measureOnly(t *testing.T) {
	t.Helper()
	if os.Getenv("HOARDMQ_TEST_CHECKPOINT_HOLD") == "" {
		t.Skip("a measurement: set HOARDMQ_TEST_CHECKPOINT_HOLD to run it")
	}
}

// measuredSizes is the list of replica-set sizes to measure, from
// HOARDMQ_TEST_CHECKPOINT_COPIES (comma-separated) or def.
func measuredSizes(t *testing.T, def []int) []int {
	t.Helper()
	v := os.Getenv("HOARDMQ_TEST_CHECKPOINT_COPIES")
	if v == "" {
		return def
	}
	var out []int
	for _, s := range strings.Split(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 0 {
			t.Fatalf("HOARDMQ_TEST_CHECKPOINT_COPIES: %q", v)
		}
		out = append(out, n)
	}
	return out
}

// fillerCopy is a replica copy shaped as a scale run's: a client with one
// QoS 1 subscription and nothing queued.
func fillerCopy(i int) *store.SessionState {
	id := fmt.Sprintf("g%07d", i)
	return &store.SessionState{
		SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 35000 + uint64(i%1000), Version: 5,
			ExpiryInterval: 3600, ExpiresAt: time.Date(2026, 11, 12, 14, 33, 2, 102178685, time.FixedZone("", -7*3600))},
		Subscriptions: []store.SubscriptionRecord{{Filter: "f/" + id, QoS: 1}},
		InboundQoS2:   []uint16{},
		Entries:       []store.Entry{},
	}
}

// addFiller appends n filler copies to f's state and builds its index, as a
// follower that has held them a while has.
func addFiller(f *follower, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range n {
		f.state.Sessions = append(f.state.Sessions, fillerCopy(i))
	}
	f.state.Find("default", "none")
}

// measureFollower is a follower over a replica store in a temporary
// directory, with no transport, map or metrics.
func measureFollower(t *testing.T) *follower {
	t.Helper()
	rs, err := store.Open(t.TempDir(), store.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rs.Close() })
	return &follower{r: &Replicator{o: Options{Replica: rs}}, state: store.NewState()}
}

func holdsOf(f *follower, kind int) (int64, time.Duration) {
	return f.lockHolds.count[kind].Load(), time.Duration(f.lockHolds.total[kind].Load())
}

// TestMeasureCheckpointHoldAgainstCopies: for each replica-set size, a whole
// checkpoint through the replica store, then the follower's copy alone. Per
// checkpoint: its hold as the lock trace records it, its total time, and what
// it allocated (bytes and objects, and the heap left above a collected heap
// just before). It uses nothing the copy added, so it also runs on a tree
// without it, as the other arm of a comparison.
func TestMeasureCheckpointHoldAgainstCopies(t *testing.T) {
	measureOnly(t)
	reps := 3
	for _, n := range measuredSizes(t, []int{50000, 100000, 200000, 350000, 500000, 600000}) {
		f := measureFollower(t)
		addFiller(f, n)
		for rep := range reps {
			c0, h0 := holdsOf(f, holdCheckpoint)
			runtime.GC()
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			began := time.Now()
			if err := f.r.o.Replica.Checkpoint(f.checkpointState); err != nil {
				t.Fatal(err)
			}
			total := time.Since(began)
			runtime.ReadMemStats(&m1)
			c1, h1 := holdsOf(f, holdCheckpoint)
			if c1-c0 != 1 {
				t.Fatalf("checkpoint at %d copies: %d holds recorded, want 1", n, c1-c0)
			}
			st := f.checkpointState(func() uint64 { return 0 })
			if len(st.Sessions) != n {
				t.Fatalf("copy at %d copies: %d sessions", n, len(st.Sessions))
			}
			t.Logf("measure copies=%d rep=%d hold_ms=%.1f total_ms=%.1f alloc_mb=%.1f mallocs=%d heap_growth_mb=%.1f",
				n, rep, msOf(h1-h0), msOf(total), float64(m1.TotalAlloc-m0.TotalAlloc)/1e6, m1.Mallocs-m0.Mallocs,
				(float64(m1.HeapAlloc)-float64(m0.HeapAlloc))/1e6)
		}
	}
}

func msOf(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// TestMeasureFindsWaitOutTheCheckpoint is a per-wait trace: finds asked at
// spaced points inside one checkpoint's hold each return when the hold ends,
// so each waits the hold's remainder. The hold's end is read inside it, by the
// checkpoint's own index callback, which runs just before the release.
func TestMeasureFindsWaitOutTheCheckpoint(t *testing.T) {
	measureOnly(t)
	for _, n := range measuredSizes(t, []int{500000}) {
		f := measureFollower(t)
		addFiller(f, n)
		f.checkpointState(func() uint64 { return 1 }) // warm
		_, before := holdsOf(f, holdCheckpoint)
		expect := before

		var holdEnd time.Time
		start := time.Now()
		done := make(chan struct{})
		go func() {
			f.checkpointState(func() uint64 { holdEnd = time.Now(); return 1 })
			close(done)
		}()
		offsets := []float64{0.05, 0.25, 0.5, 0.75, 0.9}
		type wait struct {
			asked, returned time.Time
			held            bool
		}
		waits := make([]wait, len(offsets))
		var wg sync.WaitGroup
		for i, o := range offsets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(o * float64(expect)))
				asked := time.Now()
				_, held := f.find("default", fmt.Sprintf("g%07d", i))
				waits[i] = wait{asked, time.Now(), held}
			}()
		}
		wg.Wait()
		<-done
		_, after := holdsOf(f, holdCheckpoint)
		hold := after - before
		holdBegan := holdEnd.Add(-hold)
		t.Logf("measure copies=%d hold_ms=%.1f hold_began_ms=%.1f hold_end_ms=%.1f", n, msOf(hold), msOf(holdBegan.Sub(start)), msOf(holdEnd.Sub(start)))
		inside := 0
		for i, w := range waits {
			in := w.asked.After(holdBegan) && w.asked.Before(holdEnd)
			if in {
				inside++
			}
			t.Logf("measure find=%d asked_ms=%.1f returned_ms=%.1f waited_ms=%.1f after_release_ms=%.2f inside_hold=%v found=%v",
				i, msOf(w.asked.Sub(start)), msOf(w.returned.Sub(start)), msOf(w.returned.Sub(w.asked)), msOf(w.returned.Sub(holdEnd)), in, w.held)
			if in && w.returned.Before(holdEnd) {
				t.Errorf("find %d asked inside the hold returned %s before it ended", i, holdEnd.Sub(w.returned))
			}
		}
		if inside == 0 {
			t.Fatalf("no find was asked inside the hold (%s)", hold)
		}
	}
}

// TestMeasureTakeoverFetchBehindACheckpoint is the takeover fetch's route
// with a real checkpoint in place of a held mutex: n3 holds n filler copies
// besides X's absence, n1 holds X, and n3's fetch for previous owner n1 has n3
// alone as its source, under the product's 2 s budget. The checkpoint starts
// first; the fetch is asked a little after, as a takeover batch arriving
// inside a hold is. It logs the outcome; it asserts nothing about it.
func TestMeasureTakeoverFetchBehindACheckpoint(t *testing.T) {
	measureOnly(t)
	for _, n := range measuredSizes(t, []int{500000}) {
		h := newHarnessWith(t, []string{"n1", "n3"}, 2, 2, func(o *Options) { o.AckTimeout = time.Second })
		m := shardmap.Map{Version: 1, Basis: []string{"n1", "n3"}, Replicas: 2, Shards: []shardmap.Shard{
			{Leader: "n1", Followers: []string{"n3"}}, {Leader: "n3", Followers: []string{"n1"}}}}
		h.applyMap(m)
		waitSettled(h, 1)
		id := clientIn(2, 0)
		if got := m.Targets(0, "n1"); len(got) != 1 || got[0] != "n3" {
			t.Fatalf("precondition: X's sources for n1 are %v, want [n3]", got)
		}
		h.setLease("default", id, "n1", 5)
		f1 := h.nodes["n1"].rep.follower
		f1.mu.Lock()
		f1.state.Sessions = append(f1.state.Sessions, &store.SessionState{
			SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2, Version: 5, ExpiryInterval: 300},
			Subscriptions: []store.SubscriptionRecord{{Filter: "b/x", QoS: 1}}})
		f1.mu.Unlock()
		f3 := h.nodes["n3"].rep.follower
		addFiller(f3, n)

		for _, lead := range []time.Duration{20 * time.Millisecond, 200 * time.Millisecond} {
			c0, h0 := holdsOf(f3, holdCheckpoint)
			var holdEnd time.Time
			done := make(chan struct{})
			start := time.Now()
			go func() {
				f3.checkpointState(func() uint64 { holdEnd = time.Now(); return 1 })
				close(done)
			}()
			time.Sleep(lead)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			asked := time.Now()
			got := h.nodes["n3"].rep.FetchKept(ctx, "default", id, "n1")
			took := time.Since(asked)
			cancel()
			<-done
			c1, h1 := holdsOf(f3, holdCheckpoint)
			if c1-c0 != 1 {
				t.Fatalf("%d checkpoint holds recorded, want 1", c1-c0)
			}
			t.Logf("measure copies=%d lead_ms=%d hold_ms=%.1f fetch_asked_ms=%.1f hold_end_ms=%.1f fetch_took_ms=%.1f state=%v answered=%v",
				n, lead.Milliseconds(), msOf(h1-h0), msOf(asked.Sub(start)), msOf(holdEnd.Sub(start)), msOf(took), got.State != nil, got.Answered)
		}
	}
}
