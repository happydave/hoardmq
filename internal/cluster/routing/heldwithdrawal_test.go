package routing

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A session that left this node during a move has its route's withdrawal
// held for the grace, on a timer. When the client comes back here as the
// timer fires, the withdrawal must not land over the return's route: the
// dispatcher, holding the returned subscriber, tells the plane nothing more,
// and a publish made on another node would find no subscriber anywhere.

// heldPlane is movingPlane counting into m.
func heldPlane(t *testing.T, grace time.Duration, m *labelMetrics) *Plane {
	t.Helper()
	tr := newStubTransport()
	p := New(Options{Node: "n1", Transport: tr, SyncInterval: time.Hour, MoveGrace: grace, Logger: quiet, Metrics: m,
		Members: func() []string { return []string{"n1"} }, OwnedSessions: func(string) (int, bool) { return 0, true }})
	p.SetSink(&fakeSink{})
	p.Start()
	t.Cleanup(p.Stop)
	return p
}

// heldRace arms the plane so the next own merge (the held withdrawal's, once
// the timer fires) waits before its merge until release is called.
func heldRace(t *testing.T, p *Plane) (entered <-chan struct{}, release func()) {
	t.Helper()
	hold := make(chan struct{})
	rel := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(rel)
	in := make(chan struct{}, 1)
	var armed atomic.Bool
	armed.Store(true)
	p.beforeOwnMerge = func() {
		if armed.CompareAndSwap(true, false) {
			in <- struct{}{}
			<-hold
		}
	}
	return in, rel
}

// TestAReturnAsTheHeldWithdrawalFiresKeepsTheRoute: the withdrawal's timer
// has fired and its merge is still to come when the client's session is
// installed here again. The node holds one subscriber for the filter, so its
// route stays advertised, and the withdrawal sends nothing and is counted
// as superseded.
func TestAReturnAsTheHeldWithdrawalFiresKeepsTheRoute(t *testing.T) {
	m := newLabelMetrics()
	p := heldPlane(t, 50*time.Millisecond, m)
	p.Observe("default", "t/0", 1)
	entered, release := heldRace(t, p)
	handOver(p, "t/0")

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the held withdrawal never fired")
	}
	// The client is back: the dispatcher's count for the filter is 1 again.
	p.Observe("default", "t/0", 1)
	before := p.version.Load()
	sent := m.get("hoardmq_route_updates_total", "direction", "out")
	release()
	// The released merge takes the next own version; a withdrawal decided
	// superseded takes none, so the wait is bounded, not asserted.
	for end := time.Now().Add(time.Second); p.version.Load() == before && time.Now().Before(end); {
		time.Sleep(5 * time.Millisecond)
	}

	if got := p.table.OwnCount("default", "t/0"); got != 1 {
		t.Fatalf("the returned session's subscriber is held here but the node advertises count %d for its filter: %+v", got, p.table.Own())
	}
	if got := m.get("hoardmq_route_updates_total", "direction", "out"); got != sent {
		t.Fatalf("a superseded withdrawal sent %v route updates, want none", got-sent)
	}
	if got := m.get("hoardmq_route_withdrawals_superseded_total"); got != 1 {
		t.Fatalf("superseded withdrawals counted %v, want 1", got)
	}
}

// TestAReturnBeforeTheHeldWithdrawalFiresKeepsTheRoute: the same return
// inside the grace, before the timer fires, stops the withdrawal.
func TestAReturnBeforeTheHeldWithdrawalFiresKeepsTheRoute(t *testing.T) {
	p := movingPlane(t, 200*time.Millisecond)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	p.Observe("default", "t/0", 1)
	time.Sleep(400 * time.Millisecond)
	if got := p.table.OwnCount("default", "t/0"); got != 1 {
		t.Fatalf("a return inside the grace left count %d advertised: %+v", got, p.table.Own())
	}
}

// TestAHeldWithdrawalWithNoReturnWithdrawsTheRoute: with no return the
// withdrawal is written at the grace's end, and nothing is counted
// superseded.
func TestAHeldWithdrawalWithNoReturnWithdrawsTheRoute(t *testing.T) {
	m := newLabelMetrics()
	p := heldPlane(t, 50*time.Millisecond, m)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	waitFor(t, "the withdrawal to be written", 5*time.Second, func() bool {
		e, ok := p.table.Lookup("n1", "default", "t/0")
		return ok && e.Tombstone
	})
	if got := m.get("hoardmq_route_withdrawals_superseded_total"); got != 0 {
		t.Fatalf("superseded withdrawals counted %v with no return, want 0", got)
	}
}

// TestASecondHoldAfterAReturnWithdrawsAtItsOwnFire: the client returns
// inside the grace and leaves again; the second hold is held against the
// return's route and stands when its timer fires.
func TestASecondHoldAfterAReturnWithdrawsAtItsOwnFire(t *testing.T) {
	m := newLabelMetrics()
	p := heldPlane(t, 100*time.Millisecond, m)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	waitFor(t, "the second withdrawal to be written", 5*time.Second, func() bool {
		e, ok := p.table.Lookup("n1", "default", "t/0")
		return ok && e.Tombstone
	})
	if got := m.get("hoardmq_route_withdrawals_superseded_total"); got != 0 {
		t.Fatalf("superseded withdrawals counted %v, want 0", got)
	}
}

func TestHeldWithdrawalStands(t *testing.T) {
	for _, c := range []struct {
		name   string
		heldAt uint64
		cur    Entry
		ok     bool
		want   bool
	}{
		{"the entry it was held against", 7, Entry{Version: 7, Count: 1}, true, true},
		{"announced again since", 7, Entry{Version: 9, Count: 1}, true, false},
		{"no entry, held against none", 0, Entry{}, false, true},
		{"no entry, held against one", 7, Entry{}, false, false},
	} {
		if got := heldWithdrawalStands(c.heldAt, c.cur, c.ok); got != c.want {
			t.Errorf("%s: stands %v, want %v", c.name, got, c.want)
		}
	}
}

// BenchmarkOwnMerge compares the plain own merge, every own change's path,
// with the conditional merge a held withdrawal takes, on a table holding
// 100,000 own entries. Each iteration announces then withdraws one filter.
func BenchmarkOwnMerge(b *testing.B) {
	const n = 100000
	setup := func() (*Table, *atomic.Uint64) {
		t := NewTable("n1")
		var v atomic.Uint64
		for i := 0; i < n; i++ {
			t.MergeOwn(Entry{Namespace: "default", Filter: "f/" + strconv.Itoa(i), Owner: "n1", Count: 1}, &v)
		}
		return t, &v
	}
	b.Run("plain", func(b *testing.B) {
		t, v := setup()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f := "f/" + strconv.Itoa(i%n)
			t.MergeOwn(Entry{Namespace: "default", Filter: f, Owner: "n1", Count: 1}, v)
			t.MergeOwn(Entry{Namespace: "default", Filter: f, Owner: "n1", Tombstone: true}, v)
		}
	})
	b.Run("held", func(b *testing.B) {
		t, v := setup()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f := "f/" + strconv.Itoa(i%n)
			e := t.MergeOwn(Entry{Namespace: "default", Filter: f, Owner: "n1", Count: 1}, v)
			t.MergeOwnIf(Entry{Namespace: "default", Filter: f, Owner: "n1", Tombstone: true}, v, func(cur Entry, ok bool) bool {
				return heldWithdrawalStands(e.Version, cur, ok)
			})
		}
	})
}
