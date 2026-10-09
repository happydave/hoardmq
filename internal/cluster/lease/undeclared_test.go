package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func TestOwnerUnaccounted(t *testing.T) {
	for _, c := range []struct {
		name        string
		prev        string
		asked, live bool
		want        bool
	}{
		{"not asked, alive by the log", "n9", false, true, true},
		{"not asked, declared dead", "n9", false, false, false},
		{"asked", "n9", true, true, false},
		{"no previous owner", "", false, true, false},
		{"this node", "n1", false, true, false},
	} {
		if got := ownerUnaccounted(c.prev, "n1", c.asked, c.live); got != c.want {
			t.Errorf("%s: ownerUnaccounted = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestHeldNowhere(t *testing.T) {
	for _, c := range []struct {
		answered, unaccounted, want bool
	}{{true, false, true}, {true, true, false}, {false, false, false}, {false, true, false}} {
		if got := heldNowhere(c.answered, c.unaccounted); got != c.want {
			t.Errorf("heldNowhere(answered %v, unaccounted %v) = %v, want %v", c.answered, c.unaccounted, got, c.want)
		}
	}
}

func TestOwnerWait(t *testing.T) {
	for _, c := range []struct {
		name                  string
		prev                  string
		asked, answered, live bool
		want                  undeclaredWait
	}{
		{"not asked, alive by the log", "n9", false, true, true, waitOwner},
		{"asked, answered, alive", "n9", true, true, true, waitNone},
		{"asked, silent, alive", "n9", true, false, true, waitOwnerSilent},
		{"asked, silent, declared dead", "n9", true, false, false, waitNone},
		{"not asked, declared dead", "n9", false, true, false, waitNone},
		{"no previous owner", "", false, true, true, waitNone},
		{"this node", "n1", false, false, true, waitNone},
	} {
		if got := ownerWait(c.prev, "n1", c.asked, c.answered, c.live); got != c.want {
			t.Errorf("%s: ownerWait = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAdoptionGivesUp(t *testing.T) {
	for _, c := range []struct {
		name  string
		spent int
		wait  undeclaredWait
		want  bool
	}{
		{"an unreachable owner, long past the retries", 40, waitOwner, false},
		{"a silent owner, long past the retries", 40, waitOwnerSilent, false},
		{"a down holder, long past the retries", 40, waitHolder, false},
		{"a peer unanswered, at the retries", 6, waitNone, false},
		{"a peer unanswered, retries spent", 7, waitNone, true},
	} {
		if got := adoptionGivesUp(c.spent, 6, c.wait); got != c.want {
			t.Errorf("%s: adoptionGivesUp = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestNoStateVerdict(t *testing.T) {
	for _, c := range []struct {
		name                      string
		owner                     undeclaredWait
		pending, holder, answered bool
		got                       fetchResult
		wait                      undeclaredWait
	}{
		{"an unreachable owner, with a pending owner, a down holder and a silent source", waitOwner, true, true, false, fetchUnanswered, waitOwner},
		{"a silent owner, with a down holder and a silent source", waitOwnerSilent, false, true, false, fetchUnanswered, waitOwnerSilent},
		{"a pending owner, with a down holder and a silent source", waitNone, true, true, false, fetchUnanswered, waitOwner},
		{"a down holder and a silent source", waitNone, false, true, false, fetchUnanswered, waitHolder},
		{"a down holder, every node asked answered", waitNone, false, true, true, fetchUnanswered, waitHolder},
		{"a silent source alone", waitNone, false, false, false, fetchUnanswered, waitNone},
		{"nothing, every node asked answered", waitNone, false, false, true, fetchEmpty, waitNone},
	} {
		got, wait := noStateVerdict(c.owner, c.pending, c.holder, c.answered)
		if got != c.got || wait != c.wait {
			t.Errorf("%s: noStateVerdict = %v, %v; want %v, %v", c.name, got, wait, c.got, c.wait)
		}
	}
}

func TestGiveUpServes(t *testing.T) {
	for _, c := range []struct {
		name string
		wait undeclaredWait
		kept bool
		want giveUpServe
	}{
		{"a silent source, a kept copy found", waitNone, true, serveKept},
		{"a silent source, none found", waitNone, false, serveFresh},
		{"a down holder, a kept copy found", waitHolder, true, serveFresh},
		{"an undeclared owner, a kept copy found", waitOwner, true, serveFresh},
	} {
		if got := giveUpServes(c.wait, c.kept); got != c.want {
			t.Errorf("%s: giveUpServes = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestWaitPastBound(t *testing.T) {
	bound := time.Minute
	for _, c := range []struct {
		name string
		wait undeclaredWait
		age  time.Duration
		want bool
	}{
		{"an unreachable owner, past", waitOwner, bound + time.Millisecond, true},
		{"a silent owner, past", waitOwnerSilent, bound + time.Millisecond, true},
		{"an unreachable owner, at the bound", waitOwner, bound, false},
		{"a down holder, past", waitHolder, bound + time.Millisecond, true},
		{"a down holder, at the bound", waitHolder, bound, false},
		{"nothing alive, past", waitNone, time.Hour, false},
	} {
		if got := waitPastBound(c.wait, c.age, bound); got != c.want {
			t.Errorf("%s: waitPastBound = %v, want %v", c.name, got, c.want)
		}
	}
}

// undeclaredOwner gives id's lease to n9: a node the log says is alive and
// that no node has a link to, since no transport knows it. Its alive record
// is proposed through via every 200 ms until the returned stop, after which
// the leader declares it dead a ttl later, as it declares a crashed node.
func undeclaredOwner(t *testing.T, via *tnode, id string) (stop func()) {
	t.Helper()
	propose := func(op Op) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if res, err := via.svc.submit(ctx, op); err != nil || !res.OK {
			t.Errorf("proposing %v for n9: %+v, %v", op.Kind, res, err)
		}
	}
	propose(Op{Kind: OpAlive, Node: "n9", Now: time.Now()})
	propose(Op{Kind: OpAcquire, Namespace: "default", ClientID: id, Node: "n9", ExpiryInterval: 300, Now: time.Now()})
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				propose(Op{Kind: OpAlive, Node: "n9", Now: time.Now()})
			}
		}
	}()
	stop = sync.OnceFunc(func() { close(done); wg.Wait() })
	t.Cleanup(stop)
	return stop
}

func declared(t *testing.T, n *tnode, node string) {
	t.Helper()
	waitFor(t, n.name+"'s table to show "+node+" declared dead", 15*time.Second, func() bool { return !n.svc.table.Live(node) })
}

// undeclaredCluster is three nodes whose replica fallback is fb, with x owned
// by n9 (undeclaredOwner) and n2 seeing it so. The adoption worker is held
// back, so the client's CONNECT is the fetcher.
func undeclaredCluster(t *testing.T, fb func(context.Context, string, string, string) (*store.SessionState, bool)) (b *tnode, stop func()) {
	t.Helper()
	holdWorkerBack(t)
	fixtureFallback = fb
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b = c.nodes["n2"]
	stop = undeclaredOwner(t, c.nodes["n1"], "x")
	ownedBy(t, b, "x", "n9")
	return b, stop
}

// TestTakeoverFromAnUndeclaredOwnerFoundNowhereWaitsForTheDeclaration is WI
// 1759's acceptance test on the takeover path. The previous owner could not be
// asked and the log says it is alive; every replica answers "not found". The
// client is refused and the adoption kept until the leader declares the owner
// dead, and then served a fresh session, counted.
func TestTakeoverFromAnUndeclaredOwnerFoundNowhereWaitsForTheDeclaration(t *testing.T) {
	b, stop := undeclaredCluster(t, func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true })
	res, err := acquire(t, b, "x", nil, 300, false)
	if !b.svc.table.Live("n9") {
		t.Fatal("precondition not reached: n9 was declared dead before the takeover was judged")
	}
	if !refusedBusy(err) {
		t.Fatalf("takeover from an undeclared owner, found nowhere: %+v, %v; want Server Busy", res, err)
	}
	k := key{"default", "x"}
	b.svc.adopting.mu.Lock()
	ad := b.svc.adopting.m[k]
	b.svc.adopting.mu.Unlock()
	if ad == nil || ad.job.prev != "n9" || !ad.job.takeover {
		t.Fatalf("adoption %+v; want one kept with n9 as its previous owner", ad)
	}
	if st := b.svc.LivenessStats(); st.TakeoverOwnerUndeclared != 1 || st.TakeoverUnanswered != 1 || st.TakeoverEmpty != 0 {
		t.Fatalf("counters: undeclared %d, unanswered %d, empty %d; want 1, 1, 0", st.TakeoverOwnerUndeclared, st.TakeoverUnanswered, st.TakeoverEmpty)
	}
	// A retry before the declaration is refused again.
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) || !b.svc.table.Live("n9") {
		t.Fatalf("retry before the declaration: %v, n9 live %v; want Server Busy while n9 is live", err, b.svc.table.Live("n9"))
	}
	stop()
	declared(t, b, "n9")
	res, err = acquire(t, b, "x", nil, 300, false)
	wantEmpty(t, "retry after the declaration", res, err, EmptyAdoption, "n9")
	if n := b.svc.adopting.unfinished(); n != 0 {
		t.Fatalf("%d adoptions unfinished after the fresh session was served", n)
	}
}

// TestTakeoverFromAnUndeclaredOwnerWithACopyIsServedAtOnce: a copy on a
// replica is served without waiting for anything.
func TestTakeoverFromAnUndeclaredOwnerWithACopyIsServedAtOnce(t *testing.T) {
	b, _ := undeclaredCluster(t, peerFetch(replicaOf("x")))
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || !hasFilter(res.State, "t/replica") || res.Empty.Reason != "" {
		t.Fatalf("takeover from an undeclared owner with a replica copy: state %v, evidence %+v, err %v; want the copy", res.State, res.Empty, err)
	}
	if st := b.svc.LivenessStats(); st.TakeoverOwnerUndeclared != 1 || st.TakeoverOwnerUndeclaredServed != 1 || b.svc.adopting.unfinished() != 0 {
		t.Fatalf("counters: undeclared %d, served %d, unfinished %d; want 1, 1, 0", st.TakeoverOwnerUndeclared, st.TakeoverOwnerUndeclaredServed, b.svc.adopting.unfinished())
	}
}

// undeclaredAdoption is undeclaredCluster with the worker running on opts,
// after the takeover that records the adoption.
func undeclaredAdoption(t *testing.T, opts func(o *Options)) (b *tnode, stop func()) {
	t.Helper()
	fixtureOptions = opts
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b = c.nodes["n2"]
	stop = undeclaredOwner(t, c.nodes["n1"], "x")
	ownedBy(t, b, "x", "n9")
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: the takeover was not refused (%v)", err)
	}
	return b, stop
}

// TestWorkerWaitsForAnUndeclaredOwnersDeclaration: its retries spent, the
// worker keeps the adoption while the previous owner is undeclared, and ends
// it as found nowhere once the owner is declared.
func TestWorkerWaitsForAnUndeclaredOwnersDeclaration(t *testing.T) {
	b, stop := undeclaredAdoption(t, func(o *Options) {
		o.AdoptRetries, o.AdoptRetryDelay, o.SealTimeout = 1, 5*time.Millisecond, time.Minute
	})
	waitFor(t, "the worker to fetch past its retries", 10*time.Second, func() bool {
		return b.svc.LivenessStats().AdoptOwnerUndeclared >= 4
	})
	if st := b.svc.LivenessStats(); st.AdoptGaveUp != 0 || !b.svc.adopting.unfinishedFor(key{"default", "x"}) || !b.svc.table.Live("n9") {
		t.Fatalf("after %d fetches with retries 1: gave up %d, unfinished %v, n9 live %v; want the adoption kept while n9 is live", st.AdoptOwnerUndeclared, st.AdoptGaveUp, b.svc.adopting.unfinishedFor(key{"default", "x"}), b.svc.table.Live("n9"))
	}
	stop()
	declared(t, b, "n9")
	waitFor(t, "the worker to end the adoption", 15*time.Second, func() bool { return b.svc.adopting.unfinished() == 0 })
	if st := b.svc.LivenessStats(); st.AdoptWorkerEmpty != 1 || st.AdoptGaveUp != 0 {
		t.Fatalf("after the declaration: worker empty %d, gave up %d; want 1, 0", st.AdoptWorkerEmpty, st.AdoptGaveUp)
	}
}

// TestWorkerKeepsWaitingOnAnUndeclaredOwnerPastTheBound: an owner that stays
// alive and out of reach may hold the session sealed for as long, so neither
// the retries nor the bound end the wait; the wait past the bound is counted
// once, and the declaration ends it.
func TestWorkerKeepsWaitingOnAnUndeclaredOwnerPastTheBound(t *testing.T) {
	b, stop := undeclaredAdoption(t, func(o *Options) {
		o.AdoptRetries, o.AdoptRetryDelay, o.SealTimeout = 1, 5*time.Millisecond, 300*time.Millisecond
	})
	began := time.Now()
	waitFor(t, "six fetches and twice the bound", 10*time.Second, func() bool {
		return b.svc.LivenessStats().AdoptOwnerUndeclared >= 6 && time.Since(began) > 600*time.Millisecond
	})
	st := b.svc.LivenessStats()
	if st.AdoptGaveUp != 0 || !b.svc.adopting.unfinishedFor(key{"default", "x"}) || !b.svc.table.Live("n9") {
		t.Fatalf("after %d fetches, past the bound with retries 1: gave up %d, unfinished %v, n9 live %v; want the adoption kept while n9 is live",
			st.AdoptOwnerUndeclared, st.AdoptGaveUp, b.svc.adopting.unfinishedFor(key{"default", "x"}), b.svc.table.Live("n9"))
	}
	if st.AdoptWaitsPastBound != 1 || st.AdoptWaitingNow != 1 {
		t.Fatalf("waits past the bound %d, waiting now %d; want 1, 1", st.AdoptWaitsPastBound, st.AdoptWaitingNow)
	}
	stop()
	declared(t, b, "n9")
	waitFor(t, "the worker to end the adoption", 15*time.Second, func() bool { return b.svc.adopting.unfinished() == 0 })
	if st := b.svc.LivenessStats(); st.AdoptWorkerEmpty != 1 || st.AdoptGaveUp != 0 || st.AdoptWaitingNow != 0 || st.AdoptWaitsPastBound != 1 {
		t.Fatalf("after the declaration: worker empty %d, gave up %d, waiting now %d, waits past the bound %d; want 1, 0, 0, 1",
			st.AdoptWorkerEmpty, st.AdoptGaveUp, st.AdoptWaitingNow, st.AdoptWaitsPastBound)
	}
}

// TestSilentSourceDoesNotHideAnUndeclaredOwner: every replica source stays
// silent while the previous owner is out of reach and alive by the log. The
// owner may hold the session sealed, so the wait is on it, not on the
// sources' retries.
func TestSilentSourceDoesNotHideAnUndeclaredOwner(t *testing.T) {
	fixtureOptions = func(o *Options) {
		o.AdoptRetries, o.AdoptRetryDelay, o.SealTimeout = 1, 5*time.Millisecond, time.Minute
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, false }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b := c.nodes["n2"]
	undeclaredOwner(t, c.nodes["n1"], "x")
	ownedBy(t, b, "x", "n9")
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: the takeover was not refused (%v)", err)
	}
	waitFor(t, "the worker to fetch past its retries, or give up", 10*time.Second, func() bool {
		st := b.svc.LivenessStats()
		return st.AdoptWorkerUnanswered >= 4 || st.AdoptGaveUp >= 1
	})
	if st := b.svc.LivenessStats(); st.AdoptGaveUp != 0 || st.AdoptOwnerUndeclared < 4 || !b.svc.adopting.unfinishedFor(key{"default", "x"}) {
		t.Fatalf("after %d fetches with silent sources and retries 1: gave up %d, unfinished %v; want the adoption kept for n9",
			st.AdoptOwnerUndeclared, st.AdoptGaveUp, b.svc.adopting.unfinishedFor(key{"default", "x"}))
	}
}

// TestSilentSourceGetsItsWholeWindowAfterAnOwnerWait: the worker waits on an
// undeclared owner for more fetches than its retries, then the owner is
// declared and only a silent source is left. It is given up after the full
// retries counted from the first fetch that waited on that source alone.
func TestSilentSourceGetsItsWholeWindowAfterAnOwnerWait(t *testing.T) {
	fixtureOptions = func(o *Options) {
		o.AdoptRetries, o.AdoptRetryDelay, o.SealTimeout = 2, 5*time.Millisecond, time.Minute
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, false }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b := c.nodes["n2"]
	stop := undeclaredOwner(t, c.nodes["n1"], "x")
	ownedBy(t, b, "x", "n9")
	if _, err := acquire(t, b, "x", nil, 300, false); !refusedBusy(err) {
		t.Fatalf("precondition not reached: the takeover was not refused (%v)", err)
	}
	waitFor(t, "the worker to fetch past its retries, or give up", 10*time.Second, func() bool {
		st := b.svc.LivenessStats()
		return st.AdoptWorkerUnanswered >= 4 || st.AdoptGaveUp >= 1
	})
	if st := b.svc.LivenessStats(); st.AdoptGaveUp != 0 || st.AdoptOwnerUndeclared < 4 {
		t.Fatalf("before the declaration: gave up %d after %d fetches on n9; want the wait on n9 kept past the retries", st.AdoptGaveUp, st.AdoptOwnerUndeclared)
	}
	stop()
	declared(t, b, "n9")
	waitFor(t, "the worker to give the adoption up", 30*time.Second, func() bool { return b.svc.LivenessStats().AdoptGaveUp >= 1 })
	st := b.svc.LivenessStats()
	if silent := st.AdoptWorkerUnanswered - st.AdoptOwnerUndeclared; silent != 3 {
		t.Fatalf("given up after %d fetches that waited on the silent source alone (%d unanswered, %d on n9); want 3, the retries plus one",
			silent, st.AdoptWorkerUnanswered, st.AdoptOwnerUndeclared)
	}
}

// TestRefusalPastTheBoundNamesTheLeader: a CONNECT refused for an adoption
// waiting on an undeclared owner is told Server Busy until the adoption is
// older than SealTimeout, then Use Another Server naming the leader, which a
// node whose wait is that long hears. On the leader itself it stays Server
// Busy. Either way the adoption stays.
func TestRefusalPastTheBoundNamesTheLeader(t *testing.T) {
	fixtureOptions = func(o *Options) {
		o.AdoptRetryDelay, o.SealTimeout = time.Hour, 500*time.Millisecond
	}
	t.Cleanup(func() { fixtureOptions = nil })
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, func(name string) string { return "client." + name + ":1883" })
	via := c.nodes["n1"]
	undeclaredOwner(t, via, "x")
	undeclaredOwner(t, via, "y")
	leader := ""
	waitFor(t, "every node to know the leader", 10*time.Second, func() bool {
		leader, _ = c.nodes["n1"].svc.leaderHint.Load().(string)
		for _, n := range c.nodes {
			if h, _ := n.svc.leaderHint.Load().(string); h == "" || h != leader {
				return false
			}
		}
		return leader != ""
	})
	follower := "n1"
	if leader == "n1" {
		follower = "n2"
	}
	for _, cs := range []struct {
		node, id string
		want     packet.ReasonCode
	}{{follower, "x", packet.UseAnotherServer}, {leader, "y", packet.ServerBusy}} {
		n := c.nodes[cs.node]
		ownedBy(t, n, cs.id, "n9")
		began := time.Now()
		if _, err := acquire(t, n, cs.id, nil, 300, false); !refusedBusy(err) {
			t.Fatalf("%s: precondition not reached: the takeover was not refused (%v)", cs.node, err)
		}
		if _, err := acquire(t, n, cs.id, nil, 300, false); time.Since(began) < 500*time.Millisecond && !refusedBusy(err) {
			t.Fatalf("%s: a retry before the bound: %v, want Server Busy", cs.node, err)
		}
		time.Sleep(time.Until(began.Add(700 * time.Millisecond)))
		_, err := acquire(t, n, cs.id, nil, 300, false)
		var r *Refusal
		if !errors.As(err, &r) || r.Reason != cs.want || (cs.want == packet.UseAnotherServer && r.ServerReference != "client."+leader+":1883") {
			t.Fatalf("%s (leader %s): a retry past the bound: %v, want %s", cs.node, leader, err, cs.want)
		}
		if !n.svc.adopting.unfinishedFor(key{"default", cs.id}) {
			t.Fatalf("%s: the adoption ended with the refusal", cs.node)
		}
		if st := n.svc.LivenessStats(); st.AdoptWaitsPastBound != 1 || (cs.want == packet.UseAnotherServer) != (st.AdoptRedirected == 1) {
			t.Fatalf("%s: waits past the bound %d, redirected %d", cs.node, st.AdoptWaitsPastBound, st.AdoptRedirected)
		}
	}
}
