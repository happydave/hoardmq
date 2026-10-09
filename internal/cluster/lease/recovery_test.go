package lease

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// recoveredWithAStaleSession brings up n1 and n2, gives x's lease to n1
// offline, puts a copy of x's session on n2 as if n2 had restarted holding it,
// and runs n2's recovery.
func recoveredWithAStaleSession(t *testing.T) (n1, n2 *tnode, l Lease) {
	t.Helper()
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, func(string) string { return "" })
	n1, n2 = c.nodes["n1"], c.nodes["n2"]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := n1.svc.Acquire(ctx, "default", "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	n1.svc.Disconnected("default", "x", res.Epoch, time.Now().Add(time.Hour), false)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l, ok := n2.svc.Table().Lookup("default", "x"); ok && !l.Online {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	l, ok := n2.svc.Table().Lookup("default", "x")
	if !ok || l.Owner != "n1" || l.Online {
		t.Fatalf("precondition not reached: n2's table shows x as %+v, %v; want offline on n1", l, ok)
	}
	n2.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x"},
		Subscriptions: []store.SubscriptionRecord{{Filter: "t/x", QoS: 1}}})
	n2.svc.recovered()
	return n1, n2, l
}

// TestRecoveredReconcilesTimersAndStaleSessions: at the end of recovery the
// service starts timers for the offline leases it owns and seals a local
// session whose lease belongs to another node, at that lease's token. The
// sealed copy answers the owner's pull: a restarted node's journal can hold the
// only copy of a session its new owner has not fetched yet (WI 1742).
func TestRecoveredReconcilesTimersAndStaleSessions(t *testing.T) {
	n1, n2, l := recoveredWithAStaleSession(t)
	if n2.sessions.has("default", "x") {
		t.Fatal("the stale session is still held after recovery")
	}
	st, token, ok := n2.sessions.Sealed("default", "x")
	if !ok || token != l.Epoch {
		t.Fatalf("sealed %v at token %d; want sealed at the lease's token %d", ok, token, l.Epoch)
	}
	if len(st.Subscriptions) != 1 {
		t.Fatalf("the sealed copy lost the session's subscriptions: %+v", st)
	}
	n2.sessions.mu.Lock()
	evicted := append([]string(nil), n2.sessions.evicted...)
	n2.sessions.mu.Unlock()
	if len(evicted) != 0 {
		t.Fatalf("recovery evicted %v; want nothing deleted before the owner has the session", evicted)
	}
	if got := n2.svc.LivenessStats().SealedByRecovery; got != 1 {
		t.Fatalf("sealed_by_recovery %d; want 1", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pulled, answered, superseded, _ := n1.svc.pull(ctx, "n2", "default", "x", l.Epoch, Requester{})
	if !answered || superseded || pulled == nil || len(pulled.Subscriptions) != 1 {
		t.Fatalf("the owner's pull: state %+v, answered %v, superseded %v; want the sealed copy", pulled, answered, superseded)
	}
	// The owner's recovery starts the offline lease's timers and leaves the
	// session it holds for that lease where it is.
	n1.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x"}})
	n1.svc.cancelTimers(key{"default", "x"})
	n1.svc.recovered()
	n1.svc.mu.Lock()
	_, has := n1.svc.timers[key{"default", "x"}]
	n1.svc.mu.Unlock()
	if !has {
		t.Fatal("owner did not start timers for its offline lease at recovery")
	}
	if _, _, sealed := n1.sessions.Sealed("default", "x"); sealed || !n1.sessions.has("default", "x") {
		t.Fatalf("the owner's own session: sealed %v, held %v; want held", sealed, n1.sessions.has("default", "x"))
	}
}

// TestRecoveryEvictsWithTheHandOverRemoving: with the injected defect that
// restores remove-then-send, recovery deletes the stale session as it did
// before the seal, and the owner's pull finds nothing.
func TestRecoveryEvictsWithTheHandOverRemoving(t *testing.T) {
	fixtureOptions = func(o *Options) { o.RemoveOnHandOver = true }
	t.Cleanup(func() { fixtureOptions = nil })
	n1, n2, l := recoveredWithAStaleSession(t)
	n2.sessions.mu.Lock()
	evicted := append([]string(nil), n2.sessions.evicted...)
	n2.sessions.mu.Unlock()
	if len(evicted) != 1 || evicted[0] != "default/x" {
		t.Fatalf("evicted %v; want default/x", evicted)
	}
	if _, _, ok := n2.sessions.Sealed("default", "x"); ok {
		t.Fatal("sealed with the defect on")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if pulled, answered, _, _ := n1.svc.pull(ctx, "n2", "default", "x", l.Epoch, Requester{}); !answered || pulled != nil {
		t.Fatalf("the owner's pull: state %+v, answered %v; want answered with nothing", pulled, answered)
	}
}

// TestRecoveryChoice: recovery seals a session whose lease another node holds,
// keeps one whose lease is its own or absent, and evicts only with the
// injected defect on. Evicting journals a delete, so each case is a decision
// the function makes without acting.
func TestRecoveryChoice(t *testing.T) {
	for _, c := range []struct {
		name             string
		found            bool
		owner            string
		removeOnHandOver bool
		want             evictChoice
	}{
		{"no lease", false, "", false, evictKeep},
		{"no lease, defect on", false, "", true, evictKeep},
		{"lease here", true, "n2", false, evictKeep},
		{"lease here, defect on", true, "n2", true, evictKeep},
		{"lease elsewhere", true, "n1", false, evictSeal},
		{"lease elsewhere, defect on", true, "n1", true, evictNow},
	} {
		if got := recoveryChoice(c.found, c.owner, "n2", c.removeOnHandOver); got != c.want {
			t.Errorf("%s: %v; want %v", c.name, got, c.want)
		}
	}
}

// TestRecoverySealEndsOnceATargetHoldsTheSession: nothing confirms a seal made
// at recovery when the new owner already had the session. The seal is kept
// past its bound while no target of the owner shows the session at the
// lease's token, and ends, with its delete, once one does.
func TestRecoverySealEndsOnceATargetHoldsTheSession(t *testing.T) {
	held := newFakeHeld()
	fixtureOptions = func(o *Options) {
		o.SealTimeout = 300 * time.Millisecond
		o.HeldAtTargets = held.check
	}
	t.Cleanup(func() { fixtureOptions = nil })
	_, n2, l := recoveredWithAStaleSession(t)
	if _, _, ok := n2.sessions.Sealed("default", "x"); !ok {
		t.Fatal("precondition not reached: recovery did not seal x")
	}
	time.Sleep(700 * time.Millisecond) // two bounds
	if _, _, ok := n2.sessions.Sealed("default", "x"); !ok {
		t.Fatal("the recovery's seal was dropped while no target of its owner held the session")
	}
	held.set("x", l.Epoch)
	waitFor(t, "the recovery's seal to end once a target holds x", 5*time.Second, func() bool {
		n2.sessions.mu.Lock()
		defer n2.sessions.mu.Unlock()
		return len(n2.sessions.dropped) == 1 && n2.sessions.dropped[0] == "default/x"
	})
	if ls := n2.svc.LivenessStats(); ls.SealsHeld != 1 || ls.SealsExpired != 1 {
		t.Fatalf("counters %+v", ls)
	}
}
