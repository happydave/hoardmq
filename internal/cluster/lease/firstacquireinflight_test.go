package lease

import (
	"testing"
	"time"
)

// firstAcquireRaceCluster is three nodes with no lease for x, every adoption
// worker held back, and n2's acquires of x passing through before (held before
// the proposal) and taking (held after a grant from another owner, before the
// pull).
func firstAcquireRaceCluster(t *testing.T) (n1, n2 *tnode, before, taking *acquireHold) {
	t.Helper()
	before, taking = newAcquireHold(false), newAcquireHold(true)
	fixtureFallback = foundNowhere
	t.Cleanup(func() { fixtureFallback = nil })
	fixtureOptions = func(o *Options) {
		heldBack(o)
		if o.Node == "n2" {
			o.AcquireGate = func(ns, id string, granted bool) {
				before.gate(ns, id, granted)
				taking.gate(ns, id, granted)
			}
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(before.unblock)
	t.Cleanup(taking.unblock)
	return c.nodes["n1"], c.nodes["n2"], before, taking
}

// TestAFirstAcquireRacedIsNotAnsweredWhileATakeoverIsInFlight: a CONNECT of x
// on n2 finds no lease and is held before it proposes. x then connects on n1,
// which creates the lease and holds the session. Another CONNECT on n2 takes
// the lease from n1 and is held before its pull. The held CONNECT's acquire
// applies after that grant, so the lease is already n2's: it must not be
// answered before the takeover, which carries the session, has ended. The
// same race with a lease found at the lookup is
// TestSecondConnectRacedIsNotAnsweredWhileTheFirstsTakeoverIsInFlight.
func TestAFirstAcquireRacedIsNotAnsweredWhileATakeoverIsInFlight(t *testing.T) {
	n1, n2, before, taking := firstAcquireRaceCluster(t)
	before.armed.Store(true)
	held := acquireAsyncAt(n2, "x", 300)
	before.wait(t, "a CONNECT of x on n2 found no lease and is held before its proposal")
	if _, err := acquire(t, n1, "x", nil, 300, false); err != nil {
		t.Fatal(err)
	}
	n1.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, n2, "x", "n1")
	taking.armed.Store(true)
	takeover := acquireAsyncAt(n2, "x", 300)
	taking.wait(t, "another CONNECT on n2 took x from n1 and is held before its pull")
	ownedBy(t, n2, "x", "n2")
	before.unblock()
	if o, early := answeredWhileHeld(held, 500*time.Millisecond); early {
		t.Fatalf("the CONNECT that found no lease was answered while the takeover was in flight: token %d, previous owner %q, state %v, evidence %+v, err %v", o.res.Epoch, o.res.PrevOwner, o.res.State != nil, o.res.Empty, o.err)
	}
	taking.unblock()
	tk := outcomeOf(t, takeover)
	if tk.err != nil || tk.res.PrevOwner != "n1" || tk.res.State == nil {
		t.Fatalf("precondition not reached: the takeover %+v (err %v), want the session taken from n1", tk.res, tk.err)
	}
	o := outcomeOf(t, held)
	if o.err != nil || o.res.Epoch != tk.res.Epoch {
		t.Fatalf("the CONNECT that found no lease, after the takeover: %+v, %v; want served at the takeover's token %d", o.res, o.err, tk.res.Epoch)
	}
	if st := n2.svc.LivenessStats(); st.ConnectsInFlight != 0 || st.TakeoverWaitsServed != 1 {
		t.Fatalf("after both: %d CONNECTs in flight, %d takeover waits served; want 0 and 1", st.ConnectsInFlight, st.TakeoverWaitsServed)
	}
}

func TestGrantLeftItHere(t *testing.T) {
	for _, c := range []struct {
		prev string
		want bool
	}{
		{"", true},
		{"n2", true},
		{"n1", false},
	} {
		if got := grantLeftItHere(c.prev, "n2"); got != c.want {
			t.Errorf("grantLeftItHere(%q, n2) = %v, want %v", c.prev, got, c.want)
		}
	}
}
