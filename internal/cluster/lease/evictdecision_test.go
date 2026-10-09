package lease

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TestShouldEvict is the predicate on its own. It is pure, so this passes it
// exactly the arguments that would be destructive — a session whose lease has
// gone, with a client connected — without any of them being acted on.
func TestShouldEvict(t *testing.T) {
	for _, tc := range []struct {
		name             string
		owns, gone, here bool
		deferred         int
		want             evictChoice
	}{
		{"lease came back, client here", true, false, true, 0, evictKeep},
		{"lease came back, no client", true, false, false, 0, evictKeep},
		{"lease elsewhere, no client here", false, false, false, 0, evictSeal},
		{"lease elsewhere, client here", false, false, true, 0, evictDefer},
		{"lease elsewhere, client here, one defer used", false, false, true, 1, evictDefer},
		{"lease elsewhere, client here, at the bound", false, false, true, evictDeferBound, evictSeal},
		{"lease elsewhere, client here, past the bound", false, false, true, evictDeferBound + 3, evictSeal},
		{"lease elsewhere, no client, past the bound", false, false, false, evictDeferBound + 3, evictSeal},
		{"lease released, no client", false, true, false, 0, evictNow},
		{"lease released, client here", false, true, true, 0, evictDefer},
		{"lease released, client here, at the bound", false, true, true, evictDeferBound, evictNow},
	} {
		if got := shouldEvict(tc.owns, tc.gone, tc.here, tc.deferred); got != tc.want {
			t.Errorf("%s: shouldEvict(%v,%v,%v,%d) = %v, want %v", tc.name, tc.owns, tc.gone, tc.here, tc.deferred, got, tc.want)
		}
	}

	// The invariant the fix must not break: a session whose lease has left and
	// which has no client here is reclaimed at every deferral count — sealed
	// while another node holds the lease, evicted once no lease exists.
	// Without this the fix could be "stop reclaiming".
	for d := 0; d <= evictDeferBound+2; d++ {
		if got := shouldEvict(false, false, false, d); got != evictSeal {
			t.Errorf("lease elsewhere, no client here at deferred=%d: %v, want evictSeal", d, got)
		}
		if got := shouldEvict(false, true, false, d); got != evictNow {
			t.Errorf("lease released, no client here at deferred=%d: %v, want evictNow", d, got)
		}
	}
}

// TestLostLeaseDoesNotEvictAClientThatCameBack drives lostLease itself, not
// the predicate. WI 1495's own B3 and WI 1511 both shipped a tested function
// that nothing called; this is the control against that.
func TestLostLeaseDoesNotEvictAClientThatCameBack(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 5*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]

	const kept = "came-back"
	const gone = "never-returned"
	for _, id := range []string{kept, gone} {
		if _, err := acquire(t, b, id, nil, 300, true); err != nil {
			t.Fatal(err)
		}
		b.sessions.put("default", id, &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if res, err := a.svc.AcquireFrom(ctx, "default", id, nil, 300, false, Requester{}); err != nil || res.PrevOwner != b.name {
			cancel()
			t.Fatalf("takeover of %s: %+v %v", id, res, err)
		}
		cancel()
	}

	// serveTakeover seals the session, which in the fake removes the state,
	// so both sessions are gone by now for a reason that has nothing to do
	// with the deferred reclaim. Put them back: what is under test is what the
	// timer scheduled by lostLease does when it fires, and it fires later.
	//
	// The first version of this test skipped this and passed two of its three
	// assertions by observing Take's effect instead. Only the deferral counter
	// caught it.
	for _, id := range []string{kept, gone} {
		b.sessions.put("default", id, &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2}})
	}
	// One of the two clients has reconnected to b — the case the old guard
	// could not see, because the lease is still a's while the takeover settles.
	b.sessions.setConnAge("default", kept, 200*time.Millisecond)

	// b's TakeoverTimeout is 1 s in the harness, so the reclaim runs about 2 s
	// after the lease left. Wait for it to have acted on the client-less one.
	waitFor(t, "the deferred reclaim ran", 10*time.Second, func() bool {
		return !b.sessions.has("default", gone)
	})

	if !b.sessions.has("default", kept) {
		t.Error("the client that came back was evicted — the defect this work item exists to fix")
	}
	if got := b.svc.LivenessStats().EvictDeferrals; got == 0 {
		t.Error("no deferral was counted, so nothing shows the eviction was held off")
	}
	if got := b.svc.LivenessStats().EvictBoundHit; got != 0 {
		t.Errorf("the bound was hit %d times in a 2 s window; it should take %d deferrals", got, evictDeferBound)
	}
}
