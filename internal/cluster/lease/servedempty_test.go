package lease

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

func wantEmpty(t *testing.T, what string, res AcquireResult, err error, reason, owner string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if res.Empty.Reason != reason || res.Empty.Owner != owner || res.Empty.Token != res.Epoch || res.State != nil {
		t.Fatalf("%s: evidence %+v, state %v, token %d; want reason %q, owner %q, the served token", what, res.Empty, res.State != nil, res.Epoch, reason, owner)
	}
}

// TestEmptyTakeoverCarriesItsEvidence: the previous owner answered that it
// holds nothing and every replica answered "not found".
func TestEmptyTakeoverCarriesItsEvidence(t *testing.T) {
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, b, "x", "n1")
	res, err := acquire(t, b, "x", nil, 300, false)
	wantEmpty(t, "takeover", res, err, EmptyTakeover, "n1")
}

// TestTakeoverWithStateCarriesNoEvidence: the previous owner handed the
// session over.
func TestTakeoverWithStateCarriesNoEvidence(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	a.sessions.put("default", "x", sessionFor("x"))
	ownedBy(t, b, "x", "n1")
	res, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || res.State == nil || res.Empty.Reason != "" {
		t.Fatalf("takeover with state: state %v, evidence %+v, err %v; want the state and no evidence", res.State != nil, res.Empty, err)
	}
}

// TestEmptyAdoptionCarriesItsEvidence: this CONNECT fetched its adoption and
// every source answered "not found".
func TestEmptyAdoptionCarriesItsEvidence(t *testing.T) {
	f := newAdoptionFixtureWith(t, false, func(int) (*store.SessionState, bool) { return nil, true })
	res, err := acquire(t, f.n, "c", nil, 60, false)
	wantEmpty(t, "adoption", res, err, EmptyAdoption, "n9")
}

// TestLocalLeaseWithNothingFetchedCarriesItsEvidence: the lease is this
// node's, no adoption is left and no session is held; the table's online flag
// says whether an acquire granted it or it is offline here.
func TestLocalLeaseWithNothingFetchedCarriesItsEvidence(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	// Granted by an acquire whose CONNECT never took a session here: online.
	if _, err := acquire(t, n, "g", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, n, "g", "n1")
	res, err := acquire(t, n, "g", nil, 300, false)
	wantEmpty(t, "online, nothing held", res, err, EmptyGrantUnfetched, "n1")
	// Offline here with no adoption left.
	first, err := acquire(t, n, "o", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	n.svc.Disconnected("default", "o", first.Epoch, time.Now().Add(time.Hour), false)
	waitFor(t, "the lease to go offline", 5*time.Second, func() bool {
		l, ok := n.svc.table.Lookup("default", "o")
		return ok && !l.Online
	})
	res, err = acquire(t, n, "o", nil, 300, false)
	wantEmpty(t, "offline, no adoption", res, err, EmptyAdoptionEnded, "n1")
}

// TestNoEvidenceOnACleanStartOrAFirstConnect: a clean start asked for a fresh
// session, and a first connect had no lease.
func TestNoEvidenceOnACleanStartOrAFirstConnect(t *testing.T) {
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	res, err := acquire(t, n, "new", nil, 300, false)
	if err != nil || res.Empty.Reason != "" {
		t.Fatalf("first connect: evidence %+v, err %v", res.Empty, err)
	}
	ownedBy(t, n, "new", "n1")
	res, err = acquire(t, n, "new", nil, 300, true)
	if err != nil || res.Empty.Reason != "" {
		t.Fatalf("clean start: evidence %+v, err %v", res.Empty, err)
	}
}

// TestLeaseThatBecameLocalDuringTheAcquireCarriesItsEvidence: the CONNECT saw
// another owner, and by the time its acquire applied another grant had made
// the lease this node's, with no adoption to fetch.
func TestLeaseThatBecameLocalDuringTheAcquireCarriesItsEvidence(t *testing.T) {
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	// The race needs b's table to still show n1 when the CONNECT looks, with a
	// grant to b already queued; it is constructed per client until it holds.
	for i := 0; i < 20; i++ {
		id := fmt.Sprint("r", i)
		if _, err := acquire(t, a, id, nil, 300, true); err != nil {
			t.Fatal(err)
		}
		ownedBy(t, b, id, "n1")
		b.svc.submitAsync(Op{Kind: OpAcquire, Namespace: "default", ClientID: id, Node: "n2", ExpiryInterval: 300, Now: time.Now()})
		res, err := acquire(t, b, id, nil, 300, false)
		if err == nil && res.PrevOwner == "" && !res.Local {
			wantEmpty(t, "raced", res, err, EmptyRaced, "n1")
			return
		}
	}
	t.Fatal("precondition not reached in 20 clients: the CONNECT never saw n1 with a grant to n2 applying first")
}
