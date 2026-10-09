package lease

import (
	"context"
	"testing"
	"time"
)

// TestAFirstAcquireGrantedWithNobodyWaitingIsNotServedEmpty: a first CONNECT
// gives up before its acquire is proposed, the acquire creates the lease here
// afterwards, and the client's next CONNECT here finds it with nothing
// fetched. The client never had a session, so nothing counts it as served
// empty.
func TestAFirstAcquireGrantedWithNobodyWaitingIsNotServedEmpty(t *testing.T) {
	for _, names := range [][]string{{"n1"}, {"n1", "n2", "n3"}} {
		c := newCluster(t, names, 2*time.Second, nil)
		n := c.nodes["n1"]
		giveUp(t, n, "y")
		ownedBy(t, n, "y", "n1")
		l, _ := n.svc.table.Lookup("default", "y")
		if !l.Online {
			t.Fatalf("%d nodes: precondition: the abandoned grant left the lease offline", len(names))
		}
		res, err := acquire(t, n, "y", nil, 300, false)
		if err != nil || !res.Local || res.Epoch != l.Epoch || res.State != nil {
			t.Fatalf("%d nodes: retry %+v, err %v; want a local acquire at the created token %d", len(names), res, err, l.Epoch)
		}
		if res.Empty.Reason != "" {
			t.Fatalf("%d nodes: the retry carries evidence %+v, but the client never had a session", len(names), res.Empty)
		}
		if got := n.svc.LivenessStats().FirstGrantsUnfetched; got != 1 {
			t.Fatalf("%d nodes: first grants unfetched %d, want 1", len(names), got)
		}
		if got := n.svc.LivenessStats().FirstAcquireMarks; got != 0 {
			t.Fatalf("%d nodes: %d first-acquire marks left after the retry, want 0", len(names), got)
		}
		// A first acquire whose CONNECT gets its result leaves no mark.
		if _, err := acquire(t, n, "z", nil, 300, false); err != nil {
			t.Fatal(err)
		}
		if got := n.svc.LivenessStats().FirstAcquireMarks; got != 0 {
			t.Fatalf("%d nodes: %d first-acquire marks left after a first acquire that was answered, want 0", len(names), got)
		}
	}
}

// TestAFirstAcquiresLeaseThatMovedKeepsItsEvidence: the lease the abandoned
// first acquire created moves to n2 and back; at the token it has now, a
// session may have been written elsewhere, so the evidence stays.
func TestAFirstAcquiresLeaseThatMovedKeepsItsEvidence(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	giveUp(t, a, "y")
	ownedBy(t, a, "y", "n1")
	ownedBy(t, b, "y", "n1")
	if _, err := acquire(t, b, "y", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, a, "y", "n2")
	if _, err := acquire(t, a, "y", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, a, "y", "n1")
	res, err := acquire(t, a, "y", nil, 300, false)
	wantEmpty(t, "a lease that moved since its creation", res, err, EmptyGrantUnfetched, "n1")
	if got := a.svc.LivenessStats().FirstGrantsUnfetched; got != 0 {
		t.Fatalf("first grants unfetched %d, want 0", got)
	}
}

// TestAFirstAcquireThatTookTheLeaseIsCountedWhenFoundNowhere: n1's first acquire of x
// is held before its proposal while n2 creates x's lease; n1's CONNECT gives
// up, and its acquire then takes the lease from n2, where a session may be.
// The retry fetches from n2, which holds none here, so it is served with the
// adoption's evidence: still counted, not withheld.
func TestAFirstAcquireThatTookTheLeaseIsCountedWhenFoundNowhere(t *testing.T) {
	hold := newAcquireHold(false)
	fixtureOptions = func(o *Options) {
		heldBack(o)
		if o.Node == "n1" {
			o.AcquireGate = hold.gate
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	t.Cleanup(hold.unblock)
	a, b := c.nodes["n1"], c.nodes["n2"]
	hold.armed.Store(true)
	gaveUp := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := a.svc.Acquire(ctx, "default", "x", nil, 300, false)
		gaveUp <- err
	}()
	hold.wait(t, "n1's first acquire of x held before its proposal")
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, a, "x", "n2")
	hold.unblock()
	if err := <-gaveUp; err == nil {
		t.Fatal("precondition: n1's acquire on an ended context was not refused")
	}
	ownedBy(t, a, "x", "n1")
	res, err := acquire(t, a, "x", nil, 300, false)
	wantEmpty(t, "a first acquire that took the lease from n2", res, err, EmptyAdoption, "n2")
	if st := a.svc.LivenessStats(); st.FirstGrantsUnfetched != 0 || st.FirstGrantsTakenAdopted != 1 {
		t.Fatalf("first grants unfetched %d, taken adopted %d; want 0 and 1", st.FirstGrantsUnfetched, st.FirstGrantsTakenAdopted)
	}
}

func TestCreatedHere(t *testing.T) {
	for _, c := range []struct {
		abandoned        bool
		created, current uint64
		want             bool
	}{
		{true, 7, 7, true},
		{false, 7, 7, false},
		{true, 0, 7, false},
		{true, 7, 9, false},
	} {
		if got := createdHere(c.abandoned, c.created, c.current); got != c.want {
			t.Errorf("createdHere(%v, %d, %d) = %v, want %v", c.abandoned, c.created, c.current, got, c.want)
		}
	}
}

func TestFirstAcquireMarks(t *testing.T) {
	f := newFirstAcquires()
	now := time.Unix(1000, 0)
	ttl := time.Minute
	kDone, kHere, kGone, kYoung := key{"d", "done"}, key{"d", "here"}, key{"d", "gone"}, key{"d", "young"}
	for _, k := range []key{kDone, kHere, kGone, kYoung} {
		f.begin(k, now)
	}
	f.end(kDone)
	f.abandon(kHere, now, ttl)
	f.abandon(kGone, now, ttl)
	f.abandon(kYoung, now.Add(ttl), ttl)
	f.created(kHere, 7)
	f.sweep(now.Add(ttl+time.Second), ttl, func(k key) bool { return k == kHere })
	if _, ok := f.take(kDone); ok {
		t.Error("an acquire whose CONNECT got its result left a mark to take")
	}
	if _, ok := f.take(kGone); ok {
		t.Error("an old mark whose lease is not here was kept")
	}
	if m, ok := f.take(kHere); !ok || m.created != 7 {
		t.Errorf("an old mark whose lease is here: %+v %v, want kept with its token 7", m, ok)
	}
	if _, ok := f.take(kYoung); !ok {
		t.Error("a young mark was dropped")
	}
	if f.len() != 0 {
		t.Errorf("%d marks left after every take", f.len())
	}
	// A mark whose acquire is still in flight is not taken: its CONNECT may
	// yet give up and need it.
	kFlight := key{"d", "flight"}
	f.begin(kFlight, now)
	if _, ok := f.take(kFlight); ok || f.len() != 1 {
		t.Errorf("an in-flight mark was taken (len %d)", f.len())
	}
}
