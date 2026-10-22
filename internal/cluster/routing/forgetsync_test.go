package routing

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// TestForgetKeepsASyncThatArrivedAfterItsRead: n3 is lost and its sessions
// are served on n1. While this node's judging pass, which read n3 as hidden,
// counts n3's leases, n3 comes back and its first full sync arrives. The
// forget that pass decides must not erase that sync: once n3 owns leases
// again, a QoS 1 publish is not refused as an owner's routes unknown, and
// n3's routes from the sync are held.
func TestForgetKeepsASyncThatArrivedAfterItsRead(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 4, "n2": 1, "n3": 3}, applied: 5, hooked: 5}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", nil, 100)
	tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/old", Version: 1, Count: 1}}, 200)
	tr.setState("n3", transport.Down)
	// n3's sessions move to n1 at index 10; n1's fact and routes are in.
	f.set(func(f *servedFixture) {
		f.owned["n3"], f.owned["n1"], f.applied, f.hooked = 0, 7, 10, 10
		f.facts["n1"] = servedFact{10, 101}
	})
	tr.deliverDelta(t, "n1", []Entry{{Namespace: "default", Filter: "t/old", Version: 2, Count: 1}}, 101)

	var once sync.Once
	f.set(func(f *servedFixture) {
		f.onOwned = func(node string) {
			if node != "n3" {
				return
			}
			once.Do(func() {
				// n3's new incarnation: up, and its first full sync names t/new.
				tr.setState("n3", transport.Up)
				tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/new", Version: 1, Count: 1}}, 1)
			})
		}
	})
	p.checkServed()
	f.set(func(f *servedFixture) { f.onOwned = nil })
	if !p.syncedFrom("n3") {
		t.Errorf("n3's sync, which arrived after the pass read n3 as hidden, is no longer recorded")
	}

	// n3 takes leases again, as a drain's redirect gives it clients.
	f.set(func(f *servedFixture) { f.owned["n3"] = 2 })
	p.refreshOwnerKnowledge()
	unknownBefore := f.metrics.get("hoardmq_forward_failures_total{routes-unknown}")
	if _, err := p.Forward(qos1("t/other")); errors.Is(err, ErrRoutesUnavailable) {
		t.Errorf("publish with n3 owning leases after its sync: %v, want no refusal", err)
	}
	if got := f.metrics.get("hoardmq_forward_failures_total{routes-unknown}") - unknownBefore; got != 0 {
		t.Errorf("routes-unknown refusals of that publish: %v, want 0", got)
	}
	if sent, err := p.Forward(qos1("t/new")); sent != 1 || err != nil {
		t.Errorf("publish to n3's synced filter: forwarded %d, err %v; want 1 forward to n3", sent, err)
	}
	if got := f.metrics.get("hoardmq_route_owners_forget_skipped_total{synced}"); got != 1 {
		t.Errorf("forget_skipped_total{synced} = %v, want 1", got)
	}
	if got := f.metrics.get("hoardmq_route_owners_forgotten_total{sessions-served}"); got != 0 {
		t.Errorf("forgotten_total = %v, want 0: the forget did not stand", got)
	}
}

func TestForgetStands(t *testing.T) {
	for _, c := range []struct {
		read, now uint64
		want      bool
	}{
		{0, 0, true},
		{3, 3, true},
		{0, 1, false},
		{3, 5, false},
	} {
		if got := forgetStands(c.read, c.now); got != c.want {
			t.Errorf("forgetStands(%d, %d) = %v, want %v", c.read, c.now, got, c.want)
		}
	}
}

// TestASyncDuringAForgetLandsAfterIt: a full sync from the owner arrives while
// a forget that stands is between its decision and its delete. The sync waits
// for the forget, so its routes and its record are both held afterwards, never
// the record without the routes.
func TestASyncDuringAForgetLandsAfterIt(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 4, "n2": 1, "n3": 3}, applied: 5, hooked: 5}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", nil, 100)
	tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/old", Version: 1, Count: 1}}, 200)
	tr.setState("n3", transport.Down)
	f.set(func(f *servedFixture) {
		f.owned["n3"], f.owned["n1"], f.applied, f.hooked = 0, 7, 10, 10
		f.facts["n1"] = servedFact{10, 101}
	})
	tr.deliverDelta(t, "n1", []Entry{{Namespace: "default", Filter: "t/old", Version: 2, Count: 1}}, 101)

	synced := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	p.beforeForget = func(owner string) {
		if owner != "n3" {
			return
		}
		go func() {
			tr.setState("n3", transport.Up)
			tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/new", Version: 1, Count: 1}}, 1)
			close(synced)
		}()
		select {
		case <-synced:
			t.Error("n3's full sync completed while a forget was between its decision and its delete")
		case <-time.After(100 * time.Millisecond):
		}
		<-release
	}
	go func() {
		time.Sleep(150 * time.Millisecond)
		unblock()
	}()
	p.checkServed()
	p.beforeForget = nil
	select {
	case <-synced:
	case <-time.After(5 * time.Second):
		t.Fatal("n3's full sync never completed after the forget")
	}
	if got := f.metrics.get("hoardmq_route_owners_forgotten_total{sessions-served}"); got != 1 {
		t.Fatalf("precondition: forgotten_total = %v, want the forget to have stood", got)
	}
	if !p.syncedFrom("n3") {
		t.Error("n3's sync, which landed after the forget, is not recorded")
	}
	f.set(func(f *servedFixture) { f.owned["n3"] = 2 })
	p.refreshOwnerKnowledge()
	if sent, err := p.Forward(qos1("t/new")); sent != 1 || err != nil {
		t.Errorf("publish to n3's synced filter: forwarded %d, err %v; want 1 forward to n3", sent, err)
	}
}

// TestASyncBeforeTheHiddenReadIsKept: n3 comes back and syncs after the pass
// has counted the syncs but before it reads the hidden owners. Counted first,
// the sync is after the count, and the pass that reads n3 as hidden cannot
// erase it.
func TestASyncBeforeTheHiddenReadIsKept(t *testing.T) {
	f := &servedFixture{owned: map[string]int{"n1": 4, "n2": 1, "n3": 3}, applied: 5, hooked: 5}
	p, tr := newServedPlane(t, f)
	tr.setState("n1", transport.Up)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n1", nil, 100)
	tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/old", Version: 1, Count: 1}}, 200)
	tr.setState("n3", transport.Down)
	f.set(func(f *servedFixture) {
		f.owned["n3"], f.owned["n1"], f.applied, f.hooked = 0, 7, 10, 10
		f.facts["n1"] = servedFact{10, 101}
	})
	tr.deliverDelta(t, "n1", []Entry{{Namespace: "default", Filter: "t/old", Version: 2, Count: 1}}, 101)
	p.afterSyncCount = func() {
		p.afterSyncCount = nil
		tr.setState("n3", transport.Up)
		tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/new", Version: 1, Count: 1}}, 1)
	}
	p.checkServed()
	if !p.syncedFrom("n3") {
		t.Error("n3's sync, which arrived after the pass counted the syncs, is no longer recorded")
	}
	if sent, err := p.Forward(qos1("t/new")); sent != 1 || err != nil {
		t.Errorf("publish to n3's synced filter: forwarded %d, err %v; want 1 forward to n3", sent, err)
	}
}
