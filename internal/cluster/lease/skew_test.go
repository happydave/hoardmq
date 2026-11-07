package lease

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// The skew tests give each node its own wall clock, offset from the real one
// by an amount the test sets once roles are known; the steady clocks stay
// real. A client's Will delay (5 s) and expiry (8 s) both run past the
// adoption, so the adopter's timers decide when they land.
const (
	skewTTL      = 1500 * time.Millisecond
	skewWill     = 5 * time.Second
	skewExpiry   = 8 * time.Second
	skewEarly    = 200 * time.Millisecond
	skewLateness = 600 * time.Millisecond
)

// skewCluster is a three-node cluster whose nodes' wall clocks are offset by
// offsets[name].
func skewCluster(t *testing.T) (*tcluster, map[string]*atomic.Int64) {
	t.Helper()
	offsets := map[string]*atomic.Int64{"n1": {}, "n2": {}, "n3": {}}
	fixtureClock = func(node string) func() time.Time {
		off := offsets[node]
		return func() time.Time { return time.Now().Add(time.Duration(off.Load())) }
	}
	t.Cleanup(func() { fixtureClock = nil })
	return newCluster(t, []string{"n1", "n2", "n3"}, skewTTL, nil), offsets
}

// disconnectOn holds client x on owner with a 5 s Will and records it offline
// with an 8 s expiry computed on the owner's clock, as the session manager
// does. It returns the real time of the disconnect.
func disconnectOn(t *testing.T, owner, lead *tnode) time.Time {
	t.Helper()
	res, err := acquire(t, owner, "x", willFor("will/x", uint32(skewWill/time.Second)), uint32(skewExpiry/time.Second), true)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	owner.svc.Disconnected("default", "x", res.Epoch, owner.svc.o.Clock().Add(skewExpiry), true)
	waitFor(t, "x recorded offline", 3*time.Second, func() bool {
		l, ok := lead.svc.Table().Lookup("default", "x")
		return ok && !l.Online
	})
	return at
}

// landings waits on adopter for x's Will and x's release, and returns when
// each came after disconnected.
func landings(t *testing.T, adopter *tnode, watch *tnode, disconnected time.Time) (will, release time.Duration) {
	t.Helper()
	deadline := time.Now().Add(skewExpiry + 8*time.Second)
	for time.Now().Before(deadline) && (will == 0 || release == 0) {
		if will == 0 && adopter.sessions.willCount() > 0 {
			will = time.Since(disconnected)
		}
		if _, ok := watch.svc.Table().Lookup("default", "x"); release == 0 && !ok {
			release = time.Since(disconnected)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if will == 0 || release == 0 {
		t.Fatalf("by the deadline: Will after %s, release after %s (zero is never)", will, release)
	}
	return will, release
}

// onTime requires a landing within early..late of its due time.
func onTime(t *testing.T, what string, got, due, early, late time.Duration) {
	t.Helper()
	t.Logf("%s landed %s after the disconnect, due at %s", what, got.Round(time.Millisecond), due)
	if got < due-early || got > due+late {
		t.Errorf("%s landed %s after the disconnect, want %s to %s", what, got.Round(time.Millisecond), due-early, due+late)
	}
}

// TestAdoptionKeepsTimeWithAnOwnerAhead: the owner's clock is 3 s ahead of
// the leader's. The owner dies; the leader adopts. The Will and the expiry
// land on time.
func TestAdoptionKeepsTimeWithAnOwnerAhead(t *testing.T) {
	c, offsets := skewCluster(t)
	lead := c.settled()
	owner := c.follower(lead)
	offsets[owner.name].Store(int64(3 * time.Second))
	at := disconnectOn(t, owner, lead)
	c.stopNode(owner)
	will, release := landings(t, lead, lead, at)
	onTime(t, "the Will", will, skewWill, skewEarly, skewLateness)
	onTime(t, "the expiry", release, skewExpiry, skewEarly, skewLateness)
}

// TestHandoffKeepsTimeWithASkewedReceiver: a drain hands an offline lease to
// a receiver whose clock is off from the leader's, behind and then ahead. The
// Will and the expiry land on time.
func TestHandoffKeepsTimeWithASkewedReceiver(t *testing.T) {
	for _, tc := range []struct {
		name string
		skew time.Duration
	}{{"receiver 3s behind", -3 * time.Second}, {"receiver 3s ahead", 3 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			c, offsets := skewCluster(t)
			lead := c.settled()
			owner := c.follower(lead)
			var receiver *tnode
			for _, n := range c.nodes {
				if n != lead && n != owner {
					receiver = n
				}
			}
			offsets[receiver.name].Store(int64(tc.skew))
			at := disconnectOn(t, owner, lead)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := owner.svc.Handoff(ctx, receiver.name); err != nil {
				t.Fatal(err)
			}
			will, release := landings(t, receiver, lead, at)
			onTime(t, "the Will", will, skewWill, skewEarly, skewLateness)
			onTime(t, "the expiry", release, skewExpiry, skewEarly, skewLateness)
		})
	}
}

// TestAdoptionAfterALeaderChangeIsOffByTheLeadersSkew: the disconnect is
// stamped by a leader on time; leadership then moves to a node 2 s behind,
// which adopts after the owner dies. The two stamps are on two clocks, so the
// landings may be off by up to that skew, and no more.
func TestAdoptionAfterALeaderChangeIsOffByTheLeadersSkew(t *testing.T) {
	const skew = 2 * time.Second
	c, offsets := skewCluster(t)
	lead := c.settled()
	owner := c.follower(lead)
	var next *tnode
	for _, n := range c.nodes {
		if n != lead && n != owner {
			next = n
		}
	}
	offsets[next.name].Store(int64(-skew))
	at := disconnectOn(t, owner, lead)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lead.core.TransferLeadership(ctx, next.name); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the new leader", 5*time.Second, func() bool { return next.core.Status().Role == "leader" })
	c.stopNode(owner)
	will, release := landings(t, next, next, at)
	onTime(t, "the Will", will, skewWill, skew+skewEarly, skew+skewLateness)
	onTime(t, "the expiry", release, skewExpiry, skew+skewEarly, skew+skewLateness)
}
