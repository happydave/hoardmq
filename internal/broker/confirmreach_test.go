package broker

import (
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
)

// dropAppendsFrom drops every replication append from one node to the others.
func dropAppendsFrom(t *testing.T, nodes map[string]*fixture, from string) {
	t.Helper()
	for name := range nodes {
		if name == from {
			continue
		}
		nodes[from].srv.Cluster().SetFault(name, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded test fault
		t.Cleanup(func() { nodes[from].srv.Cluster().SetFault(name, nil) })
	}
}

// reconnect brings a durable client back on f, expecting its session.
func reconnect(t *testing.T, f *fixture, id string) *client {
	t.Helper()
	exp := uint32(600)
	c, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: id, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success || !ack.SessionPresent {
		t.Fatalf("reconnect %s: %s, session present %v", id, ack.ReasonCode, ack.SessionPresent)
	}
	return c
}

// TestDrainSettlesASessionTakenFromItsAdopter: an offline session on the
// draining node D goes to the adopter A at D's hand-off, and its client then
// comes back on the third node Q, which takes it from A. A, whose appends
// are all dropped, never confirms. D's drain waits for the session and is
// settled by Q's confirmation.
func TestDrainSettlesASessionTakenFromItsAdopter(t *testing.T) {
	nodes, _ := leavingTrio(t, 2)
	adopter := leaderOf(nodes)
	if adopter == "" {
		t.Fatal("precondition: no leader")
	}
	var others []string
	for _, n := range []string{"n1", "n2", "n3"} {
		if n != adopter {
			others = append(others, n)
		}
	}
	drained, third := others[0], others[1]
	c := durableSubscriber(t, nodes[drained], "h", "t/h")
	c.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = c.c.Close()
	waitUntil(t, "the session to go offline", 5*time.Second, func() bool {
		rec, ok := nodes[drained].srv.Sessions().Record("h")
		return ok && !rec.Online
	})
	dropAppendsFrom(t, nodes, adopter)
	nodes[drained].srv.Drain()
	waitUntil(t, "the adopter to hold the session", 10*time.Second, func() bool {
		l, ok := nodes[adopter].srv.Leases().Table().Lookup("default", "h")
		_, held := nodes[adopter].srv.Sessions().HeldAt("default", "h")
		return ok && l.Owner == adopter && held
	})
	reconnect(t, nodes[third], "h")
	select {
	case <-nodes[drained].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("the drain did not complete")
	}
	st := nodes[drained].srv.DrainStatus()
	if st.SessionsUnconfirmed != 0 || st.SessionsConfirmed < 1 {
		t.Fatalf("drain status %+v: want the session confirmed by %s, which took it from %s", st, third, adopter)
	}
	if got := metricSum(t, nodes[drained], "hoardmq_drain_sessions_unconfirmed_total", nil); got != 0 {
		t.Fatalf("sessions unconfirmed counted %v, want 0", got)
	}
}

// TestDrainLeavesWhenItsMovedCopysSessionChangesOwner: n3 keeps x's copy when
// its drain moves x's target from n3 to n2, and n1's whole write to n2, with
// the confirmation that follows it, never lands. x's client then comes back
// on n2, which takes the session from n1 after n2 reconciled the map. n3's
// leaving phase ends on n2's confirmation, not at its bound.
func TestDrainLeavesWhenItsMovedCopysSessionChangesOwner(t *testing.T) {
	nodes, m := leavingTrio(t, 2)
	id := clientTargeting(m, "n1", "n3")
	durableSubscriber(t, nodes["n1"], id, "t/moved")
	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publishAccepted(t, pub, "t/moved")
	waitUntil(t, "n3 to hold x's copy", 5*time.Second, func() bool {
		return nodes["n3"].srv.Replication().ReplicaState().Find("default", id) != nil
	})
	nodes["n1"].srv.Cluster().SetFault("n2", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAppend}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded test fault
	t.Cleanup(func() { nodes["n1"].srv.Cluster().SetFault("n2", nil) })
	nodes["n3"].srv.Drain()
	waitUntil(t, "n2 and n3 to reconcile the map without n3, n3 keeping x's copy", 10*time.Second, func() bool {
		for _, n := range []string{"n2", "n3"} {
			r := nodes[n].srv.Replication()
			cur := r.Map().Current()
			if slices.Contains(cur.Basis, "n3") || r.Reconciled() < cur.Version {
				return false
			}
		}
		return nodes["n3"].srv.Replication().KeptMoved() >= 1
	})
	reconnect(t, nodes["n2"], id)
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(15 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "copies-expired"}); got != 0 {
		t.Fatalf("placement{copies-expired} on n3 = %v, want 0: n2 took x after n1's rebalance and did not tell n3", got)
	}
	if got := metricSum(t, nodes["n3"], "hoardmq_drain_placement_total", map[string]string{"outcome": "left"}); got != 1 {
		t.Fatalf("placement{left} on n3 = %v, want 1", got)
	}
	if nodes["n3"].srv.Replication().ReplicaState().Find("default", id) != nil {
		t.Fatal("n3 still holds x's copy")
	}
}
