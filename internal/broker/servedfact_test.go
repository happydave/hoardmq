package broker

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

// metricSum totals one metric on a node, over the series whose labels
// include every pair in match.
func metricSum(t *testing.T, f *fixture, name string, match map[string]string) float64 {
	t.Helper()
	snap, ok := f.srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
	if !ok {
		t.Fatal("the metrics module has no snapshot")
	}
	var n float64
	for _, m := range snap.Snapshot() {
		if m.Name != name {
			continue
		}
		all := true
		for k, v := range match {
			all = all && m.Labels[k] == v
		}
		if all {
			n += m.Value
		}
	}
	return n
}

// TestANodeLossCostsAServedFactPerOwningSurvivor: after a node is lost, each
// survivor that owns leases puts its served fact in the log once, and both
// forget the lost node's routes. The log is asked for no more than that.
func TestANodeLossCostsAServedFactPerOwningSurvivor(t *testing.T) {
	nodes, _ := startReplicatedTrio(t)
	exp := uint32(600)
	x, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect: %s", ack.ReasonCode)
	}
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t/x", QoS: 1}}})
	if _, ok := x.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK")
	}
	x.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = x.c.Close()
	// A client on each survivor, so each owns a lease when n1 is lost.
	for _, n := range []string{"n2", "n3"} {
		if _, ack := nodes[n].connect("tcp", packet.V5, &packet.Connect{ClientID: "c-" + n, CleanStart: true}, false); ack.ReasonCode != packet.Success {
			t.Fatalf("connect on %s: %s", n, ack.ReasonCode)
		}
	}
	for _, n := range []string{"n2", "n3"} {
		if got := metricSum(t, nodes[n], "hoardmq_lease_proposals_total", map[string]string{"op": "served"}); got != 0 {
			t.Fatalf("%s proposed %v served facts before any node was lost", n, got)
		}
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("20 s without %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor("no owner hidden before n1 stops", func() bool {
		return len(nodes["n2"].srv.Routes().Table().HiddenOwners()) == 0 && len(nodes["n3"].srv.Routes().Table().HiddenOwners()) == 0
	})
	nodes["n1"].stop()
	waitFor("n1 hidden on both survivors", func() bool {
		return len(nodes["n2"].srv.Routes().Table().HiddenOwners()) == 1 && len(nodes["n3"].srv.Routes().Table().HiddenOwners()) == 1
	})
	waitFor("n1's routes forgotten on both survivors", func() bool {
		return len(nodes["n2"].srv.Routes().Table().HiddenOwners()) == 0 && len(nodes["n3"].srv.Routes().Table().HiddenOwners()) == 0
	})
	// Each survivor's own fact may still be in flight when the other has
	// forgotten; wait for both, then read the count once nothing proposes.
	waitFor("both survivors' facts in the log", func() bool {
		for _, n := range []string{"n2", "n3"} {
			for _, o := range []string{"n2", "n3"} {
				if i, _ := nodes[n].srv.Leases().ServedFact(o); i == 0 {
					return false
				}
			}
		}
		return true
	})
	time.Sleep(time.Second) // a second of checks after both facts, in which none may propose again
	total := 0.0
	for _, n := range []string{"n2", "n3"} {
		got := metricSum(t, nodes[n], "hoardmq_lease_proposals_total", map[string]string{"op": "served"})
		if got != 1 {
			t.Errorf("%s proposed %v served facts for one lost node, want 1", n, got)
		}
		total += got
	}
	if total > 2 {
		t.Fatalf("%v served proposals for one lost node and two owning survivors", total)
	}
}
