package transport

import (
	"testing"
	"time"
)

// TestHeldDataConnectionLeavesThePeerOnItsControlConnection: after a cut, a
// pair whose data connection is held comes back on its control connection
// only, not Up, and is Up once the hold is lifted. n1 dials (the lower name);
// the hold is set at both ends, and at the dialing end alone.
func TestHeldDataConnectionLeavesThePeerOnItsControlConnection(t *testing.T) {
	for _, c := range []struct {
		name    string
		holders []string
	}{{"both ends", []string{"n1", "n2"}}, {"the dialer", []string{"n1"}}} {
		t.Run(c.name, func(t *testing.T) {
			cl := newCluster(t, "secret", []string{"n1", "n2"}, nil)
			cl.start(t)
			cl.nodes["n1"].waitUp(t, "n2")
			n1, n2 := cl.nodes["n1"].t, cl.nodes["n2"].t
			other := map[string]string{"n1": "n2", "n2": "n1"}
			peer := func() PeerInfo { return n1.Peers()[0] }
			n1.Isolate("n2", true)
			n2.Isolate("n1", true)
			cl.nodes["n1"].waitDown(t, "n2")
			for _, h := range c.holders {
				cl.nodes[h].t.HoldData(other[h], true)
			}
			n1.Isolate("n2", false)
			n2.Isolate("n1", false)
			waitFor(t, "the control connection back", func() bool { return peer().ControlUp })
			time.Sleep(500 * time.Millisecond) // five of the data dial's 100 ms checks
			if p := peer(); !p.ControlUp || p.State == Up {
				t.Fatalf("held by %v: n1 reads n2 with control up %v and state %s, want control up and not Up", c.holders, p.ControlUp, p.State)
			}
			for _, h := range c.holders {
				cl.nodes[h].t.HoldData(other[h], false)
			}
			cl.nodes["n1"].waitUp(t, "n2")
		})
	}
}
