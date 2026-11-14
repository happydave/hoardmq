package transport

import (
	"context"
	"testing"
	"time"
)

// dataInfo is a node's view of one peer.
func dataInfo(n *node, peer string) PeerInfo {
	for _, p := range n.t.Peers() {
		if p.Name == peer {
			return p
		}
	}
	return PeerInfo{}
}

// The data link's own version, its generation and its ordered queue's counts
// are what a sender pacing a long transfer reads: the version decides what it
// may send, a changed generation says the link it was writing to has gone,
// and taken reaching admitted says its frames have left the queue.
func TestDataLinkVersionGenerationAndCounts(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, nil)
	got := received(c.nodes["n2"], RouteUpdate)
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	n1 := c.nodes["n1"]
	before := dataInfo(n1, "n2")
	if before.DataVersion != Version6 || before.DataGeneration == 0 {
		t.Fatalf("data link %+v, want version %d and a generation", before, Version6)
	}

	// Heartbeats pass on the urgent lane and are not counted.
	time.Sleep(5 * 100 * time.Millisecond)
	idle := dataInfo(n1, "n2")
	if idle.DataAdmitted != before.DataAdmitted || idle.DataTaken != before.DataTaken {
		t.Fatalf("heartbeats moved the counts: %d/%d to %d/%d", before.DataAdmitted, before.DataTaken, idle.DataAdmitted, idle.DataTaken)
	}

	const sends = 20
	for i := 0; i < sends; i++ {
		if err := n1.t.Send(context.Background(), "n2", RouteUpdate, []byte("r")); err != nil {
			t.Fatal(err)
		}
	}
	after := dataInfo(n1, "n2")
	if after.DataAdmitted != idle.DataAdmitted+sends {
		t.Fatalf("admitted %d after %d sends from %d", after.DataAdmitted, sends, idle.DataAdmitted)
	}
	waitFor(t, "every frame received", func() bool { return got.n() == sends })
	waitFor(t, "taken to reach admitted", func() bool {
		p := dataInfo(n1, "n2")
		return p.DataTaken == p.DataAdmitted
	})

	gen := dataInfo(n1, "n2").DataGeneration
	n1.t.Reset("n2", RoleData, "test")
	waitFor(t, "a new data link", func() bool {
		p := dataInfo(n1, "n2")
		return p.DataState == Up && p.DataGeneration != gen
	})
}

// A link with an end at version 5 is a version-5 data link at both ends.
func TestDataVersionIsTheLinksNegotiatedVersion(t *testing.T) {
	c := newCluster(t, "secret", []string{"n1", "n2"}, func(name string, o *Options) {
		if name == "n2" {
			o.MaxVersion = Version5
		}
	})
	c.start(t)
	c.nodes["n1"].waitUp(t, "n2")
	c.nodes["n2"].waitUp(t, "n1")
	if v := dataInfo(c.nodes["n1"], "n2").DataVersion; v != Version5 {
		t.Fatalf("n1's data link to n2 at %d", v)
	}
	if v := dataInfo(c.nodes["n2"], "n1").DataVersion; v != Version5 {
		t.Fatalf("n2's data link to n1 at %d", v)
	}
}
