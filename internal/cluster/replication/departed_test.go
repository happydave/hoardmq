package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// departedKey is a session n1 owns whose one replica target, under the map
// naming all three nodes, is n3: the barrier over it waits on n3 while the map
// names n3, and on n2 once the map has dropped it.
var departedKey = store.Scope{Namespace: "default", ClientID: clientTargeting(shardmap.Compute(1, []string{"n1", "n2", "n3"}, 2, 4), 4, "n1", "n3", "departed")}

func barrierOnce(n *tnode, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return n.barrier(ctx, departedKey)
}

func streamState(n *tnode, peer string) StreamState {
	for _, st := range n.rep.StreamStates() {
		if st.Peer == peer {
			return st
		}
	}
	return StreamState{Peer: peer, Reason: "no stream"}
}

// TestDepartedPeerStopsFailingTheBarrierOnceTheMapDropsIt: a node that goes
// away and does not come back fails every barrier while the shard map still
// names it, and none once the map has dropped it. Its stream is failed at
// everything it was sent and is sent nothing more, so nothing else ends it.
func TestDepartedPeerStopsFailingTheBarrierOnceTheMapDropsIt(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 4, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 4))
	n1 := h.nodes["n1"]
	for _, peer := range []string{"n2", "n3"} {
		p := n1.rep.outboundFor(peer)
		p.push(frame{Kind: kindResyncMark})
		h.waitFor("an acknowledged frame on the stream to "+peer, 5*time.Second, func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.ackSeq >= p.sent
		})
	}
	h.nodes["n3"].rep.Stop()
	h.nodes["n3"].tr.Stop()
	// The replicator breaks the stream when the transport reports the peer
	// down, just after the peer reads down: until then the stream reads fully
	// acknowledged and a barrier on it passes.
	down := n1.rep.outboundFor("n3")
	h.waitFor("n1's stream to n3 to be broken", 5*time.Second, func() bool {
		down.mu.Lock()
		defer down.mu.Unlock()
		return !n1.rep.up("n3") && down.failed >= down.sent
	})

	// Still named: n3 may be a target of what is being acknowledged.
	if err := barrierOnce(n1, 500*time.Millisecond); !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("barrier while the map still names the down peer: %v, want ErrReplicaUnavailable", err)
	}
	if st := streamState(n1, "n3"); st.Reason != "holed" {
		t.Fatalf("the down peer's stream reads %q while the map names it, want holed", st.Reason)
	}

	for _, name := range []string{"n1", "n2"} {
		if _, err := h.nodes[name].table.Apply(0, shardmap.Encode(shardmap.Compute(2, []string{"n1", "n2"}, 2, 4))); err != nil {
			t.Fatal(err)
		}
	}
	h.waitFor("n1 to have reconciled the map without n3", 5*time.Second, func() bool { return n1.rep.Reconciled() == 2 })
	if err := barrierOnce(n1, 2*time.Second); err != nil {
		t.Fatalf("barrier after the map dropped the departed peer: %v", err)
	}
	if st := streamState(n1, "n3"); st.Reason != "" || st.Blocked != 0 {
		t.Fatalf("a stream no barrier waits on is reported as blocking: %+v", st)
	}
	if holed := n1.rep.HoledStreams(); len(holed) != 0 {
		t.Fatalf("holed streams %v: the departed peer is not one a publish can need", holed)
	}
}

// TestPeerTheMapNamesAgainIsWaitedOnAgain: leaving the map's basis is not a
// verdict about a peer. One the map names again is waited on with its stream
// as it is — here holed and never resynced.
func TestPeerTheMapNamesAgainIsWaitedOnAgain(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 4, 2)
	with := func(version uint64) shardmap.Map { return shardmap.Compute(version, []string{"n1", "n2", "n3"}, 2, 4) }
	without := func(version uint64) shardmap.Map { return shardmap.Compute(version, []string{"n1", "n2"}, 2, 4) }
	apply := func(m shardmap.Map) {
		t.Helper()
		h.applyMap(m)
		h.waitFor("n1 to reconcile the map", 5*time.Second, func() bool { return h.nodes["n1"].rep.Reconciled() == m.Version })
	}
	n1 := h.nodes["n1"]
	p := n1.rep.outboundFor("n3")
	p.push(frame{Kind: kindResyncMark})
	h.waitFor("an acknowledged frame on the stream to n3", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.ackSeq >= p.sent
	})
	hole := func() {
		// A break with no resync asked for: the link is up, so nothing else
		// will ask either, and the hole stays for the barrier to find.
		p.mu.Lock()
		p.hole = true
		p.holeGen++
		p.mu.Unlock()
	}
	wantResync := func(when string) {
		t.Helper()
		hole()
		err := barrierOnce(n1, 500*time.Millisecond)
		if got := barrierReason(err); got != "resync" {
			t.Fatalf("%s: barrier %v (reason %q), want it to fail on n3's hole", when, err, got)
		}
	}

	apply(with(1))
	wantResync("named by the map")
	apply(without(2))
	hole()
	if err := barrierOnce(n1, 2*time.Second); err != nil {
		t.Fatalf("not named by the map: %v", err)
	}
	apply(with(3))
	wantResync("named by the map again")
}
