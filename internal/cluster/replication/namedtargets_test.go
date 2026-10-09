package replication

import (
	"context"
	"errors"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

func session(id string) store.Scope { return store.Scope{Namespace: "default", ClientID: id} }

func barrierOver(n *tnode, d time.Duration, keys ...store.Scope) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return n.rep.Barrier(ctx, keys)
}

// TestBarrierWaitsOnlyOnTheNamedKeysTargets: with n3 down and still named by
// the map, a barrier over sessions whose records went only to n2 passes, and
// one over a session whose records went to n3 fails on n3 (WI 1631).
func TestBarrierWaitsOnlyOnTheNamedKeysTargets(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 4, 2)
	m := shardmap.Compute(1, h.order, 2, 4)
	h.applyMap(m)
	n1 := h.nodes["n1"]
	avoid := clientAvoiding(m, 4, "n1", "n3", "avoid")
	via := clientTargeting(m, 4, "n1", "n3", "via")
	h.writeSession("n1", avoid, 1)
	h.writeSession("n1", via, 1)
	if err := barrierOver(n1, 2*time.Second, session(avoid), session(via)); err != nil {
		t.Fatalf("precondition: a barrier with every node up: %v", err)
	}

	h.nodes["n3"].rep.Stop()
	h.nodes["n3"].tr.Stop()
	h.waitFor("n1 to see n3 down", 5*time.Second, func() bool { return !n1.rep.up("n3") })
	// Written again while n3 is down: via's record is pushed to n3 and fails
	// there, avoid's goes to n2 only.
	h.writeSession("n1", avoid, 1)
	h.writeSession("n1", via, 1)
	if v := n1.rep.Map().Current().Version; v != 1 {
		t.Fatalf("precondition: the map moved to version %d; the case needs n3 down and still named", v)
	}

	if err := barrierOver(n1, 2*time.Second, session(avoid)); err != nil {
		t.Errorf("a barrier over a session with no target on the down node: %v, want it to pass", err)
	}
	err := barrierOver(n1, 2*time.Second, session(via))
	var be *barrierError
	if !errors.Is(err, ErrReplicaUnavailable) || !errors.As(err, &be) || be.peer != "n3" {
		t.Errorf("a barrier over a session whose target is the down node: %v, want it to fail on n3", err)
	}
	if err := barrierOver(n1, 2*time.Second, session(avoid), session(via)); !errors.Is(err, ErrReplicaUnavailable) {
		t.Errorf("a barrier over both: %v, want it to fail", err)
	}
	if v := n1.rep.Map().Current().Version; v != 1 {
		t.Fatalf("the map moved to version %d during the assertions; they do not describe a named down node", v)
	}
}

// TestBarrierWaitsWhereTheRecordsWent: for each kind of key the peers the
// barrier waits on are the peers that hold the key's records once everything
// n1 sent has been applied — a session's targets, a retained topic's followers
// on its shard's leader, and the leader a non-leader forwards a retained write
// to.
func TestBarrierWaitsWhereTheRecordsWent(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 3, 2)
	m := shardmap.Compute(1, h.order, 2, 3) // shard i: leader n(i+1), follower n(i+2 mod 3)
	h.applyMap(m)
	n1 := h.nodes["n1"]
	peers := []string{"n2", "n3"}
	// settle pushes a marker to each peer and waits for its acknowledgement:
	// the peer's worker applies in order, so every frame n1 sent before it has
	// been applied or answered.
	settle := func() {
		t.Helper()
		for _, peer := range peers {
			p := n1.rep.outboundFor(peer)
			p.push(frame{Kind: kindResyncMark})
			p.mu.Lock()
			mark := p.sent
			p.mu.Unlock()
			h.waitFor("the marker's acknowledgement from "+peer, 5*time.Second, func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.ackSeq >= mark
			})
		}
	}
	waitsOn := func(k store.Scope) []string {
		got, ok := barrierTargets(n1.rep.Map().Current(), n1.rep.Map().ShardOf, "n1", []store.Scope{k})
		if !ok {
			t.Fatalf("%+v names nothing", k)
		}
		got = slices.Clone(got)
		sort.Strings(got)
		return got
	}
	holders := func(has func(peer string) bool) []string {
		var out []string
		for _, peer := range peers {
			if has(peer) {
				out = append(out, peer)
			}
		}
		return out
	}

	for shard := 0; shard < 3; shard++ {
		id := clientIn(3, shard, "s")
		h.writeSession("n1", id, 1)
		settle()
		held := holders(func(peer string) bool { return h.nodes[peer].rep.ReplicaState().Find("default", id) != nil })
		if got := waitsOn(session(id)); !slices.Equal(got, held) || len(held) == 0 {
			t.Errorf("session %s on shard %d: the barrier waits on %v, its copies are on %v", id, shard, got, held)
		}
	}

	led := topicIn(3, 0) // led by n1, followed by n2
	n1.sessions.InjectRetained(&message.Message{Namespace: "default", Topic: led, Payload: []byte("r"), Retain: true})
	settle()
	held := holders(func(peer string) bool {
		for _, msg := range h.nodes[peer].rep.ReplicaState().Retained {
			if msg.Topic == led {
				return true
			}
		}
		return false
	})
	if got := waitsOn(store.Scope{Namespace: "default", Topic: led}); !slices.Equal(got, held) || len(held) == 0 {
		t.Errorf("retained on a shard n1 leads: the barrier waits on %v, its copies are on %v", got, held)
	}

	forwarded := topicIn(3, 1) // led by n2
	if n1.rep.StoreRetained(&message.Message{Namespace: "default", Topic: forwarded, Payload: []byte("f"), Retain: true}) {
		t.Fatal("precondition: a non-leader stored the retained write locally")
	}
	settle()
	held = holders(func(peer string) bool { _, ok := h.nodes[peer].sessions.has("default", forwarded); return ok })
	if got := waitsOn(store.Scope{Namespace: "default", Topic: forwarded}); !slices.Equal(got, held) || len(held) == 0 {
		t.Errorf("retained forwarded to its leader: the barrier waits on %v, the write was stored by %v", got, held)
	}
}

// TestRefusalOutlivesAMapChange: a key refused by a peer fails the barriers
// naming it while the map names that peer, even once the peer is no longer
// the key's target. Only a whole rewrite on that stream or a resync clears a
// refusal (R5); a map change does not.
func TestRefusalOutlivesAMapChange(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 3, 2)
	m := shardmap.Compute(1, h.order, 2, 3)
	h.applyMap(m)
	n1 := h.nodes["n1"]
	id := clientTargeting(m, 3, "n1", "n2", "k")
	other := clientTargeting(m, 3, "n1", "n3", "o")
	h.writeSession("n1", other, 1)
	// n2's table has the lease at a later epoch, so it refuses n1's records.
	h.nodes["n1"].leases.set("default", id, "n1", 1)
	h.nodes["n2"].leases.set("default", id, "n2", 2)
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if got := barrierReason(barrierOver(n1, 2*time.Second, session(id))); got != "not-held" {
		t.Fatalf("precondition: a barrier over the refused key failed %q, want not-held", got)
	}

	// Same members, but the refused key's shard now has n1 and n3 as holders:
	// n2 is named and is no longer the key's target.
	shard := shardmap.ShardOf("default", id, 3)
	moved := shardmap.Compute(2, h.order, 2, 3)
	moved.Shards[shard] = shardmap.Shard{Leader: "n1", Followers: []string{"n3"}}
	h.applyMap(moved)
	h.waitFor("n1 to reconcile the moved map", 5*time.Second, func() bool { return n1.rep.Reconciled() == 2 })
	if targets := moved.Targets(shard, "n1"); slices.Contains(targets, "n2") {
		t.Fatalf("precondition: n2 is still a target, %v", targets)
	}

	err := barrierOver(n1, 2*time.Second, session(id))
	var be *barrierError
	if got := barrierReason(err); got != "not-held" || !errors.As(err, &be) || be.peer != "n2" {
		t.Errorf("a barrier over a key n2 refused, once n2 is not its target: %v (reason %q), want not-held on n2", err, got)
	}
	if err := barrierOver(n1, 2*time.Second, session(other)); err != nil {
		t.Errorf("a barrier over a key nobody refused: %v", err)
	}
}
