package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

const unappliedMetric = "hoardmq_replication_unapplied_total"

func feedSession(n *tnode, id string, entries ...store.Entry) {
	n.sessions.mu.Lock()
	n.sessions.sessions = []*store.SessionState{{
		SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1, Version: 5, ExpiryInterval: 300},
		Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}},
		Entries:       entries,
	}}
	n.sessions.mu.Unlock()
}

func repairs(sinks map[string]*counters, node, outcome string) float64 {
	return sinks[node].get("hoardmq_replication_repairs_total", map[string]string{"outcome": outcome})
}

// TestUnappliedRecordIsRefusedAndTheKeyRepaired: a follower whose copy of a
// session lost an entry, and then the session, refuses the owner's records
// for them — counted by reason and type — and the owner repairs the key by
// sending the session whole, so the barrier over the refused record passes
// on a copy that holds it again.
func TestUnappliedRecordIsRefusedAndTheKeyRepaired(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, quickAck)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0) // leader n1, follower n2
	h.writeSession("n1", id, 1)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	entry := store.Entry{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("queued")}, QoS: 1}
	// A repair sends what the owner's session manager holds; the harness's
	// manager is a fake fed by hand.
	feedSession(n1, id, entry)
	key := store.Key{Namespace: "default", ClientID: id, Epoch: 1}
	count := func(reason, typ string) float64 {
		return sinks["n2"].get(unappliedMetric, map[string]string{"reason": reason, "type": typ})
	}
	settle := func() {
		t.Helper()
		if err := n1.own.Sync(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := n1.barrier(ctx); err != nil {
			t.Fatalf("the barrier over the refused record did not pass on the repaired copy: %v", err)
		}
	}
	held := func() int {
		ss := n2.rep.ReplicaState().Find("default", id)
		if ss == nil {
			return -1
		}
		return len(ss.Entries)
	}

	// The follower's copy loses its entry; the owner then marks it sent.
	f := n2.rep.follower
	f.mu.Lock()
	f.state.Find("default", id).Entries = nil
	f.mu.Unlock()
	if err := n1.own.Sent(key, 1, 4); err != nil {
		t.Fatal(err)
	}
	settle()
	if got := count("no-entry", "sent"); got != 1 {
		t.Fatalf("unapplied{no-entry,sent} = %v, want 1", got)
	}
	if got := repairs(sinks, "n1", "snapshot"); got != 1 {
		t.Fatalf("repairs{snapshot} = %v, want 1: the owner did not send the session whole", got)
	}
	if got := held(); got != 1 {
		t.Fatalf("after the repair the follower holds %d entries, want 1", got)
	}

	// The follower loses the session; the owner enqueues for it. Past the
	// repair interval, so this refusal is repaired too.
	time.Sleep(400 * time.Millisecond)
	if _, err := f.apply(store.DeleteRecord("default", id)); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Enqueue(key, store.Entry{Seq: 2, Message: &message.Message{Namespace: "default", Topic: "a/b"}, QoS: 1}); err != nil {
		t.Fatal(err)
	}
	settle()
	if got := count("no-session", "enqueue"); got != 1 {
		t.Fatalf("unapplied{no-session,enqueue} = %v, want 1", got)
	}
	if got := repairs(sinks, "n1", "snapshot"); got != 2 {
		t.Fatalf("repairs{snapshot} = %v, want 2", got)
	}
	if got := held(); got != 1 {
		t.Fatalf("after the second repair the follower holds %d entries, want 1 (the fed state)", got)
	}
}

// TestUnappliedInsideAGapWindowIsCountedNotRefused: one frame lost with the
// link up opens a gap; a record that changes nothing before the resync mark
// arrives is counted as such and acknowledged — the stream's barriers already
// fail and the resync replaces the copy — not refused and repaired on top.
func TestUnappliedInsideAGapWindowIsCountedNotRefused(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, quickAck)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	feedSession(n1, id)
	key := store.Key{Namespace: "default", ClientID: id, Epoch: 1}
	// The sent record is lost on the wire; the completion that follows
	// reveals the gap and finds the entry still queued.
	dropAll(h, "n1", "n2", 7)
	if err := n1.own.Sent(key, 1, 4); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	n1.tr.SetFault("n2", nil)
	if err := n1.own.Complete(key, 4); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	count := func(reason, typ string) float64 {
		return sinks["n2"].get(unappliedMetric, map[string]string{"reason": reason, "type": typ})
	}
	h.waitFor("the completion to be counted inside the gap window", 2*time.Second, func() bool { return count("holed", "complete") == 1 })
	if got := count("no-entry", "complete"); got != 0 {
		t.Fatalf("unapplied{no-entry,complete} = %v, want 0: refused inside the gap window", got)
	}
	h.waitFor("the resync to replace the copy", 3*time.Second, func() bool {
		ss := n2.rep.ReplicaState().Find("default", id)
		return ss != nil && len(ss.Entries) == 0
	})
	if got := repairs(sinks, "n1", "snapshot") + repairs(sinks, "n1", "delete") + repairs(sinks, "n1", "repeat"); got != 0 {
		t.Fatalf("repairs counted %v inside a gap window, want 0", got)
	}
}

// TestRefusedKeysAreReadableUntilAResync: a refused key is reported with the
// class of its refusal, and only a whole-stream resync of that peer — which
// is counted — takes it away.
func TestRefusedKeysAreReadableUntilAResync(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0) // leader n1, follower n2
	h.nodes["n1"].leases.set("default", id, "n1", 1)
	h.nodes["n2"].leases.set("default", id, "n2", 2)
	n1 := h.nodes["n1"]
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	scope := store.Scope{Namespace: "default", ClientID: id, Epoch: 1}
	if err := n1.rep.Barrier(context.Background(), []store.Scope{scope}); err == nil {
		t.Fatal("the stale-epoch record was not refused")
	}
	if got := n1.rep.RefusedKeys()["n2"][scope]; got != "stale-epoch" {
		t.Fatalf("refused keys for n2 = %v, want %v as stale-epoch", n1.rep.RefusedKeys()["n2"], scope)
	}
	resyncs := func() float64 {
		return sinks["n1"].get("hoardmq_replication_resyncs_total", map[string]string{"peer": "n2"})
	}
	before := resyncs()
	// A resync runs only for a holed stream: break it, as a dropped send
	// would, and repair it.
	n1.rep.outboundFor("n2").broken()
	n1.rep.resync("n2")
	h.waitFor("the resync to clear the refused key", 5*time.Second, func() bool {
		return len(n1.rep.RefusedKeys()["n2"]) == 0
	})
	if got := resyncs() - before; got != 1 {
		t.Fatalf("resyncs of n2 counted %v, want 1", got)
	}
}

// TestRefusalIsClearedByTheDeleteThatBeginsAWholeWrite: a refusal holds the
// key, at any epoch the barrier names, until the owner sends the session
// whole — which begins with a delete.
func TestRefusalIsClearedByTheDeleteThatBeginsAWholeWrite(t *testing.T) {
	p := &outbound{name: "peer"}
	p.cond = sync.NewCond(&p.mu)
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c", Epoch: 2})
	p.failedAt(1, "epoch 2 is behind lease epoch 3", store.Scope{})
	up := func(string) bool { return true }
	wait := func(epoch uint64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		return p.wait(ctx, up, []store.Scope{{Namespace: "default", ClientID: "c", Epoch: epoch}})
	}
	for _, epoch := range []uint64{2, 0, 4} {
		if err := wait(epoch); err == nil || barrierReason(err) != "not-held" {
			t.Fatalf("a barrier at epoch %d before the whole write: %v, want not-held", epoch, err)
		}
	}
	// A second old frame is in flight when the whole write begins.
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c", Epoch: 2})
	del := store.DeleteRecord("default", "c")
	p.push(frame{Kind: kindAppend, Type: del.Type, Namespace: "default", ClientID: "c", Epoch: 4, Payload: del.Payload})
	// Its rejection arrives after the delete was pushed: it is a refusal of
	// the copy being replaced, and holds nothing.
	p.failedAt(2, "epoch 2 is behind lease epoch 3", store.Scope{})
	p.acked(3)
	if err := wait(4); err != nil && barrierReason(err) == "not-held" {
		t.Fatalf("a barrier after the delete that begins the whole write was held by an old refusal: %v", err)
	}
	// A refusal of the whole write itself is recorded again.
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c", Epoch: 4})
	p.failedAt(4, "epoch 4 is behind lease epoch 5", store.Scope{})
	if err := wait(4); err == nil || barrierReason(err) != "not-held" {
		t.Fatalf("a refusal of the whole write was not recorded: %v", err)
	}
}

// TestSessionComingBackAtAHigherEpochIsNotHeldByTheOldRefusal is the seed-2
// chain in miniature: the owner's records at epoch 2 are refused by a peer
// whose table is at 3 (the session had left); the session comes back to the
// owner at epoch 4 and is written whole. The barrier for it at epoch 4 passes
// and the peer's copy is the epoch-4 state, nothing older.
func TestSessionComingBackAtAHigherEpochIsNotHeldByTheOldRefusal(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	id := clientIn(2, 0) // leader n1, follower n2
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	entry := func(seq uint64) store.Entry {
		return store.Entry{Seq: seq, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("x")}, QoS: 1}
	}
	// write is what an import journals: the delete, then the session whole.
	write := func(epoch uint64, seqs ...uint64) {
		t.Helper()
		if err := n1.own.SessionDelete(store.Key{Namespace: "default", ClientID: id, Epoch: epoch}); err != nil {
			t.Fatal(err)
		}
		if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch}); err != nil {
			t.Fatal(err)
		}
		for _, seq := range seqs {
			if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: epoch}, entry(seq)); err != nil {
				t.Fatal(err)
			}
		}
		if err := n1.own.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	barrier := func(epoch uint64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return n1.rep.Barrier(ctx, []store.Scope{{Namespace: "default", ClientID: id, Epoch: epoch}})
	}
	// Epoch 2, held by both.
	n1.leases.set("default", id, "n1", 2)
	n2.leases.set("default", id, "n1", 2)
	write(2, 1)
	if err := barrier(2); err != nil {
		t.Fatal(err)
	}
	// The session leaves: n2's table moves to epoch 3 elsewhere; n1's late
	// record at epoch 2 is refused.
	n2.leases.set("default", id, "n2", 3)
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 2}, entry(2)); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := barrier(2); err == nil || barrierReason(err) != "not-held" {
		t.Fatalf("the epoch-2 record after the move: %v, want not-held", err)
	}
	// It comes back to n1 at epoch 4, written whole.
	n1.leases.set("default", id, "n1", 4)
	n2.leases.set("default", id, "n1", 4)
	write(4, 1, 2, 3)
	if err := barrier(4); err != nil {
		t.Fatalf("the barrier for the session back at epoch 4 failed: %v", err)
	}
	ss := n2.rep.ReplicaState().Find("default", id)
	var seqs []uint64
	for _, e := range ss.Entries {
		seqs = append(seqs, e.Seq)
	}
	if ss.Epoch != 4 || len(seqs) != 3 || seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("the peer's copy is epoch %d with entries %v, want epoch 4 with [1 2 3]", ss.Epoch, seqs)
	}
}

// TestReconnectEpochBumpKeepsTheReplica: a client reconnecting to its own
// owner bumps the session epoch and the owner journals the meta at the new
// epoch with no delete. The replica's entries survive it (WI 1563: a rule
// that replaced the copy on a higher epoch wiped them).
func TestReconnectEpochBumpKeepsTheReplica(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	id := clientIn(2, 0)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	h.setLease("default", id, "n1", 1)
	entry := func(seq uint64) store.Entry {
		return store.Entry{Seq: seq, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("x")}, QoS: 1}
	}
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 1}, entry(1)); err != nil {
		t.Fatal(err)
	}
	// The reconnect: the meta again at epoch 2, then a new entry.
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 2}, entry(2)); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	ss := n2.rep.ReplicaState().Find("default", id)
	var seqs []uint64
	for _, e := range ss.Entries {
		seqs = append(seqs, e.Seq)
	}
	if ss.Epoch != 2 || len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("after the owner's epoch bump the replica holds %v at epoch %d, want [1 2] at 2", seqs, ss.Epoch)
	}
}
