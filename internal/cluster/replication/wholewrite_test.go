package replication

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// holdsEntry reports whether a node's replica copy of id holds the entry at seq.
func holdsEntry(h *harness, node, id string, seq uint64) bool {
	ss := h.nodes[node].rep.ReplicaState().Find("default", id)
	if ss == nil {
		return false
	}
	for _, e := range ss.Entries {
		if e.Seq == seq {
			return true
		}
	}
	return false
}

// writeEntry writes the last of entries to id on owner as the session manager
// writes a delivery: under the session's lock, the entries held in the
// session's state before the record is journaled at epoch 1.
func writeEntry(h *harness, owner, id string, entries ...store.Entry) error {
	n := h.nodes[owner]
	n.sessions.lock.Lock()
	defer n.sessions.lock.Unlock()
	feedSession(n, id, entries...)
	if err := n.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 1}, entries[len(entries)-1]); err != nil {
		return err
	}
	return n.own.Sync()
}

// holdWholeWrite holds n1's first whole write of a session once its state is
// read, starts the write of X's entries (the last one new), and gives it 300
// ms before the whole write goes on. It reports whether the write completed
// while the whole write was held, after waiting for both.
func holdWholeWrite(t *testing.T, h *harness, id string, start func(), done chan struct{}, entries ...store.Entry) (wroteDuringHold bool) {
	t.Helper()
	reached, release := make(chan struct{}), make(chan struct{})
	first := sync.OnceFunc(func() { close(reached) })
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	h.nodes["n1"].rep.o.WholeWriteGate = func() { first(); <-release }
	start()
	waitClosed(t, "a whole write of X to be held", reached)
	written := make(chan error, 1)
	go func() { written <- writeEntry(h, "n1", id, entries...) }()
	var err error
	select {
	case err = <-written:
		wroteDuringHold = true
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	waitClosed(t, "the whole writes to be queued", done)
	if !wroteDuringHold {
		select {
		case err = <-written:
		case <-time.After(5 * time.Second):
			t.Fatal("the second entry's write never completed")
		}
	}
	if err != nil {
		t.Fatalf("writing the second entry: %v", err)
	}
	return wroteDuringHold
}

// TestAResyncKeepsARecordWrittenAfterItsRead: n1's resync of n3 has read X's
// state, and a record of X is written meanwhile. The record must reach n3
// behind the whole write, not ahead of it, where the older copy would replace
// it after the barrier acknowledged it.
func TestAResyncKeepsARecordWrittenAfterItsRead(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 4, 2, quickAck)
	m := shardmap.Compute(1, h.order, 2, 4)
	h.applyMap(m)
	waitSettled(h, 1)
	shard := -1
	for s := range 4 {
		if slices.Contains(m.Targets(s, "n1"), "n3") {
			shard = s
			break
		}
	}
	if shard < 0 {
		t.Fatal("precondition: no shard replicates n1's sessions to n3")
	}
	id := clientIn(4, shard, "w")
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	ctx := context.Background()
	x := []store.Scope{{Namespace: "default", ClientID: id}}
	if err := h.nodes["n1"].rep.Barrier(ctx, x); err != nil {
		t.Fatalf("first barrier: %v", err)
	}
	if !holdsEntry(h, "n3", id, 1) {
		t.Fatal("precondition: n3 does not hold X's first entry")
	}
	done := make(chan struct{})
	during := holdWholeWrite(t, h, id, func() {
		go func() {
			defer close(done)
			h.nodes["n1"].rep.resyncPeer("n3")
		}()
	}, done, queued(1, "queued"), queued(2, "second"))
	if err := h.nodes["n1"].rep.Barrier(ctx, x); err != nil {
		t.Fatalf("barrier after the resync: %v", err)
	}
	if !holdsEntry(h, "n3", id, 2) {
		t.Fatalf("n3's copy lost the entry the barrier acknowledged (written while the whole write was held: %v): the resync's whole write, read before it, replaced it", during)
	}
	if during {
		t.Fatal("the second entry was written while the whole write was held: the whole write does not hold the session's lock")
	}
}

// TestARebalanceKeepsARecordWrittenAfterItsRead: n1 applies a map that gives
// X a new target, n3, and its rebalance has read X's state for n3; a record of
// X is written meanwhile. It must reach n3 behind the whole write.
func TestARebalanceKeepsARecordWrittenAfterItsRead(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	v1, v2 := unfedMaps(h.order)
	h.applyMap(v1)
	waitSettled(h, 1)
	id := clientIn(2, 0)
	alive(h, "n1")
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	ctx := context.Background()
	x := []store.Scope{{Namespace: "default", ClientID: id}}
	if err := h.nodes["n1"].rep.Barrier(ctx, x); err != nil {
		t.Fatalf("first barrier: %v", err)
	}
	reconciled := make(chan struct{})
	during := holdWholeWrite(t, h, id, func() {
		if _, err := h.nodes["n1"].table.Apply(0, shardmap.Encode(v2)); err != nil {
			t.Fatal(err)
		}
		go func() {
			defer close(reconciled)
			for deadline := time.Now().Add(10 * time.Second); h.nodes["n1"].rep.Reconciled() != 2 && time.Now().Before(deadline); {
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}, reconciled, queued(1, "queued"), queued(2, "second"))
	if err := h.nodes["n1"].rep.Barrier(ctx, x); err != nil {
		t.Fatalf("barrier after the rebalance: %v", err)
	}
	if !holdsEntry(h, "n3", id, 2) {
		t.Fatalf("n3's copy lost the entry the barrier acknowledged (written while the whole write was held: %v): the rebalance's whole write, read before it, replaced it", during)
	}
	if during {
		t.Fatal("the second entry was written while the whole write was held: the whole write does not hold the session's lock")
	}
}

// TestARepairKeepsARecordWrittenAfterItsRead: n2 has lost X's copy, so it
// refuses n1's next record of X and n1 repairs the key by writing X whole; a
// record of X written once the repair has read X's state must reach n2 behind
// the whole write.
func TestARepairKeepsARecordWrittenAfterItsRead(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, quickAck)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 0)
	h.writeSession("n1", id, 1)
	feedSession(h.nodes["n1"], id, queued(1, "queued"))
	ctx := context.Background()
	x := []store.Scope{{Namespace: "default", ClientID: id}}
	if err := h.nodes["n1"].rep.Barrier(ctx, x); err != nil {
		t.Fatalf("first barrier: %v", err)
	}
	if _, err := h.nodes["n2"].rep.follower.apply(store.DeleteRecord("default", id)); err != nil {
		t.Fatal(err)
	}
	repaired := make(chan struct{})
	during := holdWholeWrite(t, h, id, func() {
		if err := writeEntry(h, "n1", id, queued(1, "queued"), queued(2, "second")); err != nil {
			t.Fatal(err)
		}
		go func() {
			defer close(repaired)
			for deadline := time.Now().Add(10 * time.Second); repairs(sinks, "n1", "snapshot") < 1 && time.Now().Before(deadline); {
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}, repaired, queued(1, "queued"), queued(2, "second"), queued(3, "third"))
	if err := h.nodes["n1"].rep.Barrier(ctx, x); err != nil {
		t.Fatalf("barrier after the repair: %v", err)
	}
	if !holdsEntry(h, "n2", id, 3) {
		t.Fatalf("n2's copy lost the entry the barrier acknowledged (written while the whole write was held: %v): the repair's whole write, read before it, replaced it", during)
	}
	if during {
		t.Fatal("the third entry was written while the whole write was held: the whole write does not hold the session's lock")
	}
}

// TestAResyncCarriesTheLeadersRetainedMessages: a follower that lost a
// retained copy of a shard the node leads gets it back from the node's resync.
func TestAResyncCarriesTheLeadersRetainedMessages(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
	m := shardmap.Compute(1, h.order, 2, 2)
	h.applyMap(m)
	waitSettled(h, 1)
	topic, follower := "", ""
	for i := 0; topic == "" && i < 1000; i++ {
		tp := fmt.Sprintf("r/%d", i)
		if s := m.Shards[shardmap.ShardOf("default", tp, 2)]; s.Leader == "n1" && len(s.Followers) > 0 {
			topic, follower = tp, s.Followers[0]
		}
	}
	if topic == "" {
		t.Fatal("precondition: n1 leads no shard with a follower")
	}
	if err := writeRetained(h, "n1", topic, "kept"); err != nil {
		t.Fatal(err)
	}
	if !retainedHeldBy(h, follower, "default", topic) {
		t.Fatalf("precondition: %s does not hold the retained copy", follower)
	}
	gone := store.RetainRecord(&message.Message{Namespace: "default", Topic: topic})
	if _, err := h.nodes[follower].rep.follower.apply(gone); err != nil {
		t.Fatal(err)
	}
	if retainedHeldBy(h, follower, "default", topic) {
		t.Fatalf("precondition: %s still holds the retained copy", follower)
	}
	h.nodes["n1"].rep.resyncPeer(follower)
	if err := h.nodes["n1"].barrier(context.Background(), store.Scope{Namespace: "default", Topic: topic}); err != nil {
		t.Fatal(err)
	}
	if !retainedHeldBy(h, follower, "default", topic) {
		t.Fatalf("%s did not get the retained copy back from n1's resync", follower)
	}
}
