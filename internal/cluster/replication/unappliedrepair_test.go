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

// TestUnappliedBarrierWaitsForTheRepair: n2 refuses n1's record for k as
// unapplied (its copy of k is gone), and n1 repairs k by sending it whole.
// The repair's push is what clears k's refusal, so a barrier over the refused
// record that started before it has a target below the repair: it must not
// pass on the acknowledgement of the refused record's own batch, which
// changed nothing on n2, but only once the repair itself is acknowledged.
// n2's fsync of the repair is held to keep that acknowledgement back. The
// repair is the session whole, or a delete when n1 no longer serves k.
func TestUnappliedBarrierWaitsForTheRepair(t *testing.T) {
	t.Run("repaired whole", func(t *testing.T) { unappliedBarrier(t, true) })
	t.Run("repaired by a delete", func(t *testing.T) { unappliedBarrier(t, false) })
}

func unappliedBarrier(t *testing.T, serves bool) {
	held := newHeldSync()
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(o *Options) {
		if o.Node != "n2" {
			return
		}
		replica, err := store.Open(t.TempDir(), store.Options{Logger: quiet, Sync: held.sync})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = replica.Close() })
		o.Replica = replica
	})
	m := shardmap.Compute(1, h.order, 2, 2)
	h.applyMap(m)
	waitSettled(h, 1)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	k := clientTargeting(m, 2, "n1", "n2", "k")
	h.setLease("default", k, "n1", 1)
	key := store.Key{Namespace: "default", ClientID: k, Epoch: 1}
	scopeK := []store.Scope{{Namespace: "default", ClientID: k}}
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: k, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.rep.Barrier(context.Background(), scopeK); err != nil {
		t.Fatal(err)
	}
	entry := store.Entry{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("queued")}, QoS: 1}
	if serves {
		feedSession(n1, k, entry)
	}
	// n2 loses its copy, so n1's next record for k changes nothing there.
	if _, err := n2.rep.follower.apply(store.DeleteRecord("default", k)); err != nil {
		t.Fatal(err)
	}

	lookups, release := holdLookups(t, n2, k)
	unblockSync := sync.OnceFunc(func() { close(held.release) })
	t.Cleanup(unblockSync)
	p := n1.rep.outboundFor("n2")
	if err := n1.own.Enqueue(key, entry); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, "n2 never looked k up", lookups[k])
	barrier := barrierWaiting(t, h, n1.rep, p, scopeK)
	p.mu.Lock()
	enqSeq := p.sent
	p.mu.Unlock()
	held.armed.Store(true)
	release[k]()

	// n2 refuses the enqueue (nothing dirty, so its batch is acknowledged at
	// once), n1 repairs k, and n2's fsync of the repair is held.
	select {
	case <-held.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: n2 never began the fsync of the repair")
	}
	if got := repairs(sinks, "n1", "snapshot") + repairs(sinks, "n1", "delete"); got != 1 {
		t.Fatalf("precondition not reached: n1 repaired k %v times, want once", got)
	}
	h.waitFor("n2's acknowledgement of the refused record's batch", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.ackSeq >= enqSeq
	})
	p.mu.Lock()
	acked, sent := p.ackSeq, p.sent
	p.mu.Unlock()
	select {
	case err := <-barrier:
		if err == nil {
			t.Fatalf("the barrier over k passed before the repair was acknowledged (acknowledged %d, enqueue at %d, repair through %d, repair fsyncs completed %d)", acked, enqSeq, sent, held.done.Load())
		}
		t.Fatalf("the barrier over k failed before the repair was acknowledged: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	unblockSync()
	select {
	case err := <-barrier:
		if err != nil {
			t.Fatalf("the barrier over k once the repair was acknowledged: %v", err)
		}
	case <-time.After(n1.rep.o.AckTimeout + 2*time.Second):
		t.Fatal("the barrier over k outlived its timeout")
	}
	if got := held.done.Load(); got < 1 {
		t.Fatalf("the barrier over k passed with %d fsyncs of the repair completed on n2", got)
	}
	ss := n2.rep.ReplicaState().Find("default", k)
	switch {
	case serves && (ss == nil || len(ss.Entries) != 1):
		t.Fatalf("after the repair n2 holds %+v for k, want the session with its one entry", ss)
	case !serves && ss != nil:
		t.Fatalf("after the repair n2 holds %+v for k, want no copy", ss)
	}
}

// TestBarrierWaitsForTheWholeWriteThatClearedItsRefusal: the same shape
// without a repair. k's frame is refused, and before the barrier over it looks
// again the owner writes k whole, which clears the refusal. The barrier must
// wait for that whole write's acknowledgement, not pass on the refused frame's.
func TestBarrierWaitsForTheWholeWriteThatClearedItsRefusal(t *testing.T) {
	p := &outbound{name: "peer"}
	p.cond = sync.NewCond(&p.mu)
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "k", Epoch: 2})
	up := func(string) bool { return true }
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- p.wait(ctx, up, []store.Scope{{Namespace: "default", ClientID: "k"}})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		waiting := len(p.waiters) > 0
		p.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("precondition not reached: the barrier never waited")
		}
		time.Sleep(time.Millisecond)
	}
	p.failedAt(1, "epoch 2 is behind lease epoch 3", store.Scope{})
	del := store.DeleteRecord("default", "k")
	p.push(frame{Kind: kindAppend, Type: del.Type, Namespace: "default", ClientID: "k", Epoch: 3, Payload: del.Payload})
	p.refusalArrived()
	p.acked(1)
	select {
	case err := <-done:
		t.Fatalf("the barrier over k returned %v on the refused frame's acknowledgement, before the whole write's", err)
	case <-time.After(200 * time.Millisecond):
	}
	p.acked(2)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the barrier over k once the whole write was acknowledged: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the barrier over k did not pass once the whole write was acknowledged")
	}
	held := func() int {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.clearedBy)
	}
	if n := held(); n != 0 {
		t.Fatalf("%d cleared refusals still held after the whole write was acknowledged, want 0", n)
	}

	// A break fails everything pushed, so what it held is dropped too.
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "k", Epoch: 3})
	p.failedAt(3, "epoch 3 is behind lease epoch 4", store.Scope{})
	p.push(frame{Kind: kindAppend, Type: del.Type, Namespace: "default", ClientID: "k", Epoch: 4, Payload: del.Payload})
	if n := held(); n != 1 {
		t.Fatalf("precondition: %d cleared refusals held after a second whole write cleared one, want 1", n)
	}
	p.broken()
	if n := held(); n != 0 {
		t.Fatalf("%d cleared refusals still held after the stream broke, want 0", n)
	}
}
