package replication

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// heldSync is a replica store's fsync that, once armed, blocks until
// released. A killed process leaves its unsynced writes in the page cache, so
// a crash test cannot see an acknowledgement that came before the fsync.
type heldSync struct {
	armed   atomic.Bool
	entered chan struct{}
	enter   sync.Once
	release chan struct{}
	done    atomic.Int64 // fsyncs completed while armed
}

func newHeldSync() *heldSync {
	return &heldSync{entered: make(chan struct{}), release: make(chan struct{})}
}

func (h *heldSync) sync(f *os.File) error {
	if !h.armed.Load() {
		return f.Sync()
	}
	h.enter.Do(func() { close(h.entered) })
	<-h.release
	err := f.Sync()
	h.done.Add(1)
	return err
}

// TestRejectionDoesNotAcknowledgeUnsyncedFrames: the follower applies a and
// refuses k in one batch, and answers the refusal before the batch's fsync.
// Acknowledgements are cumulative, so an acknowledgement at k's sequence would
// cover a while the follower's copy of it is still unsynced. While the fsync is
// held, a's barrier must not pass; once it completes, it does. k is refused as
// stale, or as unapplied, which the owner answers with a repair; each has its
// own branch where the owner reads the rejection.
func TestRejectionDoesNotAcknowledgeUnsyncedFrames(t *testing.T) {
	t.Run("stale", func(t *testing.T) { rejectionOverUnsynced(t, false) })
	t.Run("unapplied", func(t *testing.T) { rejectionOverUnsynced(t, true) })
}

func rejectionOverUnsynced(t *testing.T, unapplied bool) {
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
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]

	// x is refused on n2 and costs no fsync; its lookup is held so that a, k
	// and s queue behind it and are applied as one batch. s is refused after
	// k: once n1 has recorded s's refusal, it has finished reading k's.
	x, a, k, s := clientIn(2, 0, "x"), clientIn(2, 0, "a"), clientIn(2, 0, "k"), clientIn(2, 0, "s")
	for _, id := range []string{x, a, k, s} {
		n1.leases.set("default", id, "n1", 1)
	}
	n2.leases.set("default", a, "n1", 1)
	n2.leases.set("default", x, "n2", 2) // n2 has seen these leases move: n1's records are stale there
	n2.leases.set("default", s, "n2", 2)
	if unapplied {
		// n2 holds k, then loses its copy, so n1's next record for k changes
		// nothing there.
		n2.leases.set("default", k, "n1", 1)
		if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: k, Epoch: 1}); err != nil {
			t.Fatal(err)
		}
		if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: k}}); err != nil {
			t.Fatal(err)
		}
		if _, err := n2.rep.follower.apply(store.DeleteRecord("default", k)); err != nil {
			t.Fatal(err)
		}
	} else {
		n2.leases.set("default", k, "n2", 2)
	}
	lookupHeld := make(chan struct{})
	var lookupOnce sync.Once
	releaseLookup := make(chan struct{})
	unblockLookup := sync.OnceFunc(func() { close(releaseLookup) })
	n2.leases.onLookup = func(_, id string) {
		if id == x {
			lookupOnce.Do(func() { close(lookupHeld) })
			<-releaseLookup
		}
	}
	unblockSync := sync.OnceFunc(func() { close(held.release) })
	t.Cleanup(unblockSync)
	t.Cleanup(unblockLookup)

	p := n1.rep.outboundFor("n2")
	seqOf := func() uint64 {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.sent
	}
	upsert := func(id string) uint64 {
		t.Helper()
		if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
			t.Fatal(err)
		}
		return seqOf()
	}
	upsert(x)
	select {
	case <-lookupHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: n2 never looked x up")
	}
	held.armed.Store(true)
	aSeq := upsert(a)
	var kSeq uint64
	if unapplied {
		if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: k, Epoch: 1}, store.Entry{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b"}, QoS: 1}); err != nil {
			t.Fatal(err)
		}
		kSeq = seqOf()
	} else {
		kSeq = upsert(k)
	}
	sSeq := upsert(s)
	h.waitFor("a, k and s queued behind the held lookup", 10*time.Second, func() bool {
		n2.rep.follower.wmu.Lock()
		w := n2.rep.follower.workers["n1"]
		n2.rep.follower.wmu.Unlock()
		if w == nil {
			return false
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		seen := map[uint64]bool{}
		for _, fr := range w.queue {
			seen[fr.Seq] = true
		}
		return seen[aSeq] && seen[kSeq] && seen[sSeq]
	})
	unblockLookup()

	// The batch holding a, k and s has refused k and s and waits in its fsync.
	select {
	case <-held.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: n2 never began the batch's fsync")
	}
	h.waitFor("n2's refusal of s recorded on n1", 10*time.Second, func() bool {
		for sc := range n1.rep.RefusedKeys()["n2"] {
			if sc.ClientID == s {
				return true
			}
		}
		return false
	})
	if got := held.done.Load(); got != 0 {
		t.Fatalf("precondition not held: %d fsyncs completed on n2 since a was pushed", got)
	}
	if n2.rep.ReplicaState().Find("default", a) == nil {
		t.Fatal("precondition not reached: n2 has not applied a")
	}
	if unapplied {
		if got := repairs(sinks, "n1", "snapshot") + repairs(sinks, "n1", "delete"); got != 1 {
			t.Fatalf("precondition not reached: n1 repaired k %v times, want once (k was not refused as unapplied)", got)
		}
	}
	p.mu.Lock()
	acked := p.ackSeq
	p.mu.Unlock()
	if acked >= aSeq {
		t.Errorf("n1's acknowledged mark is %d, at or past a's frame %d, while n2 has not synced it (k refused at %d)", acked, aSeq, kSeq)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	err := n1.rep.Barrier(ctx, []store.Scope{{Namespace: "default", ClientID: a}})
	cancel()
	if err == nil {
		t.Error("a's barrier passed while n2's copy of a was unsynced")
	}

	unblockSync()
	if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: a}}); err != nil {
		t.Errorf("a's barrier once n2 synced: %v", err)
	}
	if got := held.done.Load(); got < 1 {
		t.Errorf("a's barrier passed with %d fsyncs completed on n2", got)
	}
	err = n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: s}})
	if !errors.Is(err, ErrReplicaUnavailable) || barrierReason(err) != "not-held" {
		t.Errorf("s's barrier: %v, want not-held", err)
	}
}
