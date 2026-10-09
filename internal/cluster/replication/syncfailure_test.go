package replication

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// failingSync is a replica store's fsync that, once armed, fails the next
// call without syncing, as a disk that reports a write-back error does. The
// pages it leaves are synced by the next call; on a real disk they may
// already be gone.
type failingSync struct {
	armed  atomic.Bool
	failed chan struct{}
	once   sync.Once
	after  atomic.Int64 // successful fsyncs after the failure
}

func newFailingSync() *failingSync {
	return &failingSync{failed: make(chan struct{})}
}

var errInjectedFsync = errors.New("injected fsync failure")

func (s *failingSync) sync(f *os.File) error {
	if s.armed.CompareAndSwap(true, false) {
		s.once.Do(func() { close(s.failed) })
		return errInjectedFsync
	}
	err := f.Sync()
	select {
	case <-s.failed:
		if err == nil {
			s.after.Add(1)
		}
	default:
	}
	return err
}

func (s *failingSync) waitFailed(t *testing.T) {
	t.Helper()
	select {
	case <-s.failed:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: the armed replica fsync never ran")
	}
}

// withFailingReplica builds a harness whose node follower's replica store
// fsyncs through s.
func withFailingReplica(t *testing.T, names []string, shards, replicas int, follower string, s *failingSync) (*harness, map[string]*counters) {
	return withCountersAnd(t, names, shards, replicas, func(o *Options) {
		if o.Node != follower {
			return
		}
		replica, err := store.Open(t.TempDir(), store.Options{Logger: quiet, Sync: s.sync})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = replica.Close() })
		o.Replica = replica
	})
}

// TestFailedReplicaSyncIsNotAcknowledged: a follower whose replica fsync
// failed holds a copy whose writes may never reach the disk, and a later
// successful fsync does not show they did. So no acknowledgement it sends may
// cover them: not the next batch's with nothing to apply, not the next
// batch's after a sync that succeeds, not a forwarded retained write's, and
// not another peer's stream whose writes the failed fsync also covered. Each
// case starts a barrier over the lost record before the failure and requires
// it not to pass, then requires the stream to recover: a barrier started
// afterwards passes once the follower has synced a replacement.
//
// The follower's report of the failure makes the owner fail the barrier on
// its own, so each "rejection lost" case drops that report: what then keeps
// the barrier from passing is that the follower acknowledges nothing, not
// even the owner's probe, until the owner has failed the lost frames.
func TestFailedReplicaSyncIsNotAcknowledged(t *testing.T) {
	for _, next := range []string{"refused", "applied", "retained"} {
		name := map[string]string{"refused": "next batch applies nothing", "applied": "next batch syncs", "retained": "retained write acknowledged"}[next]
		t.Run(name, func(t *testing.T) { failedSyncThenNext(t, next, false) })
		t.Run(name+", rejection lost", func(t *testing.T) { failedSyncThenNext(t, next, true) })
	}
	t.Run("another stream's fsync", failedSyncOnAnotherStream)
}

// holdLookups makes n's lease lookups of the named clients wait until the
// returned release for that client is called; held reports each one's first
// lookup.
func holdLookups(t *testing.T, n *tnode, ids ...string) (held map[string]chan struct{}, release map[string]func()) {
	t.Helper()
	held, release = map[string]chan struct{}{}, map[string]func(){}
	gates := map[string]chan struct{}{}
	onces := map[string]*sync.Once{}
	for _, id := range ids {
		held[id], gates[id], onces[id] = make(chan struct{}), make(chan struct{}), &sync.Once{}
		release[id] = sync.OnceFunc(func() { close(gates[id]) })
		t.Cleanup(release[id])
	}
	n.leases.onLookup = func(_, id string) {
		if g, ok := gates[id]; ok {
			onces[id].Do(func() { close(held[id]) })
			<-g
		}
	}
	return held, release
}

func waitClosed(t *testing.T, what string, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("precondition not reached: %s", what)
	}
}

// barrierWaiting starts a barrier over keys on r and returns once it waits on
// p, so the sequence it waits for is fixed before the test goes on.
func barrierWaiting(t *testing.T, h *harness, r *Replicator, p *outbound, keys []store.Scope) chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.Barrier(context.Background(), keys) }()
	h.waitFor("the barrier to wait on the stream", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.waiters) > 0
	})
	return done
}

// firstDrop is a fault source that reports the first frame it drops.
type firstDrop struct {
	dropped chan struct{}
	once    sync.Once
}

func (s *firstDrop) Int63() int64 {
	s.once.Do(func() { close(s.dropped) })
	return 0
}
func (s *firstDrop) Seed(int64) {}

// failedSyncThenNext fails n2's fsync of the batch holding a, with a barrier
// over a already waiting on n1, then has n1 send n2 one more frame in its own
// batch: a record n2 refuses (nothing to sync), a record n2 applies (its sync
// succeeds), or a retained write for a shard n2 leads (acknowledged on its own
// barrier). With lose, n2's first acknowledgement-class frame to n1 after the
// failure is dropped: its report of the failure, or on a follower that sends
// none, the next frame's acknowledgement.
func failedSyncThenNext(t *testing.T, next string, lose bool) {
	fs := newFailingSync()
	h, sinks := withFailingReplica(t, []string{"n1", "n2"}, 2, 2, "n2", fs)
	m := shardmap.Compute(1, h.order, 2, 2)
	h.applyMap(m)
	waitSettled(h, 1)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	a := clientTargeting(m, 2, "n1", "n2", "a")
	h.setLease("default", a, "n1", 1)
	feedSession(n1, a)
	scopeA := []store.Scope{{Namespace: "default", ClientID: a}}
	p := n1.rep.outboundFor("n2")
	seq := func() uint64 {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.sent
	}
	held, release := holdLookups(t, n2, a)
	var drop *firstDrop
	if lose {
		drop = &firstDrop{dropped: make(chan struct{})}
		n2.tr.SetFault("n1", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAck}, Rand: rand.New(drop)}) //nolint:gosec // a fixed source
	}

	fs.armed.Store(true)
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: a, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	aSeq := seq()
	waitClosed(t, "n2 never looked a up", held[a])
	barrier := barrierWaiting(t, h, n1.rep, p, scopeA)
	release[a]()
	fs.waitFailed(t)
	failedAt := time.Now()
	if n2.rep.ReplicaState().Find("default", a) == nil {
		t.Fatal("precondition not reached: n2 has not applied a")
	}
	resyncs := func() float64 {
		return sinks["n1"].get("hoardmq_replication_resyncs_total", map[string]string{"peer": "n2"})
	}

	switch next {
	case "refused":
		x := clientTargeting(m, 2, "n1", "n2", "x")
		n1.leases.set("default", x, "n1", 1)
		n2.leases.set("default", x, "n2", 2) // stale on n2: refused, nothing to sync
		if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: x, Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	case "applied":
		b := clientTargeting(m, 2, "n1", "n2", "b")
		h.setLease("default", b, "n1", 1)
		if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: b, Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	case "retained":
		tp := topicIn(2, 1)
		if l := m.Shards[shardmap.ShardOf("default", tp, 2)].Leader; l != "n2" {
			t.Fatalf("precondition: %s's shard is led by %s, want n2", tp, l)
		}
		if n1.rep.StoreRetained(&message.Message{Namespace: "default", Topic: tp, Payload: []byte("r"), Retain: true}) {
			t.Fatal("precondition: the retained write was not forwarded to n2")
		}
	}
	if lose {
		waitClosed(t, "n2 sent n1 nothing after the failure", drop.dropped)
		n2.tr.SetFault("n1", nil)
	} else {
		// The report of the failure is what starts the repair, without waiting
		// for the owner to ask.
		h.waitFor("n1 to resync n2 on n2's report of the failure", n1.rep.o.AckTimeout/2, func() bool { return resyncs() >= 1 })
	}

	// The barrier over a has waited since before the failure. Whatever n2
	// acknowledges next arrives within its timeout, a probe's included.
	var err error
	select {
	case err = <-barrier:
	case <-time.After(n1.rep.o.AckTimeout + 2*time.Second):
		t.Fatal("the barrier over a outlived its timeout")
	}
	p.mu.Lock()
	acked, sent := p.ackSeq, p.sent
	p.mu.Unlock()
	if err == nil {
		t.Errorf("a's barrier passed after n2's fsync of a failed (acknowledged mark %d, a at %d, %s next, %d pushed, rejection lost %v)", acked, aSeq, next, sent, lose)
	}

	// Recovery: a barrier started now passes once n2 has synced a copy of a
	// that it wrote after the failure.
	h.waitFor("a barrier over a to pass after the failure", 4*n1.rep.o.AckTimeout, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), n1.rep.o.AckTimeout)
		defer cancel()
		return n1.rep.Barrier(ctx, scopeA) == nil
	})
	if got := fs.after.Load(); got < 1 {
		t.Errorf("a's barrier passed with %d successful fsyncs on n2 after the failure", got)
	}
	if got := resyncs(); got < 1 {
		t.Errorf("n1 resynced n2 %v times after n2's fsync failed, want at least once", got)
	}
	t.Logf("recovered %v after the failure", time.Since(failedAt).Round(time.Millisecond))
}

// failedSyncOnAnotherStream: n2 follows n1 and n3. n2's worker for n3 has
// written c to the replica store and not yet synced it when n1's batch's
// fsync, which flushes and covers c's bytes too, fails; the fsync n2 then
// makes for c's batch succeeds. c's barrier on n3 must not pass on it.
func failedSyncOnAnotherStream(t *testing.T) {
	fs := newFailingSync()
	h, _ := withFailingReplica(t, []string{"n1", "n2", "n3"}, 3, 2, "n2", fs)
	m := shardmap.Compute(1, h.order, 2, 3)
	h.applyMap(m)
	waitSettled(h, 1)
	n1, n2, n3 := h.nodes["n1"], h.nodes["n2"], h.nodes["n3"]
	a := clientTargeting(m, 3, "n1", "n2", "a")
	x, c, y := clientTargeting(m, 3, "n3", "n2", "x"), clientTargeting(m, 3, "n3", "n2", "c"), clientTargeting(m, 3, "n3", "n2", "y")
	h.setLease("default", a, "n1", 1)
	for _, id := range []string{x, c, y} {
		h.setLease("default", id, "n3", 1)
	}
	feedSession(n3, c)
	p := n3.rep.outboundFor("n2")

	// x's lookup is held so that c and y queue behind it and are applied as
	// one batch; y's is held so that batch stops after c is written and
	// before it syncs.
	held, release := holdLookups(t, n2, x, y)
	upsert := func(n *tnode, id string) {
		t.Helper()
		if err := n.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	}
	upsert(n3, x)
	waitClosed(t, "n2 never looked x up", held[x])
	upsert(n3, c)
	upsert(n3, y)
	h.waitFor("c and y queued behind x", 10*time.Second, func() bool {
		n2.rep.follower.wmu.Lock()
		w := n2.rep.follower.workers["n3"]
		n2.rep.follower.wmu.Unlock()
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.queue) == 2
	})
	scopeC := []store.Scope{{Namespace: "default", ClientID: c}}
	barrier := barrierWaiting(t, h, n3.rep, p, scopeC)
	release[x]()
	waitClosed(t, "n2 never looked y up", held[y])
	if n2.rep.ReplicaState().Find("default", c) == nil {
		t.Fatal("precondition not reached: n2 has not applied c")
	}

	fs.armed.Store(true)
	upsert(n1, a)
	fs.waitFailed(t)
	release[y]()

	var err error
	select {
	case err = <-barrier:
	case <-time.After(n3.rep.o.AckTimeout + 2*time.Second):
		t.Fatal("the barrier over c outlived its timeout")
	}
	if err == nil {
		t.Errorf("c's barrier on n3 passed after an n2 fsync that covered c failed (successful fsyncs since: %d)", fs.after.Load())
	}
	h.waitFor("a barrier over c to pass after the failure", 4*n3.rep.o.AckTimeout, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), n3.rep.o.AckTimeout)
		defer cancel()
		return n3.rep.Barrier(ctx, scopeC) == nil
	})
}

// TestAckGate: when a worker that lost writes may acknowledge again.
func TestAckGate(t *testing.T) {
	const interval = time.Second
	t0 := time.Unix(1000, 0)
	type step struct {
		mark    *uint64 // a resync mark: its FailedBelow, 0 for an older sender's
		fail    uint64  // a part through this sequence was not durable
		restart bool
		at      time.Duration
		ack     bool // a durable settle then: acknowledge?
		reject  bool // and tell the sender again?
	}
	u := func(v uint64) *uint64 { return &v }
	cases := []struct {
		name  string
		steps []step
	}{
		{"nothing lost acknowledges", []step{{ack: true}}},
		{"lost, no mark", []step{{fail: 5}, {at: 2 * interval}}},
		{"a mark below the lost frames", []step{{fail: 5}, {mark: u(3), at: 100 * time.Millisecond}}},
		{"a mark at the lost mark has not failed it", []step{{fail: 5}, {mark: u(5), at: 100 * time.Millisecond}}},
		{"a mark past the lost frames clears", []step{{fail: 5}, {mark: u(6), ack: true}}},
		{"an older sender's mark clears", []step{{fail: 5}, {mark: u(0), ack: true}}},
		{"a mark before a later failure proves nothing", []step{{fail: 5}, {mark: u(9)}, {fail: 8}, {at: 100 * time.Millisecond}}},
		{"the higher of two failures holds", []step{{fail: 8}, {fail: 5}, {mark: u(7), at: 100 * time.Millisecond}, {mark: u(9), ack: true}}},
		{"a stale probe after an interval rejects again", []step{{fail: 5}, {mark: u(3), at: interval, reject: true}}},
		{"at most once an interval", []step{{fail: 5}, {mark: u(3), at: interval, reject: true}, {mark: u(3), at: interval + 100*time.Millisecond}}},
		{"a restart clears", []step{{fail: 5}, {restart: true, ack: true}}},
		{"after clearing, acknowledges", []step{{fail: 5}, {mark: u(6), ack: true}, {ack: true}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var g ackGate
			for i, s := range tc.steps {
				now := t0.Add(s.at)
				switch {
				case s.fail > 0:
					g.failed(s.fail, now)
					continue
				case s.restart:
					g.restarted()
				case s.mark != nil:
					g.saw(*s.mark)
				}
				if s.mark != nil && i+1 < len(tc.steps) && tc.steps[i+1].fail > 0 {
					continue // the mark and the next failure fall in one part
				}
				ack, reject := g.settled(now, interval)
				if ack != s.ack || reject != s.reject {
					t.Fatalf("step %d: settled = acknowledge %v, reject %v; want %v, %v (lost %d)", i, ack, reject, s.ack, s.reject, g.lost)
				}
				if ack != g.open() {
					t.Fatalf("step %d: open() = %v after a settle that acknowledged %v", i, g.open(), ack)
				}
			}
		})
	}
}

// TestSenderRestartClearsTheLostMark: a frame below the expected sequence is
// the sender counting from the start again, and a restarted sender's barriers
// cover none of the old stream's frames.
func TestSenderRestartClearsTheLostMark(t *testing.T) {
	w := &worker{next: 10}
	w.gate.failed(9, time.Now())
	w.checkSequence(frame{Seq: 1})
	if !w.gate.open() {
		t.Fatalf("after the sender restarted its count the gate holds lost %d, want none", w.gate.lost)
	}
}
