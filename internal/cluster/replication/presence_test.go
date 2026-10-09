package replication

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// counters is a metrics sink shared between the replicator and the test, so
// it locks like any other shared state.
type counters struct {
	mu sync.Mutex
	n  map[string]float64
}

func newCounters() *counters { return &counters{n: map[string]float64{}} }

func (c *counters) Add(name string, labels map[string]string, delta float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[key(name, labels)] += delta
}

func (c *counters) Set(string, map[string]string, float64) {}

func (c *counters) get(name string, labels map[string]string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[key(name, labels)]
}

func key(name string, labels map[string]string) string {
	out := name
	names := make([]string, 0, len(labels))
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		out += "," + k + "=" + labels[k]
	}
	return out
}

var (
	queriesIn      = map[string]string{"direction": "in"}
	queriesSkipped = map[string]string{"direction": "out", "outcome": "skipped"}
	queriesOK      = map[string]string{"direction": "out", "outcome": "ok"}
)

const queryMetric = "hoardmq_retained_queries_total"

// withCounters builds a harness whose every node reports to its own sink.
func withCounters(t *testing.T, names []string, shards, replicas int) (*harness, map[string]*counters) {
	t.Helper()
	sinks := map[string]*counters{}
	var mu sync.Mutex
	next := 0
	h := newHarnessWith(t, names, shards, replicas, func(o *Options) {
		mu.Lock()
		c := newCounters()
		sinks[names[next]] = c
		next++
		mu.Unlock()
		o.Metrics = c
	})
	return h, sinks
}

// waitSettled blocks until every node has finished reconciling the map
// version. The reconcile worker announces presence itself, so a test that
// measures what one write announced must not race it: without this, a
// synchronous announcement is indistinguishable from a deferred one that
// the reconcile worker happened to carry.
func waitSettled(h *harness, version uint64) {
	h.waitFor("every node reconciled to map version", 5*time.Second, func() bool {
		for _, n := range h.order {
			if h.nodes[n].rep.Reconciled() != version {
				return false
			}
		}
		return true
	})
	// A retained shard refuses writes while it moves, which completes once its
	// new followers acknowledge it. A test whose faults hold a move open goes
	// on after a second, as it did before moves existed.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pending := 0
		for _, n := range h.order {
			pending += h.nodes[n].rep.RetainedMovesPending()
		}
		if pending == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitPresence blocks until the node holds a statement from each named peer.
func waitPresence(h *harness, node string, peers ...string) {
	h.waitFor("presence from "+node+"'s peers", 5*time.Second, func() bool {
		r := h.nodes[node].rep
		r.pres.mu.Lock()
		defer r.pres.mu.Unlock()
		for _, p := range peers {
			if _, ok := r.pres.peers[p]; !ok {
				return false
			}
		}
		return true
	})
}

// TestQuerySkippedWhenPeersHoldNothing is the storm's case: nothing is
// retained anywhere, so a subscribe answers locally and no peer is asked.
func TestQuerySkippedWhenPeersHoldNothing(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	waitSettled(h, 1)
	waitPresence(h, "n1", "n2", "n3")

	// A filter whose shard n1 does not lead: without the statement this is
	// one request to its leader and a wait for the answer.
	f := topicIn(3, 1)
	if got := h.nodes["n1"].rep.QueryRetained("default", f, time.Now()); len(got) != 0 {
		t.Fatalf("retained messages from an empty cluster: %v", got)
	}
	if n := sinks["n1"].get(queryMetric, queriesSkipped); n != 1 {
		t.Fatalf("skipped queries = %v, want 1", n)
	}
	for _, peer := range []string{"n2", "n3"} {
		if n := sinks[peer].get(queryMetric, queriesIn); n != 0 {
			t.Fatalf("%s answered %v queries; it should not have been asked", peer, n)
		}
	}
}

// TestQuerySentWhenThePeerHoldsAMatch is the non-vacuity of the test above:
// the skip is not unconditional, and a message that exists is still found.
func TestQuerySentWhenThePeerHoldsAMatch(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	waitSettled(h, 1)
	waitPresence(h, "n1", "n2", "n3")

	f := topicIn(3, 1) // led by n2
	m := &message.Message{Namespace: "default", Topic: f, Payload: []byte("one"), Retain: true}
	if h.nodes["n1"].rep.StoreRetained(m) {
		t.Fatal("non-leader stored locally")
	}
	if err := h.nodes["n1"].barrier(context.Background(), store.Scope{Namespace: "default", Topic: f}); err != nil {
		t.Fatalf("barrier: %v", err)
	}

	got := h.nodes["n1"].rep.QueryRetained("default", f, time.Now())
	if len(got) != 1 || string(got[0].Payload) != "one" {
		t.Fatalf("retained message not returned: %v", got)
	}
	if n := sinks["n1"].get(queryMetric, queriesOK); n != 1 {
		t.Fatalf("answered queries = %v, want 1", n)
	}
	if n := sinks["n1"].get(queryMetric, queriesSkipped); n != 0 {
		t.Fatalf("a peer holding a match was skipped %v times", n)
	}
}

// TestQuerySentWithoutAStatement is invariant 3: no statement means ask.
func TestQuerySentWithoutAStatement(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	waitSettled(h, 1)
	waitPresence(h, "n1", "n2", "n3")

	// Forget what n1 knows, as a link change would.
	h.nodes["n1"].rep.forgetPresence("n2")
	h.nodes["n1"].rep.forgetPresence("n3")

	f := topicIn(3, 1)
	h.nodes["n1"].rep.QueryRetained("default", f, time.Now())
	if n := sinks["n1"].get(queryMetric, queriesSkipped); n != 0 {
		t.Fatalf("skipped %v queries with no statement to justify it", n)
	}
	if n := sinks["n2"].get(queryMetric, queriesIn); n != 1 {
		t.Fatalf("n2 answered %v queries, want 1", n)
	}
}

// presenceOut is the announcement counter; sequences is the same fact read
// from the streams the barrier waits on.
var presenceOut = map[string]string{"direction": "out"}

const presenceMetric = "hoardmq_retained_presence_total"

// sequences returns each peer's outbound stream position: the sequence of
// the last frame pushed, which is what Barrier waits to see acknowledged.
func sequences(r *Replicator) map[string]uint64 {
	out := map[string]uint64{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, p := range r.peers {
		p.mu.Lock()
		out[name] = p.sent
		p.mu.Unlock()
	}
	return out
}

// TestPresenceQueuedBeforeTheRetainedWriteReturns is the ordering the sound
// negative rests on, asserted causally rather than by timing.
//
// The barrier a retained write waits on covers every peer's outbound stream
// (TestEveryLivePeerHasAnOutboundStream establishes that every live peer has
// one). So it is enough to prove the other half here: that the announcement
// is on those streams *before the write returns*, not merely soon after. A
// test that waited for the peers to catch up would pass against an
// announcement deferred to the next tick, which is exactly the
// implementation that loses a message.
func TestPresenceQueuedBeforeTheRetainedWriteReturns(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	waitSettled(h, 1)
	waitPresence(h, "n1", "n2", "n3")
	waitPresence(h, "n3", "n1", "n2")
	waitSettled(h, 1)

	f := topicIn(3, 1) // shard 1, led by n2
	before := sinks["n2"].get(presenceMetric, presenceOut)
	seqBefore := sequences(h.nodes["n2"].rep)

	// The write goes through the store, so the tap runs inside this call.
	h.nodes["n2"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: f, Payload: []byte("one"), Retain: true})

	// Read immediately, on this goroutine, with no wait of any kind.
	if sent := sinks["n2"].get(presenceMetric, presenceOut) - before; sent != 2 {
		t.Fatalf("presence frames queued by the time the write returned = %v, want 2 (one per live peer)", sent)
	}
	for _, peer := range []string{"n1", "n3"} {
		if seq := sequences(h.nodes["n2"].rep)[peer]; seq <= seqBefore[peer] {
			t.Fatalf("%s's outbound is still at %d: the announcement is not on the stream the barrier waits for", peer, seq)
		}
	}
}

// TestAcknowledgedRetainedWriteIsVisibleToAThirdNode is the end-to-end form
// of the same invariant: once the publisher has its acknowledgement, a node
// that had been told the shard was empty must no longer believe it. It
// exercises the forwarded path, where the leader is not the publisher's
// node, and it is the guard that would catch the ordering breaking anywhere
// between the tap and the barrier rather than only in the tap.
func TestAcknowledgedRetainedWriteIsVisibleToAThirdNode(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	waitSettled(h, 1)
	waitPresence(h, "n3", "n1", "n2")

	f := topicIn(3, 1) // led by n2
	if h.nodes["n3"].rep.mayHold("n2", "default", f) {
		t.Fatal("n3 does not believe n2 is empty; the test proves nothing")
	}

	m := &message.Message{Namespace: "default", Topic: f, Payload: []byte("one"), Retain: true}
	if h.nodes["n1"].rep.StoreRetained(m) {
		t.Fatal("non-leader stored locally")
	}
	if err := h.nodes["n1"].barrier(context.Background(), store.Scope{Namespace: "default", Topic: f}); err != nil {
		t.Fatalf("barrier: %v", err)
	}

	// No retry loop: a wait here would hide the defect this test is for.
	if !h.nodes["n3"].rep.mayHold("n2", "default", f) {
		t.Fatal("n3 still believes n2 holds nothing for the shard after the write was acknowledged")
	}
	got := h.nodes["n3"].rep.QueryRetained("default", f, time.Now())
	if len(got) != 1 || string(got[0].Payload) != "one" {
		t.Fatalf("acknowledged retained message invisible to a third node: %v", got)
	}
}

// TestPresenceShrinksAfterRemoval covers the withdrawal: a shard leaves the
// set on a recompute, and the peer resumes querying it.
func TestPresenceShrinksAfterRemoval(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2"}, 2, 1)
	h.applyMap(shardmap.Compute(1, h.order, 1, 2))
	waitSettled(h, 1)
	waitPresence(h, "n1", "n2")

	// A topic in a shard n2 leads.
	f := ""
	for i := 0; ; i++ {
		c := topicIn(2, i%2)
		if h.nodes["n2"].table.Current().Shards[shardmap.ShardOf("default", c, 2)].Leader == "n2" {
			f = c
			break
		}
		if i > 4 {
			t.Fatal("no topic led by n2")
		}
	}
	shard := shardmap.ShardOf("default", f, 2)

	h.nodes["n2"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: f, Payload: []byte("one"), Retain: true})
	h.waitFor("n1 hears the shard is occupied", 5*time.Second, func() bool {
		return h.nodes["n1"].rep.mayHold("n2", "default", f)
	})

	// Remove it. The set does not shrink from the tap — only a recompute
	// removes a shard — so the shard is still claimed until one runs.
	h.nodes["n2"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: f})
	h.nodes["n2"].rep.pres.mu.Lock()
	stillClaimed := h.nodes["n2"].rep.pres.own[shard]
	h.nodes["n2"].rep.pres.mu.Unlock()
	if !stillClaimed {
		t.Fatal("the tap removed a shard from the set; only a recompute may")
	}

	h.nodes["n2"].rep.refreshPresence()
	h.waitFor("n1 hears the shard is empty", 5*time.Second, func() bool {
		return !h.nodes["n1"].rep.mayHold("n2", "default", f)
	})
}

// TestWildcardQuerySkippedOnlyWhenThePeerIsWhollyEmpty: a wildcard filter
// has no single shard, so one occupied shard is enough to make the peer
// worth asking.
func TestWildcardQuerySkippedOnlyWhenThePeerIsWhollyEmpty(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	waitSettled(h, 1)
	waitPresence(h, "n1", "n2", "n3")

	// Empty everywhere: both peers are skipped.
	h.nodes["n1"].rep.QueryRetained("default", "t/#", time.Now())
	if n := sinks["n1"].get(queryMetric, queriesSkipped); n != 2 {
		t.Fatalf("skipped %v of 2 peers for a wildcard over an empty cluster", n)
	}

	// One message on n2 makes n2 worth asking; n3 is still skipped.
	f := topicIn(3, 1)
	m := &message.Message{Namespace: "default", Topic: f, Payload: []byte("one"), Retain: true}
	if h.nodes["n1"].rep.StoreRetained(m) {
		t.Fatal("non-leader stored locally")
	}
	if err := h.nodes["n1"].barrier(context.Background(), store.Scope{Namespace: "default", Topic: f}); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	got := h.nodes["n1"].rep.QueryRetained("default", "t/#", time.Now())
	if len(got) != 1 {
		t.Fatalf("wildcard gather: %v", got)
	}
	if n := sinks["n1"].get(queryMetric, queriesSkipped); n != 3 {
		t.Fatalf("skipped queries = %v, want 3 (two empty, then n3 alone)", n)
	}
}

// TestEveryLivePeerHasAnOutboundStream asserts the fact the no-new-barrier-
// dependency argument rests on: an outbound exists for every peer that has
// come up, whether or not it holds a replica, so pushing a presence frame to
// all peers adds nothing the barrier did not already wait for. If a future
// change makes outbound streams lazy, this fails rather than a comment
// quietly becoming false.
func TestEveryLivePeerHasAnOutboundStream(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	// No map, so nothing has been replicated to anyone and no shard has a
	// target: any outbound present is one peer-up created.
	h.waitFor("outbound streams for every peer", 5*time.Second, func() bool {
		r := h.nodes["n1"].rep
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.peers["n2"] != nil && r.peers["n3"] != nil
	})
	for _, p := range h.nodes["n1"].tr.Peers() {
		if p.State != transport.Up {
			t.Fatalf("peer %s is %v; the premise of the test is that they are up", p.Name, p.State)
		}
	}
}

// TestUnknownFrameKindIsAcknowledged covers the rolling upgrade the README
// promises: a node running the previous build receives a frame kind it does
// not know. The stream's acknowledgement is cumulative, so passing over the
// frame leaves every barrier the sender holds over it waiting for the
// timeout — the new nodes' publishes would fail against the old ones for as
// long as the upgrade took.
func TestUnknownFrameKindIsAcknowledged(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2"}, 2, 1)
	h.applyMap(shardmap.Compute(1, h.order, 1, 2))
	waitSettled(h, 1)

	// A kind from a build newer than either of these nodes.
	h.nodes["n1"].rep.outboundFor("n2").push(frame{Kind: 200})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.nodes["n1"].barrier(ctx); err != nil {
		t.Fatalf("barrier over a frame the peer could not interpret: %v", err)
	}
}

// TestConcurrentFirstWritesEachReachEveryPeer runs the acknowledged-
// visibility invariant with several writers at once: each checks, for
// itself, immediately after its own barrier returns and with no wait, that
// no peer still believes its shard is empty. A wait would let the periodic
// repair or another writer's announcement supply what this write should
// have.
//
// It does not discriminate announcePresence's postcondition. The store holds
// its own lock across the tap, so two notePresence calls cannot overlap, and
// the interleaving that would break a weaker postcondition needs a tap
// racing the reconcile worker's recompute — which this harness cannot force.
// It is kept as a multi-writer guard on the invariant itself, not as the
// control for that change; see test.md.
func TestConcurrentFirstWritesEachReachEveryPeer(t *testing.T) {
	h, _ := withCounters(t, []string{"n1", "n2", "n3"}, 8, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 8))
	waitSettled(h, 1)

	// Every shard n2 leads, so each write is a first write to its own shard.
	cur := h.nodes["n2"].table.Current()
	var topics []string
	for sh := range cur.Shards {
		if cur.Shards[sh].Leader == "n2" {
			topics = append(topics, topicIn(8, sh))
		}
	}
	if len(topics) < 2 {
		t.Fatalf("n2 leads %d shards; the race needs at least two", len(topics))
	}

	var wg sync.WaitGroup
	fail := make(chan string, len(topics)*2)
	for _, tp := range topics {
		wg.Add(1)
		go func(tp string) {
			defer wg.Done()
			h.nodes["n2"].sessions.InjectRetained(&message.Message{Namespace: "default", Topic: tp, Payload: []byte("x"), Retain: true})
			if err := h.nodes["n2"].barrier(context.Background()); err != nil {
				fail <- "barrier for " + tp + ": " + err.Error()
				return
			}
			for _, peer := range []string{"n1", "n3"} {
				if !h.nodes[peer].rep.mayHold("n2", "default", tp) {
					fail <- peer + " still believes n2 holds nothing for " + tp + " after the write was acknowledged"
				}
			}
		}(tp)
	}
	wg.Wait()
	close(fail)
	for msg := range fail {
		t.Error(msg)
	}
}

// TestLinkUpKeepsWhatThePeerSaid: a link's first frames can be applied before
// this node hears that the link is up, and the peer will not repeat a
// statement it has recorded as told. Up must not discard it; a link going
// away must.
func TestLinkUpKeepsWhatThePeerSaid(t *testing.T) {
	h, sinks := withCounters(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	waitSettled(h, 1)
	waitPresence(h, "n1", "n2", "n3")
	r := h.nodes["n1"].rep
	held := func(peer string) bool {
		r.pres.mu.Lock()
		defer r.pres.mu.Unlock()
		_, ok := r.pres.peers[peer]
		return ok
	}

	before := sinks["n1"].get(presenceMetric, presenceOut)
	r.peerChanged("n2", transport.Up)
	if !held("n2") {
		t.Fatal("n2's statement was discarded when its link was reported up")
	}
	if sent := sinks["n1"].get(presenceMetric, presenceOut) - before; sent != 1 {
		t.Fatalf("n1 announced %v times to a peer whose link came up, want 1", sent)
	}

	r.peerChanged("n2", transport.Down)
	if held("n2") {
		t.Fatal("n2's statement survived its link going away")
	}
	if !held("n3") {
		t.Fatal("n3's statement was discarded by n2's link")
	}
}
