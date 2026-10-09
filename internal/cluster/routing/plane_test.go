package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

const secret = "routing-test-shared-secret-of-enough-bytes"

// fakeSink records what a node was asked to deliver.
type fakeSink struct {
	mu     sync.Mutex
	direct []*message.Message
	groups []string // "group|topic|payload"
}

func (f *fakeSink) InjectForwarded(m *message.Message) (int, []store.Scope, error) {
	f.injectForwarded(m)
	// One notional local subscriber, named so the plane's durability wait has
	// something to cover — an empty set would skip the wait entirely.
	return 1, []store.Scope{{Namespace: m.Namespace, ClientID: "fake-sub"}}, nil
}

func (f *fakeSink) injectForwarded(m *message.Message) {
	f.mu.Lock()
	f.direct = append(f.direct, m)
	f.mu.Unlock()
}

func (f *fakeSink) Durable(context.Context, []store.Scope) error { return nil }

func (f *fakeSink) InjectGroup(m *message.Message, group string) (store.Scope, bool, error) {
	f.injectGroup(m, group)
	return store.Scope{Namespace: m.Namespace, ClientID: "fake-group-member"}, true, nil
}

func (f *fakeSink) injectGroup(m *message.Message, group string) {
	f.mu.Lock()
	f.groups = append(f.groups, group+"|"+m.Topic+"|"+string(m.Payload))
	f.mu.Unlock()
}

func (f *fakeSink) directCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.direct)
}

func (f *fakeSink) groupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.groups)
}

type tnode struct {
	name  string
	tr    *transport.Transport
	plane *Plane
	sink  *fakeSink
}

type tcluster struct {
	t     *testing.T
	names []string
	nodes map[string]*tnode
}

// newCluster starts transports and planes for the names; every node knows
// every other, and the harness waits until every pair is up.
func newCluster(t *testing.T, names []string, strategy dispatch.Strategy) *tcluster {
	t.Helper()
	return newClusterMetrics(t, names, strategy, nil)
}

// newClusterMetrics is newCluster with every plane counting into m.
func newClusterMetrics(t *testing.T, names []string, strategy dispatch.Strategy, m hook.Metrics) *tcluster {
	t.Helper()
	return newClusterOpts(t, names, strategy, func(o *Options) { o.Metrics = m })
}

// newClusterOpts is newCluster with a hook on each plane's options.
func newClusterOpts(t *testing.T, names []string, strategy dispatch.Strategy, tweak func(o *Options)) *tcluster {
	t.Helper()
	c := &tcluster{t: t, names: names, nodes: map[string]*tnode{}}
	addrs := map[string]string{}
	for _, name := range names {
		var ps []config.Peer
		for _, o := range names {
			if o != name {
				ps = append(ps, config.Peer{Name: o, Address: "127.0.0.1:1"})
			}
		}
		tr, err := transport.New(transport.Options{Node: name, ClusterID: "t", Listen: "127.0.0.1:0", Peers: ps, Auth: transport.Auth{Mode: "secret", Secret: []byte(secret)},
			Heartbeat: 50 * time.Millisecond, HeartbeatMisses: 4, HandshakeTimeout: 2 * time.Second, Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		sink := &fakeSink{}
		po := Options{Node: name, Transport: tr, Strategy: strategy, SyncInterval: 300 * time.Millisecond, Logger: quiet}
		if tweak != nil {
			tweak(&po)
		}
		plane := New(po)
		plane.SetSink(sink)
		if err := tr.Start(); err != nil {
			t.Fatal(err)
		}
		plane.Start()
		t.Cleanup(func() {
			plane.Stop()
			tr.Stop()
		})
		c.nodes[name] = &tnode{name: name, tr: tr, plane: plane, sink: sink}
		addrs[name] = tr.Addr().String()
	}
	for _, name := range names {
		var ps []config.Peer
		for _, o := range names {
			if o != name {
				ps = append(ps, config.Peer{Name: o, Address: addrs[o]})
			}
		}
		c.nodes[name].tr.SetPeers(ps)
	}
	waitFor(t, "all peers up", 10*time.Second, func() bool {
		for _, n := range c.nodes {
			for _, p := range n.tr.Peers() {
				if p.State != transport.Up {
					return false
				}
			}
		}
		return true
	})
	return c
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// publish builds a message as the session manager would and runs the
// publishing node's forward step; localGroups simulates the dispatcher's
// decider for groups this node has local members of.
func publish(n *tnode, clientID, topic, payload string, seq uint64) *message.Message {
	m := &message.Message{Namespace: "default", Topic: topic, Payload: []byte(payload), QoS: 1, Publisher: message.Publisher{Namespace: "default", ClientID: clientID, Node: n.name}, Seq: seq, Arrived: time.Now()}
	_, _ = n.plane.Forward(m)
	return m
}

func TestForwardReachesRemoteSubscribers(t *testing.T) {
	c := newCluster(t, []string{"a", "b", "cc"}, dispatch.RoundRobin)
	a, b, cc := c.nodes["a"], c.nodes["b"], c.nodes["cc"]
	// b and cc subscribe to t/#; a has a local-only filter nobody else has.
	b.plane.Observe("default", "t/#", 1)
	cc.plane.Observe("default", "t/+", 1)
	a.plane.Observe("default", "local/only", 1)
	waitFor(t, "routes replicated", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 2
	})
	publish(a, "p", "t/1", "hello", 1)
	waitFor(t, "b and cc receive", 5*time.Second, func() bool { return b.sink.directCount() == 1 && cc.sink.directCount() == 1 })
	if a.sink.directCount() != 0 {
		t.Fatal("the publishing node received its own forward")
	}
	publish(a, "p", "local/only", "x", 2)
	time.Sleep(100 * time.Millisecond)
	if b.sink.directCount() != 1 || cc.sink.directCount() != 1 {
		t.Fatal("a local-only topic was forwarded")
	}
	// The message arrives intact.
	b.sink.mu.Lock()
	got := b.sink.direct[0]
	b.sink.mu.Unlock()
	if got.Topic != "t/1" || string(got.Payload) != "hello" || got.Publisher.ClientID != "p" || got.Seq != 1 || !got.Forwarded {
		t.Fatalf("forwarded message %+v", got)
	}
	// Unsubscribe on b: tombstone; only cc receives.
	b.plane.Observe("default", "t/#", 0)
	waitFor(t, "tombstone replicated", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1 && nodes[0] == "cc"
	})
}

// TestGapInForwardsFailsTheBarrier: a forwarded frame the link never
// delivers must not be covered by the cumulative acknowledgement of the
// frame after it. The first publish is dropped on the wire; its barrier
// waits; the second publish arrives with a sequence gap, the receiver
// reports the gap, and the first barrier fails instead of passing on the
// second frame's acknowledgement. (Before the gap check the barrier passed:
// sim seed 5 lost acknowledged messages 8 times in 25 runs.)
func TestGapInForwardsFailsTheBarrier(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/#", 1)
	waitFor(t, "route", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1
	})
	// Drop every forwarded frame a writes to b, then publish once.
	a.tr.SetFault("b", &transport.Fault{Drop: 1, Rand: rand.New(rand.NewSource(1)), Classes: []transport.Class{transport.ForwardedPublish}})
	lost := publish(a, "p", "t/1", "lost", 1)
	time.Sleep(50 * time.Millisecond) // the writer has taken and dropped it
	first := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		first <- a.plane.Barrier(ctx, lost.Forwards)
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-first:
		t.Fatalf("barrier returned %v before anything was acknowledged", err)
	default:
	}
	// Restore the link and publish again: seq 2 arrives, seq 1 never did.
	a.tr.SetFault("b", nil)
	arrives := publish(a, "p", "t/1", "arrives", 2)
	waitFor(t, "second forward delivered", 5*time.Second, func() bool { return b.sink.directCount() == 1 })
	select {
	case err := <-first:
		if !errors.Is(err, ErrRoutesUnavailable) {
			t.Fatalf("barrier over the dropped frame: %v (want ErrRoutesUnavailable)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("barrier over the dropped frame did not return")
	}
	// The second publish's own frame arrived and is acknowledged: it passes.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.plane.Barrier(ctx, arrives.Forwards); err != nil {
		t.Fatalf("barrier over the frame that arrived: %v", err)
	}
}

// TestDataResetFailsBarrierKeepsRoutes: a reset of the data connection
// fails the barrier over frames in flight and re-syncs the routes, but does
// not hide the peer's entries — the peer is still up (WI 1340).
func TestDataResetFailsBarrierKeepsRoutes(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/#", 1)
	waitFor(t, "route", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1
	})
	// A forward whose acknowledgement will never come: the data writer on
	// a is held, the frame is admitted, then the data connection is reset.
	a.tr.SetFault("b", &transport.Fault{Delay: 2 * time.Second, Classes: []transport.Class{transport.ForwardedPublish}})
	inFlight := publish(a, "p", "t/1", "in flight", 1)
	barrier := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		barrier <- a.plane.Barrier(ctx, inFlight.Forwards)
	}()
	time.Sleep(100 * time.Millisecond)
	a.tr.Reset("b", transport.RoleData, "reset by the test")
	select {
	case err := <-barrier:
		if !errors.Is(err, ErrRoutesUnavailable) {
			t.Fatalf("barrier over a reset data connection: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the barrier did not fail on the data reset")
	}
	a.tr.SetFault("b", nil)
	// The peer is still up and its routes still match.
	if nodes, _ := a.plane.Table().Match("default", "t/1"); len(nodes) != 1 {
		t.Fatalf("routes hidden on a data reset: %v", nodes)
	}
	if ps := a.tr.Peers(); ps[0].State != transport.Up {
		t.Fatalf("peer %s after a data reset", ps[0].State)
	}
	// The frame lost with the connection stays failed for the publish that
	// sent it, however late its barrier is asked.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.plane.Barrier(ctx, inFlight.Forwards); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a second barrier over the lost frame: %v (want ErrRoutesUnavailable)", err)
	}
	// A publish that forwards nothing is not judged on it. Before the barrier
	// took a publish's own frames this failed for as long as nothing else was
	// forwarded to b: the mark is a high-water sequence nothing lowers.
	if err := a.plane.Barrier(ctx, nil); err != nil {
		t.Fatalf("a publish that forwarded nothing was refused for an earlier publish's lost frame: %v", err)
	}
	// Once the data connection is back, forwards flow again, and the next
	// publish is judged on its own frame.
	waitFor(t, "data connection back", 5*time.Second, func() bool { return a.tr.Peers()[0].DataState == transport.Up })
	after := publish(a, "p", "t/1", "after", 2)
	waitFor(t, "forward after the reset", 5*time.Second, func() bool { return b.sink.directCount() >= 1 })
	if err := a.plane.Barrier(ctx, after.Forwards); err != nil {
		t.Fatalf("the publish after the reset: %v", err)
	}
}

// TestForwardsAreNamed: every frame sent for a locally published QoS 1 message
// is named on it with the sequence its stream took, direct or through a
// shared group, and nothing is named for QoS 0 or for a forwarded message.
func TestForwardsAreNamed(t *testing.T) {
	c := newCluster(t, []string{"a", "b", "cc"}, dispatch.RoundRobin)
	a, b, cc := c.nodes["a"], c.nodes["b"], c.nodes["cc"]
	b.plane.Observe("default", "t/#", 1)
	cc.plane.Observe("default", "t/+", 1)
	b.plane.Observe("default", "$share/g/s/#", 1)
	waitFor(t, "routes replicated", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		_, groups := a.plane.Table().Match("default", "s/1")
		return len(nodes) == 2 && len(groups) == 1
	})
	first := publish(a, "p", "t/1", "x", 1)
	got := map[message.ForwardRef]bool{}
	for _, f := range first.Forwards {
		got[f] = true
	}
	want := map[message.ForwardRef]bool{{Peer: "b", Seq: 1}: true, {Peer: "cc", Seq: 1}: true}
	if !reflect.DeepEqual(got, want) || len(first.Forwards) != 2 {
		t.Fatalf("direct forwards named %v, want %v", first.Forwards, want)
	}
	// A group with no local member: the frame goes to the group's owner, b,
	// and takes b's next sequence.
	group := publish(a, "p", "s/1", "y", 2)
	if want := []message.ForwardRef{{Peer: "b", Seq: 2}}; !reflect.DeepEqual(group.Forwards, want) {
		t.Fatalf("group forward named %v, want %v", group.Forwards, want)
	}
	q0 := &message.Message{Namespace: "default", Topic: "t/1", Payload: []byte("z"), Publisher: message.Publisher{ClientID: "p"}, Seq: 3}
	if _, _ = a.plane.Forward(q0); len(q0.Forwards) != 0 {
		t.Fatalf("a QoS 0 message was named forwards %v", q0.Forwards)
	}
	waitFor(t, "forwards delivered", 5*time.Second, func() bool { return b.sink.directCount() == 2 && cc.sink.directCount() == 2 })
	b.sink.mu.Lock()
	defer b.sink.mu.Unlock()
	for _, m := range b.sink.direct {
		if len(m.Forwards) != 0 {
			t.Fatalf("a forwarded message arrived naming forwards %v", m.Forwards)
		}
	}
}

// TestBarrierJudgesOnlyTheFramesGiven drives the decision over a stream's
// state directly. The three writers of the failure mark — a failed send, a
// receiver's failed acknowledgement, a link closing with frames in flight —
// all leave this shape, and two of them advance acked with it.
func TestBarrierJudgesOnlyTheFramesGiven(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a := c.nodes["a"]
	a.plane.mu.Lock()
	a.plane.out["b"] = &outbound{sent: 6, acked: 6, lastFailed: 5}
	a.plane.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cases := []struct {
		name     string
		forwards []message.ForwardRef
		fails    bool
	}{
		{"nothing forwarded", nil, false},
		{"the failed frame, though acknowledged past", []message.ForwardRef{{Peer: "b", Seq: 5}}, true},
		{"a frame below the failed one", []message.ForwardRef{{Peer: "b", Seq: 3}}, true},
		{"a later frame, acknowledged", []message.ForwardRef{{Peer: "b", Seq: 6}}, false},
		{"a later frame beside the failed one", []message.ForwardRef{{Peer: "b", Seq: 6}, {Peer: "b", Seq: 5}}, true},
		{"a peer with no stream", []message.ForwardRef{{Peer: "nowhere", Seq: 1}}, true},
	}
	for _, tc := range cases {
		err := a.plane.Barrier(ctx, tc.forwards)
		if tc.fails && !errors.Is(err, ErrRoutesUnavailable) {
			t.Errorf("%s: %v, want ErrRoutesUnavailable", tc.name, err)
		}
		if !tc.fails && err != nil {
			t.Errorf("%s: %v, want it to pass", tc.name, err)
		}
	}
}

// orderCheck verifies per-publisher monotone sequences on a sink.
func orderCheck(msgs []*message.Message) error {
	last := map[string]uint64{}
	for _, m := range msgs {
		if prev, ok := last[m.Publisher.ClientID]; ok && m.Seq != prev+1 {
			return fmt.Errorf("publisher %s: %d after %d", m.Publisher.ClientID, m.Seq, prev)
		}
		last[m.Publisher.ClientID] = m.Seq
	}
	return nil
}

func TestOrderingUnderLoad(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "t/#", 1)
	waitFor(t, "route", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "t/1")
		return len(nodes) == 1
	})
	const each = 1000
	var wg sync.WaitGroup
	for _, pub := range []string{"p1", "p2"} {
		wg.Add(1)
		go func(pub string) {
			defer wg.Done()
			for i := 1; i <= each; i++ {
				publish(a, pub, "t/"+pub, strconv.Itoa(i), uint64(i))
			}
		}(pub)
	}
	wg.Wait()
	waitFor(t, "all forwards", 10*time.Second, func() bool { return b.sink.directCount() == 2*each })
	b.sink.mu.Lock()
	msgs := append([]*message.Message(nil), b.sink.direct...)
	b.sink.mu.Unlock()
	if err := orderCheck(msgs); err != nil {
		t.Fatal(err)
	}
	// Negative control: the same check on a reordered copy fails.
	swapped := append([]*message.Message(nil), msgs...)
	for i := 0; i+1 < len(swapped); i += 2 {
		if swapped[i].Publisher.ClientID == swapped[i+1].Publisher.ClientID {
			swapped[i], swapped[i+1] = swapped[i+1], swapped[i]
		}
	}
	if err := orderCheck(swapped); err == nil {
		t.Fatal("the ordering check did not detect a reordering")
	}
}

func TestTablesConvergeAfterPartition(t *testing.T) {
	c := newCluster(t, []string{"a", "b"}, dispatch.RoundRobin)
	a, b := c.nodes["a"], c.nodes["b"]
	b.plane.Observe("default", "old/#", 1)
	waitFor(t, "initial route", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "old/1")
		return len(nodes) == 1
	})
	a.tr.Isolate("b", true)
	b.tr.Isolate("a", true)
	// During the partition b subscribes one filter and drops another.
	b.plane.Observe("default", "new/#", 1)
	b.plane.Observe("default", "old/#", 0)
	time.Sleep(200 * time.Millisecond)
	if nodes, _ := a.plane.Table().Match("default", "new/1"); len(nodes) != 0 {
		t.Fatal("a learned a route across the partition")
	}
	// a hides b's entries while b is down.
	waitFor(t, "b hidden on a", 5*time.Second, func() bool {
		nodes, _ := a.plane.Table().Match("default", "old/1")
		return len(nodes) == 0
	})
	a.tr.Isolate("b", false)
	b.tr.Isolate("a", false)
	waitFor(t, "convergence after heal", 10*time.Second, func() bool {
		newNodes, _ := a.plane.Table().Match("default", "new/1")
		oldNodes, _ := a.plane.Table().Match("default", "old/1")
		return len(newNodes) == 1 && len(oldNodes) == 0
	})
	e, ok := a.plane.Table().Lookup("b", "default", "old/#")
	if !ok || !e.Tombstone {
		t.Fatalf("tombstone not honoured: %+v %v", e, ok)
	}
}

func TestSharedGroupAcrossNodes(t *testing.T) {
	c := newCluster(t, []string{"a", "b", "cc"}, dispatch.RoundRobin)
	a, b, cc := c.nodes["a"], c.nodes["b"], c.nodes["cc"]
	// One member on each node; a has two so it owns the dispatch.
	a.plane.Observe("default", "$share/g/t/#", 2)
	b.plane.Observe("default", "$share/g/t/#", 1)
	cc.plane.Observe("default", "$share/g/t/#", 1)
	waitFor(t, "group routes", 5*time.Second, func() bool {
		_, groups := b.plane.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 3
	})
	// Publishes from b (a non-owner) and from a (the owner): each delivered
	// exactly once across the cluster; the round-robin spreads over nodes.
	const total = 300
	localA := 0
	for i := 0; i < total; i++ {
		msg := &message.Message{Namespace: "default", Topic: "t/1", Payload: []byte(strconv.Itoa(i)), Publisher: message.Publisher{ClientID: "p"}, Seq: uint64(i)}
		if i%2 == 0 {
			// Published on a, which has local members: the dispatcher asks
			// the decider; a true answer means a's own member delivers.
			if a.plane.Decide("default", "g/t/#", msg) {
				localA++
			}
		} else {
			// Published on b, which has a local member too.
			if b.plane.Decide("default", "g/t/#", msg) {
				t.Fatal("a non-owner delivered locally without the owner's decision")
			}
		}
	}
	waitFor(t, "every publish delivered once", 10*time.Second, func() bool {
		return localA+a.sink.groupCount()+b.sink.groupCount()+cc.sink.groupCount() == total
	})
	share := []int{localA + a.sink.groupCount(), b.sink.groupCount(), cc.sink.groupCount()}
	for i, n := range share {
		if n < total/3-total/10 || n > total/3+total/10 {
			t.Fatalf("round-robin share uneven: %v (node %d)", share, i)
		}
	}
	// cc's node leaves: its entries are hidden; new publishes go to a or b.
	cc.tr.Stop()
	waitFor(t, "cc down on a", 5*time.Second, func() bool {
		_, groups := a.plane.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 2
	})
	before := cc.sink.groupCount()
	base := localA + a.sink.groupCount() + b.sink.groupCount()
	for i := 0; i < 60; i++ {
		msg := &message.Message{Namespace: "default", Topic: "t/1", Payload: []byte("late"), Publisher: message.Publisher{ClientID: "q"}, Seq: uint64(i)}
		if a.plane.Decide("default", "g/t/#", msg) {
			localA++
		}
	}
	waitFor(t, "late publishes delivered", 5*time.Second, func() bool {
		return localA+a.sink.groupCount()+b.sink.groupCount() == base+60
	})
	if cc.sink.groupCount() != before {
		t.Fatal("a departed node received group deliveries")
	}
	_ = strings.Join
}

// TestMovingHoldsTheRouteRemoval: a session leaving a node keeps its route
// visible to the other nodes for the move grace, so a publish in the window
// is still forwarded to the previous owner (which answers that it reached no
// session) instead of matching nothing anywhere. An ordinary removal is told
// to the peers at once, and a re-subscription during the grace cancels the
// held removal.
func TestMovingHoldsTheRouteRemoval(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2"}, dispatch.RoundRobin)
	n1, n2 := c.nodes["n1"], c.nodes["n2"]
	known := func(owner, filter string) bool {
		nodes, _ := n2.plane.Table().Match("default", filter)
		for _, n := range nodes {
			if n == owner {
				return true
			}
		}
		return false
	}

	// An ordinary removal reaches the peer at once.
	n1.plane.Observe("default", "plain/t", 1)
	waitFor(t, "n2 learns plain/t", 5*time.Second, func() bool { return known("n1", "plain/t") })
	n1.plane.Observe("default", "plain/t", 0)
	waitFor(t, "n2 forgets plain/t", 5*time.Second, func() bool { return !known("n1", "plain/t") })

	// A moving session's removal is held.
	n1.plane.o.MoveGrace = 2 * time.Second
	n1.plane.Observe("default", "move/t", 1)
	waitFor(t, "n2 learns move/t", 5*time.Second, func() bool { return known("n1", "move/t") })
	n1.plane.Moving("default", []string{"move/t"})
	n1.plane.Observe("default", "move/t", 0)
	time.Sleep(500 * time.Millisecond)
	if !known("n1", "move/t") {
		t.Fatal("the moving session's route was withdrawn inside the grace")
	}
	waitFor(t, "n2 forgets move/t after the grace", 5*time.Second, func() bool { return !known("n1", "move/t") })

	// A re-subscription during the grace cancels the held removal.
	n1.plane.Observe("default", "back/t", 1)
	waitFor(t, "n2 learns back/t", 5*time.Second, func() bool { return known("n1", "back/t") })
	n1.plane.Moving("default", []string{"back/t"})
	n1.plane.Observe("default", "back/t", 0)
	n1.plane.Observe("default", "back/t", 1)
	time.Sleep(2500 * time.Millisecond)
	if !known("n1", "back/t") {
		t.Fatal("the held removal fired after the filter came back")
	}
}
