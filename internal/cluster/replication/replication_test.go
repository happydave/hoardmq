package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/topic"
)

const secret = "replication-test-shared-secret-of-enough-bytes"

// probeScope is what the direct outbound.wait tests name. These exercise the
// stream's own states — down, silent, broken, stopped — and a wait must name
// something or it fails as unnamed before reaching them.
var probeScope = []store.Scope{{Namespace: "default", ClientID: "probe"}}

var quiet = harnessLogger()

// harnessLogger discards the nodes' logs unless HOARDMQ_TEST_LOG is set, when
// they go to standard error at debug level, for diagnosing a failing test.
func harnessLogger() *slog.Logger {
	if os.Getenv("HOARDMQ_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeCore is a consensus core whose proposals apply to every node's table.
type fakeCore struct {
	h    *harness
	node string
	role string
}

func (c *fakeCore) Propose(_ context.Context, kind consensus.Kind, payload []byte) (any, error) {
	if kind != shardmap.Kind {
		return nil, consensus.ErrUnknownKind
	}
	var out any
	for _, n := range c.h.order {
		res, err := c.h.nodes[n].table.Apply(0, payload)
		if err != nil {
			return nil, err
		}
		out = res
	}
	return out, nil
}
func (c *fakeCore) AddMember(context.Context, string, string) error { return nil }
func (c *fakeCore) PromoteMember(context.Context, string) error     { return nil }
func (c *fakeCore) RemoveMember(context.Context, string) error      { return nil }
func (c *fakeCore) Leader() (string, bool)                          { return c.h.order[0], true }
func (c *fakeCore) Members() []consensus.Member {
	var out []consensus.Member
	for _, n := range c.h.order {
		out = append(out, consensus.Member{Name: n, Role: consensus.Voter})
	}
	return out
}
func (c *fakeCore) Status() consensus.Status { return consensus.Status{Role: c.role} }
func (c *fakeCore) Ready() bool              { return true }
func (c *fakeCore) Recovering() bool         { return false }
func (c *fakeCore) Stop()                    {}

// fakeLeases is one node's view of the lease table.
type fakeLeases struct {
	onLookup  func(ns, id string)
	onLeaving func(node string)
	mu        sync.Mutex
	leases    map[string]lease.Lease
	nodes     map[string]lease.NodeInfo
}

func (l *fakeLeases) Lookup(ns, id string) (lease.Lease, bool) {
	if l.onLookup != nil {
		l.onLookup(ns, id)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	x, ok := l.leases[ns+"/"+id]
	return x, ok
}
func (l *fakeLeases) Node(name string) (lease.NodeInfo, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	x, ok := l.nodes[name]
	return x, ok
}
func (l *fakeLeases) Live(name string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	x, ok := l.nodes[name]
	return ok && x.DeadAt == 0
}
func (l *fakeLeases) Leaving(name string) bool {
	if l.onLeaving != nil {
		l.onLeaving(name)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nodes[name].LeavingAt != 0
}
func (l *fakeLeases) set(ns, id, owner string, epoch uint64) {
	l.setState(ns, id, owner, epoch, true)
}

func (l *fakeLeases) setState(ns, id, owner string, epoch uint64, online bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leases[ns+"/"+id] = lease.Lease{Namespace: ns, ClientID: id, Owner: owner, Epoch: epoch, Online: online}
}

// fakeSessions is the owner's session manager and retained store: retained
// messages installed through it are journaled to the node's own store so
// the tap sees them, as the real manager does.
type fakeSessions struct {
	mu       sync.Mutex
	own      *store.Store
	sessions []*store.SessionState
	retained map[string]*message.Message
	// unjournaled makes InjectRetained report its record not durable, as a
	// session layer whose journal append or sync failed does.
	unjournaled atomic.Bool
}

func (s *fakeSessions) Snapshot() *store.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := store.NewState()
	st.Sessions = append(st.Sessions, s.sessions...)
	st.Retained = append(st.Retained, s.all()...)
	return st
}
func (s *fakeSessions) Export(ns, id string) (*store.SessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ss := range s.sessions {
		if ss.Namespace == ns && ss.ClientID == id {
			return ss, true
		}
	}
	return nil, false
}
func (s *fakeSessions) InjectRetained(m *message.Message) bool {
	// The store is set, then journaled with the lock released, as the
	// manager's router is set before its journal record is tapped: the tap
	// waits on a whole push that reads this store.
	s.mu.Lock()
	if len(m.Payload) == 0 {
		delete(s.retained, m.Namespace+"/"+m.Topic)
	} else {
		s.retained[m.Namespace+"/"+m.Topic] = m
	}
	s.mu.Unlock()
	var err error
	if len(m.Payload) == 0 {
		err = s.own.RetainDelete(m.Namespace, m.Topic)
	} else {
		err = s.own.Retain(m)
	}
	return errors.Join(err, s.own.Sync()) == nil && !s.unjournaled.Load()
}
func (s *fakeSessions) all() []*message.Message {
	var out []*message.Message
	for _, m := range s.retained {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}
func (s *fakeSessions) RetainedFor(ns, filter string, _ time.Time) []*message.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*message.Message
	for _, m := range s.all() {
		if m.Namespace == ns && topic.Match(filter, m.Topic) {
			out = append(out, m)
		}
	}
	return out
}
func (s *fakeSessions) AllRetained() []*message.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.all()
}
func (s *fakeSessions) setSessions(list ...*store.SessionState) {
	s.mu.Lock()
	s.sessions = list
	s.mu.Unlock()
}
func (s *fakeSessions) has(ns, t string) (*message.Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.retained[ns+"/"+t]
	return m, ok
}

type tnode struct {
	name     string
	tr       *transport.Transport
	table    *shardmap.Table
	core     *fakeCore
	leases   *fakeLeases
	sessions *fakeSessions
	own      *store.Store
	replica  *store.Store
	rep      *Replicator

	// written records every scope this node's store has journaled, so a test
	// barrier can name exactly what was written rather than guessing.
	wmu     sync.Mutex
	written map[store.Scope]struct{}
}

func (n *tnode) noteWritten(s store.Scope) {
	if s.Zero() {
		return
	}
	n.wmu.Lock()
	if n.written == nil {
		n.written = map[store.Scope]struct{}{}
	}
	n.written[s] = struct{}{}
	n.wmu.Unlock()
}

// barrier names every session and retained topic this node holds, plus any
// extra scopes, and waits. It is the faithful form for a test whose intent is
// "everything written so far is durable": the barrier now has to say what it
// is claiming durability for, and what this node holds is exactly that.
func (n *tnode) barrier(ctx context.Context, extra ...store.Scope) error {
	keys := append([]store.Scope{}, extra...)
	n.wmu.Lock()
	for k := range n.written {
		keys = append(keys, k)
	}
	n.wmu.Unlock()
	if len(keys) == 0 {
		// This node wrote nothing of its own — the test is exercising stream
		// mechanics (presence, unknown frames, a forwarded retained write)
		// rather than what a barrier covers. A wait must still name something
		// or it fails as unnamed before reaching the behaviour under test.
		keys = append(keys, probeScope...)
	}
	return n.rep.Barrier(ctx, keys)
}

type harness struct {
	t     *testing.T
	order []string
	nodes map[string]*tnode
}

func newHarness(t *testing.T, names []string, shards, replicas int) *harness {
	t.Helper()
	return newHarnessWith(t, names, shards, replicas, nil)
}

// newHarnessWith is newHarness with a hook over every node's options.
func newHarnessWith(t *testing.T, names []string, shards, replicas int, mutate func(*Options)) *harness {
	t.Helper()
	h := &harness{t: t, order: names, nodes: map[string]*tnode{}}
	addrs := map[string]string{}
	for _, name := range names {
		var ps []config.Peer
		for _, o := range names {
			if o != name {
				ps = append(ps, config.Peer{Name: o, Address: "127.0.0.1:1"})
			}
		}
		tr, err := transport.New(transport.Options{Node: name, ClusterID: "t", Listen: "127.0.0.1:0", Peers: ps, Auth: transport.Auth{Mode: "secret", Secret: []byte(secret)},
			Heartbeat: 50 * time.Millisecond, HeartbeatMisses: 4, HandshakeTimeout: 2 * time.Second, Logger: quiet.With("node", name)})
		if err != nil {
			t.Fatal(err)
		}
		own, err := store.Open(t.TempDir(), store.Options{Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		replica, err := store.Open(t.TempDir(), store.Options{Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		n := &tnode{name: name, tr: tr, table: shardmap.New(shards), own: own, replica: replica,
			leases:   &fakeLeases{leases: map[string]lease.Lease{}, nodes: map[string]lease.NodeInfo{}},
			sessions: &fakeSessions{own: own, retained: map[string]*message.Message{}}}
		n.core = &fakeCore{h: h, node: name, role: "follower"}
		opts := Options{Node: name, Transport: tr, Map: n.table, Core: n.core, Leases: n.leases, Sessions: n.sessions, Local: n.sessions,
			Replica: replica, Replicas: replicas, AckTimeout: 2 * time.Second, TTL: 2 * time.Second, Interval: 100 * time.Millisecond, Logger: quiet.With("node", name)}
		opts.ConfirmMoved = func(ns, id string, to []string) { h.confirmMoved(name, ns, id, to) }
		if mutate != nil {
			mutate(&opts)
		}
		rep, err := New(opts, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.rep = rep
		own.SetTap(func(rec store.Record) func() {
			n.noteWritten(rec.Key.Scope())
			return rep.Tap(rec)
		})
		if err := tr.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			rep.Stop()
			tr.Stop()
			_ = own.Close()
			_ = replica.Close()
		})
		h.nodes[name] = n
		addrs[name] = tr.Addr().String()
	}
	for _, name := range names {
		var ps []config.Peer
		for _, o := range names {
			if o != name {
				ps = append(ps, config.Peer{Name: o, Address: addrs[o]})
			}
		}
		h.nodes[name].tr.SetPeers(ps)
	}
	h.waitFor("all peers up", 10*time.Second, func() bool {
		for _, n := range h.nodes {
			for _, p := range n.tr.Peers() {
				if p.State != transport.Up {
					return false
				}
			}
		}
		return true
	})
	return h
}

// confirmMoved is the lease service's move confirmation, in the harness: once
// the owner's barrier over the session passes, each node it stopped
// replicating the session to is told the owner holds it at the epoch its
// session manager holds it at.
func (h *harness) confirmMoved(owner, ns, id string, to []string) {
	go func() {
		n := h.nodes[owner]
		if n.rep.Barrier(context.Background(), []store.Scope{{Namespace: ns, ClientID: id}}) != nil {
			return
		}
		ss, ok := n.sessions.Export(ns, id)
		if !ok {
			return
		}
		for _, t := range to {
			if tn := h.nodes[t]; tn != nil {
				tn.rep.NoteWritten(ns, id, owner, ss.Epoch)
			}
		}
	}()
}

func (h *harness) waitFor(what string, d time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

// applyMap installs the same map on every node.
func (h *harness) applyMap(m shardmap.Map) {
	for _, n := range h.order {
		if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(m)); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *harness) setLease(ns, id, owner string, epoch uint64) {
	for _, n := range h.nodes {
		n.leases.set(ns, id, owner, epoch)
	}
}

// topicIn finds a topic name whose shard is the given one.
func topicIn(shards, want int) string {
	for i := 0; ; i++ {
		t := fmt.Sprintf("t/%d", i)
		if shardmap.ShardOf("default", t, shards) == want {
			return t
		}
	}
}

// clientIn finds a client identifier whose shard is the given one; prefix
// distinguishes identifiers within one test.
func clientIn(shards, want int, prefix ...string) string {
	pre := "c"
	if len(prefix) > 0 {
		pre = prefix[0]
	}
	for i := 0; ; i++ {
		c := fmt.Sprintf("%s%d", pre, i)
		if shardmap.ShardOf("default", c, shards) == want {
			return c
		}
	}
}

// clientTargeting finds a client identifier whose session, owned by owner, has
// peer among its replica targets under m; prefix distinguishes identifiers
// within one test.
func clientTargeting(m shardmap.Map, shards int, owner, peer, prefix string) string {
	for i := 0; ; i++ {
		c := fmt.Sprintf("%s%d", prefix, i)
		if slices.Contains(m.Targets(shardmap.ShardOf("default", c, shards), owner), peer) {
			return c
		}
	}
}

// clientAvoiding finds a client identifier whose session, owned by owner, does
// not have peer among its replica targets under m, and has at least one.
func clientAvoiding(m shardmap.Map, shards int, owner, peer, prefix string) string {
	for i := 0; ; i++ {
		c := fmt.Sprintf("%s%d", prefix, i)
		targets := m.Targets(shardmap.ShardOf("default", c, shards), owner)
		if len(targets) > 0 && !slices.Contains(targets, peer) {
			return c
		}
	}
}

func (h *harness) writeSession(owner, id string, epoch uint64) {
	n := h.nodes[owner]
	h.setLease("default", id, owner, epoch)
	if err := n.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: epoch, Version: 5, ExpiryInterval: 300}); err != nil {
		h.t.Fatal(err)
	}
	if err := n.own.Subscribe(store.Key{Namespace: "default", ClientID: id, Epoch: epoch}, store.SubscriptionRecord{Filter: "a/b", QoS: 1}); err != nil {
		h.t.Fatal(err)
	}
	if err := n.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: epoch}, store.Entry{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("queued")}, QoS: 1}); err != nil {
		h.t.Fatal(err)
	}
	if err := n.own.Sync(); err != nil {
		h.t.Fatal(err)
	}
}

func TestRecordsReachFollowersAndBarrierWaits(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 4, 2)
	m := shardmap.Compute(1, h.order, 2, 4)
	h.applyMap(m)
	// A session owned by n1 in shard 0 (leader n1, follower n2): its replica
	// target is n2 only.
	id := clientIn(4, 0)
	h.writeSession("n1", id, 1)
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	rep := h.nodes["n2"].rep.ReplicaState()
	ss := rep.Find("default", id)
	if ss == nil || len(ss.Subscriptions) != 1 || len(ss.Entries) != 1 || string(ss.Entries[0].Message.Payload) != "queued" {
		t.Fatalf("n2 replica: %+v", ss)
	}
	if h.nodes["n3"].rep.ReplicaState().Find("default", id) != nil {
		t.Fatal("n3 is not a target for shard 0 yet holds a replica")
	}
	// The replica is durable: a fresh store over the same directory recovers it.
	// (The replica store stays open in the harness; recovery is exercised by
	// the store package's own tests. Here the acknowledged state is what the
	// barrier promised.)
	// A session owned by n3 in shard 0 replicates to the shard's first holder
	// that is not the owner: n1.
	id3 := clientIn(4, 0, "x")
	h.writeSession("n3", id3, 1)
	if err := h.nodes["n3"].barrier(context.Background()); err != nil {
		t.Fatalf("barrier n3: %v", err)
	}
	if h.nodes["n1"].rep.ReplicaState().Find("default", id3) == nil {
		t.Fatal("n1 should hold the replica for n3's session in shard 0")
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id3) != nil {
		t.Fatal("n2 is beyond the copy count for n3's session")
	}
}

func TestStaleEpochIsRefused(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	id := clientIn(2, 0) // leader n1, follower n2
	// n2's table already knows the lease moved on to n2 at epoch 2; n1 still
	// believes it owns epoch 1 and writes.
	h.nodes["n1"].leases.set("default", id, "n1", 1)
	h.nodes["n2"].leases.set("default", id, "n2", 2)
	n1 := h.nodes["n1"]
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	// The follower refuses the record, so the copy is not held and the
	// barrier covering it fails (1273's owner question 3).
	//
	// Each call names the session it is about. The barrier scopes a refusal to
	// what it belongs to (WI 1510), so a wait that named both sessions would
	// fail for the first session's refusal and could never show that the
	// second is accepted. Both assertions are the ones this test has always
	// made.
	if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: id}}); !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("barrier over a rejected record should fail with ErrReplicaUnavailable: %v", err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) != nil {
		t.Fatal("stale-epoch record was applied")
	}
	// The same epoch from a node the table names as owner is accepted even
	// when the follower's table lags (unknown lease).
	other := clientIn(2, 0, "y")
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: other, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: other}}); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", other) == nil {
		t.Fatal("record for an unknown lease was not applied")
	}
}

func TestBarrierFailsOnDownPeerAndOnSilence(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	n1 := h.nodes["n1"]
	n1.tr.Isolate("n2", true)
	h.waitFor("n2 down on n1", 5*time.Second, func() bool {
		for _, p := range n1.tr.Peers() {
			if p.Name == "n2" && p.State != transport.Up {
				return true
			}
		}
		return false
	})
	h.writeSession("n1", clientIn(2, 0), 1)
	start := time.Now()
	if err := n1.barrier(context.Background()); !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("barrier with a down follower should fail with ErrReplicaUnavailable: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("barrier waited on a down follower instead of failing")
	}
	// A silent but connected follower fails the barrier at the timeout.
	p := &outbound{name: "ghost"}
	p.cond = sync.NewCond(&p.mu)
	p.sent = 3
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := p.wait(ctx, func(string) bool { return true }, probeScope); err == nil {
		t.Fatal("silent follower did not fail the barrier")
	}
	// Once acknowledged, the barrier passes without waiting.
	p.acked(3)
	if err := p.wait(context.Background(), func(string) bool { return true }, probeScope); err != nil {
		t.Fatalf("acked wait: %v", err)
	}
}

func TestRetainedThroughLeaderAndScatterGather(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3)) // shard i: leader n(i+1), follower n(i+2 mod 3)
	waitSettled(h, 1)
	// Published on n1, led by n2, followed by n3.
	t1 := topicIn(3, 1)
	m1 := &message.Message{Namespace: "default", Topic: t1, Payload: []byte("one"), Retain: true}
	if h.nodes["n1"].rep.StoreRetained(m1) {
		t.Fatal("non-leader stored locally")
	}
	// The session manager names the retained topic whether it stored the write
	// or forwarded it to the leader; the leader is what that name waits on.
	if err := h.nodes["n1"].barrier(context.Background(), store.Scope{Namespace: "default", Topic: t1}); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if got, ok := h.nodes["n2"].sessions.has("default", t1); !ok || string(got.Payload) != "one" {
		t.Fatal("leader n2 does not hold the retained message")
	}
	h.waitFor("follower n3 replica", 5*time.Second, func() bool {
		for _, m := range h.nodes["n3"].rep.ReplicaState().Retained {
			if m.Topic == t1 {
				return true
			}
		}
		return false
	})
	// Published on n3 and led by n3: stored locally, replicated to n1.
	t2 := topicIn(3, 2)
	m2 := &message.Message{Namespace: "default", Topic: t2, Payload: []byte("two"), Retain: true}
	if !h.nodes["n3"].rep.StoreRetained(m2) {
		t.Fatal("leader did not store locally")
	}
	h.nodes["n3"].sessions.InjectRetained(m2)
	if err := h.nodes["n3"].barrier(context.Background()); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	// A wildcard query from n1 (which leads neither) gathers both.
	got := h.nodes["n1"].rep.QueryRetained("default", "t/#", time.Now())
	topics := map[string]string{}
	for _, m := range got {
		topics[m.Topic] = string(m.Payload)
	}
	if len(topics) != 2 || topics[t1] != "one" || topics[t2] != "two" {
		t.Fatalf("scatter-gather: %v", topics)
	}
	// An exact query asks only the topic's leader.
	if got := h.nodes["n1"].rep.QueryRetained("default", t2, time.Now()); len(got) != 1 || string(got[0].Payload) != "two" {
		t.Fatalf("exact query: %v", got)
	}
	// n1's own (misplaced) local copy of a topic it does not lead is not served.
	h.nodes["n1"].sessions.mu.Lock()
	h.nodes["n1"].sessions.retained["default/"+t1] = &message.Message{Namespace: "default", Topic: t1, Payload: []byte("stale")}
	h.nodes["n1"].sessions.mu.Unlock()
	got = h.nodes["n1"].rep.QueryRetained("default", t1, time.Now())
	if len(got) != 1 || string(got[0].Payload) != "one" {
		t.Fatalf("stale local copy served: %v", got)
	}
}

func TestFetchFromAHolder(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3))
	id := clientIn(3, 0) // leader n1, follower n2
	h.writeSession("n1", id, 1)
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	// n3 holds nothing for shard 0 and asks n1's replicas.
	ss, _, _ := h.nodes["n3"].rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || len(ss.Entries) != 1 {
		t.Fatalf("fetch from holder: %+v", ss)
	}
	// n2 answers from its own replica.
	if ss, _, _ := h.nodes["n2"].rep.Fetch(context.Background(), "default", id, "n1"); ss == nil {
		t.Fatal("local replica fetch")
	}
	if ss, _, _ := h.nodes["n3"].rep.Fetch(context.Background(), "default", "nobody", "n1"); ss != nil {
		t.Fatal("unknown session fetched")
	}
}

// TestFetchSkipsAHolderThatIsNotAReplica: a session's owner is decided by
// its lease, not by the shard map, so an owner outside its shard's holders
// replicates to only replicas-1 of them and the remaining holder receives
// nothing. That holder's copy is whatever an earlier map left it, no
// barrier ever covered it, and a takeover that adopted it would hand the
// client a queue missing everything since.
func TestFetchSkipsAHolderThatIsNotAReplica(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 3, 2)
	m := shardmap.Compute(1, h.order, 2, 3)
	h.applyMap(m)
	id := clientIn(3, 0)
	shard := h.nodes["n1"].rep.o.Map.ShardOf("default", id)
	holders := m.Holders(shard)
	owner := ""
	for _, n := range []string{"n1", "n2", "n3"} {
		if !contains(holders, n) {
			owner = n
		}
	}
	if owner == "" {
		// Three nodes and two holders: one node is always outside. A skip
		// here would report a pass for a hand-over this test never drove.
		t.Fatalf("every node holds shard %d (holders %v), so there is no owner outside it to hand over from", shard, holders)
	}
	targets := m.Targets(shard, owner)
	if len(targets) != 1 || len(holders) != 2 {
		t.Fatalf("want one target of two holders, got %v of %v", targets, holders)
	}
	spare := holders[0]
	if spare == targets[0] {
		spare = holders[1]
	}
	h.writeSession(owner, id, 2)
	if err := h.nodes[owner].own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 2}, store.Entry{Seq: 2, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("later")}, QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := h.nodes[owner].own.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := h.nodes[owner].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The spare holder carries a copy an earlier map left behind: one
	// entry, missing everything the owner has replicated since.
	stale := h.nodes[spare].rep.follower
	stale.mu.Lock()
	stale.state.Sessions = append(stale.state.Sessions, &store.SessionState{
		SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1, Version: 5, ExpiryInterval: 300},
		Entries:     []store.Entry{{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("stale")}, QoS: 1}},
	})
	stale.mu.Unlock()
	ss, _, _ := h.nodes[spare].rep.Fetch(context.Background(), "default", id, owner)
	if ss == nil {
		t.Fatal("no replica fetched")
	}
	if len(ss.Entries) != 2 {
		t.Fatalf("adopted the stale copy of a non-replica holder: %d entries, want the 2 on %s's replica %s", len(ss.Entries), owner, targets[0])
	}
}

func TestRebalanceOnMapChange(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 2, 2)
	// Only n1 and n2 live at first: shard 0 = n1 + n2, shard 1 = n2 + n1.
	allAlive(h)
	h.applyMap(shardmap.Compute(1, []string{"n1", "n2"}, 2, 2))
	waitSettled(h, 1)
	id := clientIn(2, 1) // owned by n1, target n2 (shard 1's leader)
	h.writeSession("n1", id, 1)
	h.nodes["n1"].sessions.setSessions(&store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}, Subscriptions: []store.SubscriptionRecord{{Filter: "a/b", QoS: 1}}})
	rt := topicIn(2, 1) // led by n2, followed by n1
	msg := &message.Message{Namespace: "default", Topic: rt, Payload: []byte("kept"), Retain: true}
	h.nodes["n2"].sessions.InjectRetained(msg)
	if err := h.nodes["n1"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.nodes["n2"].barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("n2 should hold the replica before the change")
	}
	// n3 joins: shard 0 = n1 + n2, shard 1 = n2 + n3. n1's session in shard 1
	// now targets n2 still (first holder that is not the owner) — so make the
	// change one where a target moves: replicas 2 over three nodes with the
	// owner leading shard 0 → target n2; after the change shard 0 = n1 + n2
	// unchanged. Use shard 1 with owner n3 instead.
	id3 := clientIn(2, 1, "z")
	h.applyMap(shardmap.Compute(2, []string{"n1", "n2", "n3"}, 2, 2))
	h.setLease("default", id3, "n3", 1)
	h.nodes["n3"].sessions.setSessions(&store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id3, Epoch: 1}})
	// Shard 1 is now led by n2 with follower n3; a session owned by n3 there
	// targets n2. Retained in shard 1 stays with n2; n1 (no longer a holder)
	// drops its replica copy, and n3 receives it as the new follower.
	h.waitFor("n1 drops the shard-1 retained replica and n3 gains it", 5*time.Second, func() bool {
		for _, m := range h.nodes["n1"].rep.ReplicaState().Retained {
			if m.Topic == rt {
				return false
			}
		}
		for _, m := range h.nodes["n3"].rep.ReplicaState().Retained {
			if m.Topic == rt {
				return true
			}
		}
		return false
	})
	// Now move leadership of shard 1 away from n2 by dropping n2: basis n1 + n3,
	// shard 0 = n1 + n3, shard 1 = n3 + n1. n3 promotes its replica copy to
	// its local store; n2 hands over what it led and lets go of the local copy.
	h.applyMap(shardmap.Compute(3, []string{"n1", "n3"}, 2, 2))
	h.waitFor("n3 leads the retained message", 5*time.Second, func() bool {
		m, ok := h.nodes["n3"].sessions.has("default", rt)
		return ok && string(m.Payload) == "kept"
	})
	h.waitFor("n2 let go of its local copy", 5*time.Second, func() bool {
		_, ok := h.nodes["n2"].sessions.has("default", rt)
		return !ok
	})
	// n1 gained shard 1 as a follower and receives the retained replica.
	h.waitFor("n1 holds the retained replica again", 5*time.Second, func() bool {
		for _, m := range h.nodes["n1"].rep.ReplicaState().Retained {
			if m.Topic == rt {
				return true
			}
		}
		return false
	})
	// The session n1 owns in shard 1 is now replicated to n3 (shard 1's
	// leader), sent whole on the change.
	h.waitFor("n3 holds n1's session", 5*time.Second, func() bool {
		return h.nodes["n3"].rep.ReplicaState().Find("default", id) != nil
	})
	// A query from any node returns the retained message once.
	got := h.nodes["n1"].rep.QueryRetained("default", "t/#", time.Now())
	if len(got) != 1 || string(got[0].Payload) != "kept" {
		t.Fatalf("query after rebalance: %v", got)
	}
}

func TestDriverProposesFromTheLiveBasis(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 4, 2)
	now := time.Now()
	for _, n := range h.nodes {
		for _, o := range h.order {
			n.leases.nodes[o] = lease.NodeInfo{Alive: now}
		}
	}
	h.nodes["n1"].core.role = "leader"
	for _, n := range h.order {
		h.nodes[n].rep.Start()
	}
	h.waitFor("first map", 5*time.Second, func() bool { return h.nodes["n3"].table.Version() == 1 })
	m := h.nodes["n3"].table.Current()
	if len(m.Basis) != 3 || m.Replicas != 2 {
		t.Fatalf("first map %+v", m)
	}
	// n3 is declared dead: the leader recomputes over n1 and n2. Its stamp
	// is as fresh as the others', and is not what the basis reads.
	for _, n := range h.nodes {
		n.leases.mu.Lock()
		n.leases.nodes["n3"] = lease.NodeInfo{Alive: now, DeadAt: 7}
		n.leases.mu.Unlock()
	}
	h.waitFor("second map", 5*time.Second, func() bool { return h.nodes["n1"].table.Version() == 2 })
	m = h.nodes["n1"].table.Current()
	if len(m.Basis) != 2 || m.Basis[0] != "n1" || m.Basis[1] != "n2" {
		t.Fatalf("second map basis %v", m.Basis)
	}
	time.Sleep(300 * time.Millisecond)
	if h.nodes["n1"].table.Version() != 2 {
		t.Fatal("map re-proposed without a basis change")
	}
}

// TestSkipEpochCheckControl is the negative control for the rejection rule:
// with the follower's epoch check disabled, the same stale write is applied
// and its barrier passes.
func TestSkipEpochCheckControl(t *testing.T) {
	h := newHarnessWith(t, []string{"n1", "n2"}, 2, 2, func(o *Options) { o.SkipEpochCheck = true })
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	id := clientIn(2, 0)
	h.nodes["n1"].leases.set("default", id, "n1", 1)
	h.nodes["n2"].leases.set("default", id, "n2", 2)
	n1 := h.nodes["n1"]
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatalf("defective build: barrier should pass: %v", err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("defective build: the stale record should have been applied")
	}
}

// TestReconcileIsOrderedAndCoalesced: map changes applied back to back are
// acted on by one worker from the last reconciled map to the latest, and the
// version last acted on is persisted for the next incarnation.
func TestReconcileIsOrderedAndCoalesced(t *testing.T) {
	dir := t.TempDir()
	h := newHarnessWith(t, []string{"n1", "n2"}, 2, 2, func(o *Options) {
		if o.Node == "n1" {
			o.ReplicaDir = dir
		}
	})
	for v := uint64(1); v <= 5; v++ {
		h.applyMap(shardmap.Compute(v, h.order, 2, 2))
	}
	h.waitFor("reconciled to the latest map", 5*time.Second, func() bool { return h.nodes["n1"].rep.Reconciled() == 5 })
	h.waitFor("reconciled version persisted", 5*time.Second, func() bool {
		data, err := os.ReadFile(filepath.Join(dir, reconciledFile))
		return err == nil && strings.Contains(string(data), `"version":5`)
	})
	// A new incarnation over the same directory starts from the persisted
	// version and never drops against a map below it.
	fresh := &Replicator{o: Options{ReplicaDir: dir, Logger: quiet}, log: quiet}
	if got, known := fresh.readReconciled(); got != 5 || !known {
		t.Fatalf("persisted version %d known %v, want 5 known", got, known)
	}
	if got, _ := fresh.readReconciled(); got != 5 {
		t.Fatal("re-read differs")
	}
	none := &Replicator{o: Options{ReplicaDir: t.TempDir(), Logger: quiet}, log: quiet}
	if got, known := none.readReconciled(); got != 0 || !known {
		t.Fatalf("missing file should read as zero and known, got %d %v", got, known)
	}
}

// The tap runs inside the store's lock, and the store's lock is reached with
// the session manager's lock held on the connect path. So a tap that reads the
// lease table hands every connect the lease table's contention — which is
// exactly what a failover storm produces (WI 1292). The epoch now rides on the
// record, and this asserts the lookup is gone rather than merely cheap: a
// Leases whose Lookup fails the test.
func TestTapDoesNotReadTheLeaseTable(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	id := clientIn(2, 0)
	h.setLease("default", id, "n1", 3)
	n1 := h.nodes["n1"]
	n1.leases.onLookup = func(ns, cid string) {
		t.Errorf("the tap read the lease table for %s/%s; the epoch rides on the record", ns, cid)
	}
	defer func() { n1.leases.onLookup = nil }()
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 3}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: id, Epoch: 3}, store.Entry{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("x")}, QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := n1.own.Sync(); err != nil {
		t.Fatal(err)
	}
	// The record still reached the follower under the epoch it carried.
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if h.nodes["n2"].rep.ReplicaState().Find("default", id) == nil {
		t.Fatal("the follower did not hold the copy")
	}
}

// TestHoleOpenedDuringResyncCooldownIsRepaired: a stream that breaks within
// the resync cooldown has its resync refused, and nothing else asks again when
// the link carries on without another failure or state change. Barriers must
// not fail for that peer indefinitely: the hole is repaired once the cooldown
// has passed (WI 1491).
func TestHoleOpenedDuringResyncCooldownIsRepaired(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	n1 := h.nodes["n1"]
	h.writeSession("n1", clientIn(2, 0), 1)
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatalf("barrier before the break: %v", err)
	}
	p := n1.rep.outboundFor("n2")
	// A resync has just finished: the cooldown starts now.
	p.mu.Lock()
	p.lastResync = n1.rep.o.Clock()
	p.mu.Unlock()
	// The stream breaks, as a send that times out under load does, and the
	// send loop asks for a resync, which the cooldown refuses.
	p.broken()
	n1.rep.resync("n2")
	p.mu.Lock()
	holed := p.hole
	p.mu.Unlock()
	if !holed {
		t.Fatal("the stream is not holed after the break; the case tests nothing")
	}
	h.writeSession("n1", clientIn(2, 0, "after"), 1)
	h.waitFor("a barrier to pass again", 4*n1.rep.o.AckTimeout, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		return n1.barrier(ctx) == nil
	})
}

// TestCooldownRetryIsArrangedOnce: however many times a holed stream asks for
// a resync during one cooldown, one retry is arranged, and a replicator
// stopped with it pending stops promptly (WI 1491).
func TestCooldownRetryIsArrangedOnce(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	n1 := h.nodes["n1"]
	p := n1.rep.outboundFor("n2")
	p.mu.Lock()
	p.lastResync = n1.rep.o.Clock()
	p.mu.Unlock()
	p.broken()
	before := p.retries.Load()
	for i := 0; i < 50; i++ {
		n1.rep.resync("n2")
	}
	p.mu.Lock()
	pending, resyncing := p.retryPending, p.resyncing
	p.mu.Unlock()
	if got := p.retries.Load() - before; got != 1 || !pending {
		t.Fatalf("50 requests in one cooldown arranged %d retries (pending %v), want exactly 1", got, pending)
	}
	if resyncing {
		t.Fatal("a resync started inside the cooldown")
	}
	stopped := make(chan struct{})
	go func() {
		n1.rep.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(n1.rep.o.AckTimeout / 2):
		t.Fatal("Stop waited out the pending retry instead of ending it")
	}
}

// TestBreakWithNothingToResendDoesNotPinTheBarrier: a stream breaks after
// frames for a session this node no longer holds, and the resync finds
// nothing to send the peer. Every frame sent was marked failed, and if nothing
// is pushed afterwards a barrier's target never passes the failed mark: every
// barrier for that peer fails until some unrelated write happens to go to it
// (WI 1494).
func TestBreakWithNothingToResendDoesNotPinTheBarrier(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	n1 := h.nodes["n1"]
	p := n1.rep.outboundFor("n2")
	// A frame for a session n1 does not hold: nothing replaces it on resync.
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "gone", Epoch: 1})
	h.waitFor("the frame acknowledged", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.ackSeq >= p.sent
	})
	p.broken()
	n1.rep.resync("n2")
	h.waitFor("the hole closed", 4*n1.rep.o.AckTimeout, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return !p.hole
	})
	h.waitFor("a barrier to pass after the resync", 4*n1.rep.o.AckTimeout, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		// The frame this test pushed is for "gone"; name it, since it was
		// pushed straight onto the stream and never went through the store.
		err := n1.barrier(ctx, store.Scope{Namespace: "default", ClientID: "gone"})
		if err != nil {
			t.Logf("barrier: %v", err)
		}
		return err == nil
	})
}

// TestBarrierFailureReasons: each way a barrier's wait fails is counted under
// its own reason (WI 1494).
func TestBarrierFailureReasons(t *testing.T) {
	fresh := func() *outbound {
		p := &outbound{name: "peer"}
		p.cond = sync.NewCond(&p.mu)
		p.sent = 3
		return p
	}
	up := func(string) bool { return true }
	down := func(string) bool { return false }
	quick := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}
	cases := map[string]func() error{
		"down":    func() error { return fresh().wait(quick(), down, probeScope) },
		"timeout": func() error { return fresh().wait(quick(), up, probeScope) },
		"refused": func() error { p := fresh(); p.failed = 3; return p.wait(quick(), up, probeScope) },
		// A refusal that could not be scoped blocks the stream whole, and is
		// counted under its own reason so it is never read as a broken send.
		"unscoped": func() error { p := fresh(); p.streamBlocked = true; return p.wait(quick(), up, probeScope) },
		// A named key this peer refused (WI 1510): the record is not held.
		"not-held": func() error {
			p := fresh()
			p.refused = map[store.Scope]refusal{probeScope[0].Bare(): {epoch: 0, class: "stale-epoch"}}
			return p.wait(quick(), up, probeScope)
		},
		"resync":  func() error { p := fresh(); p.hole = true; return p.wait(quick(), up, probeScope) },
		"stopped": func() error { p := fresh(); p.closed = true; return p.wait(quick(), up, probeScope) },
	}
	for want, run := range cases {
		err := run()
		if err == nil {
			t.Fatalf("%s: the wait passed", want)
		}
		if got := barrierReason(err); got != want {
			t.Errorf("reason %q, want %q (%v)", got, want, err)
		}
		if want != "timeout" && !errors.Is(err, ErrReplicaUnavailable) {
			t.Errorf("%s: %v no longer wraps ErrReplicaUnavailable", want, err)
		}
		if want == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("timeout: %v no longer wraps the context's error", err)
		}
	}
}

// TestLostAckWithNoFurtherFramesDoesNotPinTheBarrier: the peer applies the
// last frame but its acknowledgement is lost, and no later frame follows to
// carry a cumulative acknowledgement. Barriers must not time out for that
// peer indefinitely once acknowledgements get through again (WI 1494).
func TestLostAckWithNoFurtherFramesDoesNotPinTheBarrier(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	p := n1.rep.outboundFor("n2")
	h.waitFor("the stream settled", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.ackSeq >= p.sent
	})
	// n2's acknowledgements to n1 are dropped while one frame goes out.
	n2.tr.SetFault("n1", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.ReplicationAck}, Rand: rand.New(rand.NewSource(1))})
	p.push(frame{Kind: kindResyncMark})
	time.Sleep(500 * time.Millisecond)
	p.mu.Lock()
	stuck := p.ackSeq < p.sent
	p.mu.Unlock()
	if !stuck {
		t.Fatal("the frame was acknowledged despite the fault; the case tests nothing")
	}
	n2.tr.SetFault("n1", nil)
	h.waitFor("a barrier to pass once acknowledgements get through", 4*n1.rep.o.AckTimeout, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		// The marker this test pushed carries no session, so name a scope
		// explicitly: what is under test is the acknowledgement, not the
		// naming.
		return n1.barrier(ctx, probeScope...) == nil
	})
}
