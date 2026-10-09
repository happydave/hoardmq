package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/etcdraft"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/wal"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func init() {
	if os.Getenv("HOARDMQ_TEST_DEBUG") != "" {
		quiet = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
}

// fakeSessions stands in for the session manager: it holds exportable
// states and records what the service asked of it.
type fakeSessions struct {
	mu        sync.Mutex
	states    map[string]*store.SessionState // key ns/id
	evicted   []string
	detached  []string
	imported  []string
	refused   []string // imports ImportIf's check refused
	wills     []string // topics
	fenced    int
	fencedAt  time.Time
	evictedAt map[string]time.Time
	connAges  map[string]time.Duration
	// onTake, when set, runs at the top of Take and Seal: a test uses it to
	// make a pass take a known time.
	onTake func(ns, id string)
	// onDrop, when set, runs at the top of DropSealed: a test uses it to hold
	// a drop, as its journal sync would.
	onDrop func(ns, id string)
	// sealed holds what Seal took, with the token it is sealed for; dropped
	// lists the keys whose sealed copy DropSealed ended.
	sealed      map[string]*fakeSealed
	dropped     []string
	durableErr  error
	durableAsks int
}

type fakeSealed struct {
	st    *store.SessionState
	token uint64
}

func newFakeSessions() *fakeSessions {
	return &fakeSessions{states: map[string]*store.SessionState{}, evictedAt: map[string]time.Time{}, connAges: map[string]time.Duration{}, sealed: map[string]*fakeSealed{}}
}

func (f *fakeSessions) Seal(ns, id string, _ packet.ReasonCode, token uint64) SealResult {
	f.mu.Lock()
	fn := f.onTake
	f.mu.Unlock()
	if fn != nil {
		fn(ns, id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := f.key(ns, id)
	if st, ok := f.states[k]; ok {
		if st.Epoch > token {
			return SealResult{Token: st.Epoch, Outcome: SealNewer}
		}
		delete(f.states, k)
		f.sealed[k] = &fakeSealed{st: st, token: token}
		cp := *st
		return SealResult{State: &cp, Token: token, Outcome: SealSealed}
	}
	if c := f.sealed[k]; c != nil {
		raised := c.token < token
		if raised {
			c.token = token
		}
		cp := *c.st
		return SealResult{State: &cp, Token: c.token, Outcome: SealAlready, Raised: raised}
	}
	return SealResult{}
}

func (f *fakeSessions) Sealed(ns, id string) (*store.SessionState, uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.sealed[f.key(ns, id)]
	if c == nil {
		return nil, 0, false
	}
	cp := *c.st
	return &cp, c.token, true
}

func (f *fakeSessions) DropSealed(ns, id string, upTo uint64) bool {
	f.mu.Lock()
	fn := f.onDrop
	f.mu.Unlock()
	if fn != nil {
		fn(ns, id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := f.key(ns, id)
	c := f.sealed[k]
	if c == nil || c.token > upTo {
		return false
	}
	delete(f.sealed, k)
	f.dropped = append(f.dropped, k)
	return true
}

func (f *fakeSessions) EvictOffline(ns, id string, upTo uint64) bool {
	f.mu.Lock()
	k := f.key(ns, id)
	st, ok := f.states[k]
	_, live := f.connAges[k]
	f.mu.Unlock()
	if !ok || live || st.Epoch > upTo {
		return false
	}
	return f.Evict(ns, id, packet.SessionTakenOver)
}

func (f *fakeSessions) HeldAt(ns, id string) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[f.key(ns, id)]
	if !ok {
		return 0, false
	}
	return st.Epoch, true
}

func (f *fakeSessions) SealedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sealed)
}

func (f *fakeSessions) Durable(context.Context, []store.Scope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.durableAsks++
	return f.durableErr
}

func (f *fakeSessions) setDurableErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.durableErr = err
}

func (f *fakeSessions) sealedCount() int { return f.SealedCount() }

func (f *fakeSessions) key(ns, id string) string { return ns + "/" + id }

func (f *fakeSessions) put(ns, id string, st *store.SessionState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[f.key(ns, id)] = st
}

func (f *fakeSessions) Export(ns, id string) (*store.SessionState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[f.key(ns, id)]
	return st, ok
}

func (f *fakeSessions) Import(st *store.SessionState) { f.ImportIf(st, nil) }

func (f *fakeSessions) ImportIf(st *store.SessionState, admit func() bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if admit != nil && !admit() {
		f.refused = append(f.refused, f.key(st.Namespace, st.ClientID))
		return false
	}
	f.states[f.key(st.Namespace, st.ClientID)] = st
	f.imported = append(f.imported, f.key(st.Namespace, st.ClientID))
	delete(f.sealed, f.key(st.Namespace, st.ClientID))
	return true
}

func (f *fakeSessions) Evict(ns, id string, _ packet.ReasonCode) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := f.key(ns, id)
	_, ok := f.states[k]
	delete(f.states, k)
	f.evicted = append(f.evicted, k)
	if _, seen := f.evictedAt[k]; !seen {
		f.evictedAt[k] = time.Now()
	}
	return ok
}

func (f *fakeSessions) Each(fn func(ns, id string)) {
	f.mu.Lock()
	keys := make([]string, 0, len(f.states))
	for k := range f.states {
		keys = append(keys, k)
	}
	f.mu.Unlock()
	for _, k := range keys {
		ns, id, _ := strings.Cut(k, "/")
		fn(ns, id)
	}
}

func (f *fakeSessions) setOnDrop(fn func(ns, id string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onDrop = fn
}

// seal files a sealed copy of a client's session for token, as a pass would.
func (f *fakeSessions) seal(ns, id string, token uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sealed[f.key(ns, id)] = &fakeSealed{st: &store.SessionState{SessionMeta: store.SessionMeta{Namespace: ns, ClientID: id, Epoch: token}}, token: token}
}

func (f *fakeSessions) setOnTake(fn func(ns, id string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onTake = fn
}

func (f *fakeSessions) Take(ns, id string, r packet.ReasonCode) (*store.SessionState, bool) {
	f.mu.Lock()
	fn := f.onTake
	f.mu.Unlock()
	if fn != nil {
		fn(ns, id)
	}
	st, ok := f.Export(ns, id)
	f.Evict(ns, id, r)
	return st, ok
}

func (f *fakeSessions) Detach(ns, id string, _ packet.ReasonCode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detached = append(f.detached, f.key(ns, id))
}

func (f *fakeSessions) SetEpoch(string, string, uint64) {}

func (f *fakeSessions) PublishWill(w *store.WillRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wills = append(f.wills, w.Message.Topic)
}

func (f *fakeSessions) FenceAll(packet.ReasonCode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fenced++
	if f.fencedAt.IsZero() {
		f.fencedAt = time.Now()
	}
}

// ConnAge reports a live connection's age when the test set one.
func (f *fakeSessions) ConnAge(ns, id string) (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	age, ok := f.connAges[f.key(ns, id)]
	return age, ok
}

func (f *fakeSessions) setConnAge(ns, id string, age time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connAges[f.key(ns, id)] = age
}

func (f *fakeSessions) has(ns, id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.states[f.key(ns, id)]
	return ok
}

func (f *fakeSessions) importedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.imported)
}

func (f *fakeSessions) willCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.wills)
}

func (f *fakeSessions) fenceCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fenced
}

// tnode is one in-process node with transport, core and lease service.
type tnode struct {
	name     string
	dir      string
	tr       *transport.Transport
	core     *etcdraft.Core
	svc      *Service
	sessions *fakeSessions
}

type tcluster struct {
	t     *testing.T
	dir   string
	names []string
	nodes map[string]*tnode
	addrs map[string]string
	ttl   time.Duration
	// fallback is every node's replica fallback; nil unless a test set
	// fixtureFallback before building the cluster.
	fallback func(ctx context.Context, ns, id, prevOwner string) (*store.SessionState, bool)
	// down is the replica holders every node's fallback reports down; nil
	// unless a test set fixtureDown.
	down func(ns, id string) []string
	// kept is the copy a former holder kept, and that holder, every node's
	// fallback reports beside its answer; nil unless a test set fixtureKept.
	kept func(ns, id string) (*store.SessionState, string)
}

// fixtureFallback is handed to the next cluster built. A test that sets it
// restores nil in a cleanup and does not run in parallel.
var fixtureFallback func(ctx context.Context, ns, id, prevOwner string) (*store.SessionState, bool)

// fixtureDown names the replica holders the next cluster's fallback reports
// down, under the same rule as fixtureFallback.
var fixtureDown func(ns, id string) []string

// fixtureKept gives the next cluster's fallback a kept copy found while a
// source did not answer, under the same rule as fixtureFallback.
var fixtureKept func(ns, id string) (*store.SessionState, string)

// replicaFallback is the cluster's fallback in the form Options takes.
func (c *tcluster) replicaFallback() func(ctx context.Context, ns, id, prevOwner string) Fetched {
	if c.fallback == nil {
		return nil
	}
	return func(ctx context.Context, ns, id, prevOwner string) Fetched {
		st, answered := c.fallback(ctx, ns, id, prevOwner)
		f := Fetched{State: st, Answered: answered}
		if c.down != nil {
			f.Down = c.down(ns, id)
		}
		if c.kept != nil {
			f.Kept, f.KeptFrom = c.kept(ns, id)
		}
		return f
	}
}

// fixtureOptions adjusts each node's options in the next cluster built, under
// the same rule as fixtureFallback.
var fixtureOptions func(o *Options)

// fixtureProbe gives each node of the next cluster built its table's level
// probe, under the same rule as fixtureFallback.
var fixtureProbe func(node string) func(index, level uint64, op Op)

// fixtureClock gives each node of the next cluster built its wall clock,
// under the same rule as fixtureFallback; the steady clock stays the real one.
var fixtureClock func(node string) func() time.Time

// fixtureService adjusts each node's service in the next cluster built, after
// it is made and before it starts, under the same rule as fixtureFallback.
var fixtureService func(node string, s *Service)

const secret = "lease-test-shared-secret-of-enough-bytes"

// newCluster starts transports, cores and services for the names with
// everyone knowing everyone, and waits until the group has a leader and
// every node is a voter.
func newCluster(t *testing.T, names []string, ttl time.Duration, advertise func(name string) string) *tcluster {
	t.Helper()
	return newClusterWith(t, names, ttl, advertise, nil)
}

// newClusterWith is newCluster with a hook on each node's transport options.
func newClusterWith(t *testing.T, names []string, ttl time.Duration, advertise func(name string) string, tweak func(name string, o *transport.Options)) *tcluster {
	t.Helper()
	c := &tcluster{t: t, dir: t.TempDir(), names: names, nodes: map[string]*tnode{}, addrs: map[string]string{}, ttl: ttl, fallback: fixtureFallback, down: fixtureDown, kept: fixtureKept}
	for _, name := range names {
		var ps []config.Peer
		for _, o := range names {
			if o != name {
				ps = append(ps, config.Peer{Name: o, Address: "127.0.0.1:1"})
			}
		}
		o := transport.Options{Node: name, ClusterID: "t", Listen: "127.0.0.1:0", Peers: ps, Auth: transport.Auth{Mode: "secret", Secret: []byte(secret)},
			Heartbeat: 50 * time.Millisecond, HeartbeatMisses: 4, HandshakeTimeout: 2 * time.Second, Logger: quiet}
		if tweak != nil {
			tweak(name, &o)
		}
		tr, err := transport.New(o)
		if err != nil {
			t.Fatal(err)
		}
		if err := tr.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(tr.Stop)
		c.nodes[name] = &tnode{name: name, dir: filepath.Join(c.dir, name), tr: tr, sessions: newFakeSessions()}
		c.addrs[name] = tr.Addr().String()
	}
	for _, name := range names {
		adv := ""
		if advertise != nil {
			adv = advertise(name)
		}
		c.startNode(c.nodes[name], adv)
	}
	c.settled()
	// Every node must have renewed once, and every node must see it, before
	// a test relies on leases.
	waitFor(t, "alive records on every node", 10*time.Second, func() bool {
		for _, n := range c.nodes {
			if n.svc.Fenced() {
				return false
			}
			for _, o := range c.names {
				if _, ok := n.svc.Table().Node(o); !ok {
					return false
				}
			}
		}
		return true
	})
	return c
}

func (c *tcluster) peersFor(name string) []config.Peer {
	var ps []config.Peer
	for _, o := range c.names {
		if o != name {
			ps = append(ps, config.Peer{Name: o, Address: c.addrs[o]})
		}
	}
	return ps
}

// startNode opens the store, builds the machine, the lease service and the
// core, and starts them; the fake sessions object survives restarts.
func (c *tcluster) startNode(n *tnode, advertise string) {
	c.t.Helper()
	st, err := raftlog.Open(n.dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		c.t.Fatal(err)
	}
	machine := consensus.NewMachine()
	core, err := etcdraft.New(etcdraft.Options{Node: n.name, Peers: c.peersFor(n.name), Store: st, Transport: n.tr, Machine: machine,
		Tick: 10 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2, PromoteLag: 50, JoinTimeout: 500 * time.Millisecond, Logger: quiet})
	if err != nil {
		c.t.Fatal(err)
	}
	o := Options{Node: n.name, Core: core, Machine: machine, Transport: n.tr, Sessions: n.sessions, TTL: c.ttl, TakeoverTimeout: time.Second, Advertise: advertise, Logger: quiet, Fallback: c.replicaFallback()}
	if fixtureClock != nil {
		o.Clock, o.Steady = fixtureClock(n.name), time.Now
	}
	if fixtureOptions != nil {
		fixtureOptions(&o)
	}
	svc, err := New(o)
	if err != nil {
		c.t.Fatal(err)
	}
	if fixtureProbe != nil {
		svc.table.levelProbe = fixtureProbe(n.name)
	}
	if fixtureService != nil {
		fixtureService(n.name, svc)
	}
	if err := core.Start(); err != nil {
		c.t.Fatal(err)
	}
	svc.Start()
	n.core, n.svc = core, svc
	c.t.Cleanup(func() {
		svc.Stop()
		core.Stop()
	})
}

func (c *tcluster) stopNode(n *tnode) {
	n.svc.Stop()
	n.core.Stop()
	n.tr.Stop()
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

// settled waits for one leader and every node a voter.
func (c *tcluster) settled() *tnode {
	c.t.Helper()
	var lead *tnode
	waitFor(c.t, "a settled cluster", 20*time.Second, func() bool {
		var name string
		for _, n := range c.nodes {
			if n.core == nil {
				continue
			}
			l, ok := n.core.Leader()
			if !ok || (name != "" && l != name) {
				return false
			}
			name = l
		}
		lead = c.nodes[name]
		if lead == nil || lead.core.Status().Role != "leader" {
			return false
		}
		voters := 0
		for _, m := range lead.core.Members() {
			if m.Role == consensus.Voter {
				voters++
			}
		}
		return voters == len(c.nodes)
	})
	return lead
}

func acquire(t *testing.T, n *tnode, id string, will *store.WillRecord, expiry uint32, clean bool) (AcquireResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return n.svc.Acquire(ctx, "default", id, will, expiry, clean)
}

func willFor(topic string, delay uint32) *store.WillRecord {
	return &store.WillRecord{Message: &message.Message{Topic: topic, Payload: []byte("bye")}, Delay: delay}
}

func TestTakeoverFromLiveOwnerPullsState(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	// X connects on A: a lease at its grant's token, a session with a
	// subscription.
	res, err := acquire(t, a, "x", willFor("w/x", 0), 300, true)
	if err != nil || res.Epoch == 0 || res.Local {
		t.Fatalf("first acquire: %+v %v", res, err)
	}
	first := res.Epoch
	a.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: first}, Subscriptions: []store.SubscriptionRecord{{Filter: "t/x", QoS: 1}}})
	// Reconnect on A: served locally at the same epoch. Local is the signal
	// that no proposal was awaited; the proposal counter is not, because the
	// node's alive record is proposed every ttl/3 in the background.
	res, err = acquire(t, a, "x", nil, 300, false)
	if err != nil || !res.Local || res.Epoch != first {
		t.Fatalf("local renew: %+v %v, want the same token %d", res, err, first)
	}
	// X connects on B with Clean Start 0: takeover with the state pulled.
	res, err = acquire(t, b, "x", nil, 300, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Epoch <= first || res.PrevOwner != "n1" || res.State == nil || len(res.State.Subscriptions) != 1 || res.State.Epoch != res.Epoch {
		t.Fatalf("takeover: %+v, want a token above %d and the state stamped with it", res, first)
	}
	second := res.Epoch
	waitFor(t, "A evicted x", 3*time.Second, func() bool { return !a.sessions.has("default", "x") })
	// The third node applies the commit a moment after the proposer does.
	waitFor(t, "epoch fencing view on a third node", 3*time.Second, func() bool {
		return c.nodes["n3"].svc.Valid("default", "x", "n2", second) && !c.nodes["n3"].svc.Valid("default", "x", "n1", first)
	})
	// Clean Start 1 elsewhere: takeover without a pull; A evicts anyway.
	b.sessions.put("default", "x", res.State)
	res, err = acquire(t, a, "x", nil, 300, true)
	if err != nil || res.Epoch <= second || res.State != nil {
		t.Fatalf("clean-start takeover: %+v %v, want a token above %d", res, err, second)
	}
	waitFor(t, "B evicted x", 3*time.Second, func() bool { return !b.sessions.has("default", "x") })
}

func TestTakeoverFromDeadOwnerAndAdoption(t *testing.T) {
	ttl := 1500 * time.Millisecond
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	// Pick a non-leader owner so the leader can adopt.
	var owner *tnode
	for _, n := range c.nodes {
		if n != lead {
			owner = n
			break
		}
	}
	// Two clients on the owner: one with a Will and a 3 s expiry (adopted
	// while connected), one already offline with a 1 s expiry left.
	if _, err := acquire(t, owner, "alive", willFor("will/alive", 1), 3, true); err != nil {
		t.Fatal(err)
	}
	gone, err := acquire(t, owner, "gone", nil, 60, true)
	if err != nil {
		t.Fatal(err)
	}
	owner.svc.Disconnected("default", "gone", gone.Epoch, time.Now().Add(time.Second), false)
	waitFor(t, "gone recorded offline", 3*time.Second, func() bool {
		l, ok := lead.svc.Table().Lookup("default", "gone")
		return ok && !l.Online
	})
	info, known := lead.svc.Table().Node(owner.name)
	t.Logf("owner %s alive record known=%v alive=%s age=%s", owner.name, known, info.Alive.Format(time.StampMilli), time.Since(info.Alive))
	dead := time.Now()
	c.stopNode(owner)
	// After the ttl the leader adopts both leases.
	waitFor(t, "adoption", ttl+5*time.Second, func() bool {
		l, ok := lead.svc.Table().Lookup("default", "alive")
		return ok && l.Owner == lead.name
	})
	adopted := time.Now()
	if adopted.Sub(info.Alive) < ttl {
		t.Fatalf("adopted %s after the owner's last alive record, before the ttl %s (death was %s after it)", adopted.Sub(info.Alive), ttl, dead.Sub(info.Alive))
	}
	// The Will publishes about one second after adoption, on the adopter.
	waitFor(t, "will published", 5*time.Second, func() bool { return lead.sessions.willCount() == 1 })
	if since := time.Since(adopted); since < 700*time.Millisecond {
		t.Fatalf("will published %s after adoption, before its delay", since)
	}
	// The offline client's expiry (already due) releases its lease; the
	// adopted client's lease expires three seconds after adoption.
	waitFor(t, "gone released", 5*time.Second, func() bool {
		_, ok := lead.svc.Table().Lookup("default", "gone")
		return !ok
	})
	waitFor(t, "alive expired", 6*time.Second, func() bool {
		_, ok := lead.svc.Table().Lookup("default", "alive")
		return !ok
	})
	// A client whose dead owner's lease was adopted reconnects elsewhere:
	// takeover from the adopter, lapsed kind, no pull.
	if _, err := acquire(t, owner, "late", nil, 60, true); err == nil {
		t.Fatal("acquire on a stopped node succeeded")
	}
	other := c.nodes["n3"]
	if other == owner {
		other = c.nodes["n2"]
	}
	res, err := acquire(t, other, "alive", nil, 60, false)
	if err != nil || res.State != nil {
		t.Fatalf("reconnect after expiry: %+v %v", res, err)
	}
}

func TestPartitionFencesTheMinority(t *testing.T) {
	ttl := 1500 * time.Millisecond
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, func(name string) string { return name + ".clients:1883" })
	a, b, cc := c.nodes["n1"], c.nodes["n2"], c.nodes["n3"]
	if _, err := acquire(t, cc, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	cc.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: 1}})
	// Cut C off from A and B.
	for _, n := range []*tnode{a, b} {
		n.tr.Isolate(cc.name, true)
		cc.tr.Isolate(n.name, true)
	}
	// An immediate reconnect on A is refused: C is alive per the table and
	// unreachable, so taking the lease could give x two live sessions.
	time.Sleep(300 * time.Millisecond) // let the transport notice the cut
	if _, err := acquire(t, a, "x", nil, 300, false); err == nil {
		t.Fatal("A took over x while C might still be serving it")
	}
	// C fences itself within the ttl and refuses new ownership with a
	// redirect to a majority-side node.
	waitFor(t, "C fenced", ttl+3*time.Second, func() bool { return cc.sessions.fenceCount() == 1 })
	fencedAt := cc.sessions.fencedAt
	if d := cc.svc.LivenessStats().FencesDeferred; d != 0 {
		t.Fatalf("the isolated node deferred its fence %d times (no quorum: must not)", d)
	}
	_, err := acquire(t, cc, "y", nil, 60, true)
	var ref *Refusal
	if !errors.As(err, &ref) || (ref.Reason != packet.UseAnotherServer && ref.Reason != packet.ServerUnavailable) {
		t.Fatalf("minority acquire: %v", err)
	}
	if ref.Reason == packet.UseAnotherServer && ref.ServerReference == "" {
		t.Fatal("redirect without a server reference")
	}
	// X reconnects on A: refused while C's lease is still valid but C is
	// unreachable, accepted once the lease lapsed (by then the leader may
	// have adopted it, so the acquire is either a takeover or local).
	var res AcquireResult
	refusals := 0
	waitFor(t, "A takes over x", ttl+5*time.Second, func() bool {
		r, err := acquire(t, a, "x", nil, 300, false)
		if err != nil {
			refusals++
			return false
		}
		res = r
		return true
	})
	takenAt := time.Now()
	if res.Epoch < 2 || res.State != nil {
		t.Fatalf("takeover across the partition: %+v", res)
	}
	if l, _ := a.svc.Table().Lookup("default", "x"); l.Owner != a.name && !res.Local {
		t.Fatalf("lease after takeover %+v", l)
	}
	if takenAt.Before(fencedAt) {
		t.Fatalf("A served x at %s before C fenced at %s", takenAt.Format(time.StampMilli), fencedAt.Format(time.StampMilli))
	}
	_ = refusals
	// Heal: C rejoins, renews, and serves again.
	for _, n := range []*tnode{a, b} {
		n.tr.Isolate(cc.name, false)
		cc.tr.Isolate(n.name, false)
	}
	waitFor(t, "C unfenced", ttl+5*time.Second, func() bool { return !cc.svc.Fenced() })
	if _, err := acquire(t, cc, "z", nil, 60, true); err != nil {
		t.Fatalf("acquire after heal: %v", err)
	}
	waitFor(t, "C evicted x", 3*time.Second, func() bool { return !cc.sessions.has("default", "x") })
}

func TestBatchingUnderConcurrentConnects(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 5*time.Second, nil)
	n := c.nodes["n2"] // a follower: batches are forwarded
	before := n.svc.Proposals()
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := acquire(t, n, fmt.Sprintf("c%d", i), nil, 0, true); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	made := n.svc.Proposals() - before
	if made >= 50 {
		t.Fatalf("200 connects took %d proposals", made)
	}
	// The follower's own table catches up with the leader's applies.
	waitFor(t, "200 leases on the follower", 5*time.Second, func() bool { return n.svc.Table().Count() == 200 })
}

// follower returns a node that is not the leader.
func (c *tcluster) follower(lead *tnode) *tnode {
	for _, name := range c.names {
		if n := c.nodes[name]; n != lead {
			return n
		}
	}
	return nil
}

// wedgeOrderedLane fills a node's ordered queue to the leader with frames
// the writer takes 5 ms each to put on the wire, and keeps it full: what a
// storm's traffic does to the link (WI 1337). Anything on the ordered lane
// waits about five seconds; the urgent lane waits one frame. Since WI 1340
// the batched lease proposals ride the control connection, so the flood is
// a control-lane class the leader has no handler for (redirect hints).
func wedgeOrderedLane(t *testing.T, from *tnode, to string) {
	t.Helper()
	from.tr.SetFault(to, &transport.Fault{Delay: 5 * time.Millisecond, Classes: []transport.Class{transport.RedirectHint}})
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		payload := make([]byte, 256)
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_ = from.tr.Send(ctx, to, transport.RedirectHint, payload)
			cancel()
		}
	}()
}

// TestRenewalBypassesTheWedgedOrderedLane: a follower whose ordered lane to
// the leader is wedged cannot get a batched proposal through (its acquires
// time out), yet keeps renewing on the urgent lane and never fences. The
// negative control puts the same node's renewals on the ordered lane — a
// Version 1 link, where the service falls back to the batch — and it fences.
func TestRenewalBypassesTheWedgedOrderedLane(t *testing.T) {
	ttl := 1500 * time.Millisecond
	run := func(t *testing.T, tweak func(name string, o *transport.Options)) (*tcluster, *tnode) {
		t.Helper()
		c := newClusterWith(t, []string{"n1", "n2", "n3"}, ttl, nil, tweak)
		lead := c.settled()
		f := c.follower(lead)
		wedgeOrderedLane(t, f, lead.name)
		// The ordered lane is wedged: a batched proposal from the follower
		// does not get through within the takeover timeout.
		time.Sleep(200 * time.Millisecond)
		if _, err := acquire(t, f, "batched", nil, 0, true); err == nil {
			t.Fatal("an acquire over the wedged lane succeeded; the lane is not wedged")
		}
		return c, f
	}
	t.Run("urgent lane", func(t *testing.T) {
		_, f := run(t, nil)
		before := f.svc.LivenessStats()
		time.Sleep(2 * ttl)
		st := f.svc.LivenessStats()
		if f.sessions.fenceCount() != 0 || st.Fenced {
			t.Fatalf("follower fenced with the urgent lane open: %+v", st)
		}
		if st.RenewOK-before.RenewOK < 4 {
			t.Fatalf("renewals over two ttls: %d (want at least 4): %+v", st.RenewOK-before.RenewOK, st)
		}
		if st.RenewFallback != 0 {
			t.Fatalf("renewals took the batched path on a version-2 link: %+v", st)
		}
	})
	t.Run("negative control: ordered lane", func(t *testing.T) {
		_, f := run(t, func(name string, o *transport.Options) { o.MaxVersion = transport.Version1 })
		waitFor(t, "follower fenced", 2*ttl+2*time.Second, func() bool { return f.sessions.fenceCount() == 1 })
		if st := f.svc.LivenessStats(); st.RenewFallback == 0 {
			t.Fatalf("renewals did not take the batched path on a version-1 link: %+v", st)
		}
	})
}

// TestRenewalFallsBackOnVersion1Link: with the leader link at framing
// version 1, renewals take the batched path and the node stays unfenced.
func TestRenewalFallsBackOnVersion1Link(t *testing.T) {
	ttl := 1500 * time.Millisecond
	c := newClusterWith(t, []string{"n1", "n2", "n3"}, ttl, nil, func(name string, o *transport.Options) {
		if name == "n2" {
			o.MaxVersion = transport.Version1
		}
	})
	n := c.nodes["n2"]
	waitFor(t, "a fallback renewal", 2*ttl, func() bool { return n.svc.LivenessStats().RenewFallback > 0 })
	time.Sleep(ttl)
	if st := n.svc.LivenessStats(); n.sessions.fenceCount() != 0 || st.Fenced {
		t.Fatalf("fenced on the fallback path: %+v", st)
	}
	if _, err := acquire(t, n, "x", nil, 0, true); err != nil {
		t.Fatal(err)
	}
}

// TestRenewalInflightBound: a slow consensus (every proposal takes 900 ms,
// launches come every 375 ms) fills the two in-flight slots, further
// launches are skipped and counted, no more than two proposals ever run at
// once, and the node keeps renewing and never fences.
func TestRenewalInflightBound(t *testing.T) {
	ttl := 1500 * time.Millisecond
	core := &fakeCore{role: "leader", delay: 900 * time.Millisecond}
	sessions := newFakeSessions()
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: sessions, TTL: ttl, TakeoverTimeout: time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	t.Cleanup(s.Stop)
	time.Sleep(2 * ttl)
	st := s.LivenessStats()
	if st.RenewSkipped < 1 {
		t.Fatalf("no launch skipped with a 900 ms consensus: %+v", st)
	}
	if st.RenewOK < 4 {
		t.Fatalf("renewals over two ttls: %d: %+v", st.RenewOK, st)
	}
	if sessions.fenceCount() != 0 || st.Fenced {
		t.Fatalf("fenced: %+v", st)
	}
	if m := core.maxConcurrent.Load(); m > renewInflightMax {
		t.Fatalf("%d proposals ran at once, bound is %d", m, renewInflightMax)
	}
}

// TestRenewalWarnsBeforeFencing: a cut-off node counts a failure streak
// past the warning threshold before its ttl runs out, then fences.
func TestRenewalWarnsBeforeFencing(t *testing.T) {
	ttl := 1500 * time.Millisecond
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	n := c.nodes["n3"]
	for _, o := range []*tnode{c.nodes["n1"], c.nodes["n2"]} {
		o.tr.Isolate(n.name, true)
		n.tr.Isolate(o.name, true)
	}
	waitFor(t, "a failure streak before the fence", ttl, func() bool {
		return n.svc.LivenessStats().RenewStreak >= renewWarnStreak && n.sessions.fenceCount() == 0
	})
	waitFor(t, "fence", ttl+2*time.Second, func() bool { return n.sessions.fenceCount() == 1 })
}

// fakeCore is a leader that answers proposals after an optional delay and
// records how many ran at once; nothing is applied.
type fakeCore struct {
	members       []consensus.Member // reported by Members; nil unless a test sets it
	role          string
	leader        string // reported by Status/Leader; "me" when role is leader
	delay         time.Duration
	err           error // returned by every proposal when set
	calls         atomic.Int64
	recovering    atomic.Bool
	concurrent    atomic.Int32
	maxConcurrent atomic.Int32
}

func (f *fakeCore) Propose(ctx context.Context, _ consensus.Kind, _ []byte) (any, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	n := f.concurrent.Add(1)
	defer f.concurrent.Add(-1)
	for {
		m := f.maxConcurrent.Load()
		if n <= m || f.maxConcurrent.CompareAndSwap(m, n) {
			break
		}
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return []Result{{OK: true}}, nil
}
func (f *fakeCore) AddMember(context.Context, string, string) error { return nil }
func (f *fakeCore) PromoteMember(context.Context, string) error     { return nil }
func (f *fakeCore) RemoveMember(context.Context, string) error      { return nil }
func (f *fakeCore) Leader() (string, bool) {
	if f.role == "leader" {
		return "me", true
	}
	return f.leader, f.leader != ""
}
func (f *fakeCore) Members() []consensus.Member { return f.members }
func (f *fakeCore) Status() consensus.Status {
	if f.role == "leader" {
		return consensus.Status{Role: f.role, Leader: "me"}
	}
	return consensus.Status{Role: f.role, Leader: f.leader}
}
func (f *fakeCore) Ready() bool      { return true }
func (f *fakeCore) Recovering() bool { return f.recovering.Load() }
func (f *fakeCore) Stop()            {}

// TestConnectPathRenewalsAreSingleFlight: a fenced node's connect path
// calls alive() from every handshake at once; they must share one attempt,
// not each make a proposal (at 100k the per-caller proposals were their own
// storm on the core and kept both survivors fenced).
func TestConnectPathRenewalsAreSingleFlight(t *testing.T) {
	core := &fakeCore{role: "leader", delay: 200 * time.Millisecond}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 64
	var wg sync.WaitGroup
	oks := make(chan bool, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			oks <- s.alive()
		}()
	}
	wg.Wait()
	close(oks)
	for ok := range oks {
		if !ok {
			t.Fatal("a caller was not answered by the shared attempt")
		}
	}
	if n := s.Proposals(); n > 2 {
		t.Fatalf("%d callers made %d proposals (want at most 2)", callers, n)
	}
	if m := core.maxConcurrent.Load(); m > 1 {
		t.Fatalf("%d proposals ran at once from the connect path", m)
	}
}

// TestConnectPathRenewalRetriesAreFloored: when attempts fail at once (no
// leader), callers inside the retry floor get the failure without another
// attempt — a thousand handshakes must not turn into a thousand core calls.
func TestConnectPathRenewalRetriesAreFloored(t *testing.T) {
	core := &fakeCore{role: "leader", err: consensus.ErrDropped}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < 500; i++ {
		if s.alive() {
			t.Fatal("a failing core answered a renewal")
		}
	}
	if el := time.Since(start); el > s.renewRetry() {
		t.Skipf("the loop took %s, longer than the floor; the count would not discriminate", el)
	}
	if n := core.calls.Load(); n != 1 {
		t.Fatalf("500 callers inside the retry floor made %d core calls (want 1)", n)
	}
	time.Sleep(s.renewRetry() + 10*time.Millisecond)
	s.alive()
	if n := core.calls.Load(); n != 2 {
		t.Fatalf("a caller after the floor made %d core calls in total (want 2)", n)
	}
}

// fakeTransport reports the named peers as up (and the named down ones as
// down) and carries nothing.
type fakeTransport struct {
	up       []string
	down     []string // down for a long time (a partition)
	flapping []string // down for a moment (a link reset)
}

func (f *fakeTransport) Send(context.Context, string, transport.Class, []byte) error { return nil }
func (f *fakeTransport) Handle(transport.Class, transport.Handler)                   {}
func (f *fakeTransport) Peers() []transport.PeerInfo {
	out := make([]transport.PeerInfo, 0, len(f.up)+len(f.down))
	for _, n := range f.up {
		out = append(out, transport.PeerInfo{Name: n, State: transport.Up})
	}
	for _, n := range f.down {
		out = append(out, transport.PeerInfo{Name: n, State: transport.Down, DownForMs: 3600e3})
	}
	for _, n := range f.flapping {
		out = append(out, transport.PeerInfo{Name: n, State: transport.Connecting, DownForMs: 100})
	}
	return out
}

// TestFenceDeferredOnlyWhileLeaderlessWithQuorum: the fence rule's matrix
// (WI 1341). A lapsed record fences unless the group has no leader and this
// node plus the peers it can reach are a strict majority.
func TestFenceDeferredOnlyWhileLeaderlessWithQuorum(t *testing.T) {
	cases := []struct {
		name     string
		role     string
		leader   string
		up, down []string
		flapping []string
		known    time.Duration // how long the leader has been known
		fence    bool
	}{
		{"leader known a ttl, quorum up", "follower", "n2", []string{"n2", "n3"}, nil, nil, time.Second, true},
		{"leader known a ttl, no quorum", "follower", "n2", nil, []string{"n2", "n3"}, nil, time.Second, true},
		{"leader just elected, quorum up", "follower", "n2", []string{"n2", "n3"}, nil, nil, 100 * time.Millisecond, false},
		{"leader just elected, no quorum", "follower", "n2", nil, []string{"n2", "n3"}, nil, 100 * time.Millisecond, true},
		{"leader changed since the tenure clock started, quorum up", "follower", "n3", []string{"n2", "n3"}, nil, nil, time.Second, false},
		{"no leader, quorum up", "follower", "", []string{"n2"}, []string{"n3"}, nil, 0, false},
		{"candidate, quorum up", "candidate", "", []string{"n2", "n3"}, nil, nil, 0, false},
		{"no leader, no quorum (partition)", "follower", "", nil, []string{"n2", "n3"}, nil, 0, true},
		{"candidate, no quorum", "candidate", "", nil, []string{"n2", "n3"}, nil, 0, true},
		{"removed, peers up", "removed", "", []string{"n2", "n3"}, nil, nil, 0, false},
		{"no leader, one peer flapping (link reset)", "candidate", "", nil, []string{"n3"}, []string{"n2"}, 0, false},
		{"no leader, peers long down (partition)", "candidate", "", nil, []string{"n2", "n3"}, nil, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			sessions := newFakeSessions()
			core := &fakeCore{role: tc.role, leader: tc.leader}
			s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Transport: &fakeTransport{up: tc.up, down: tc.down, flapping: tc.flapping}, Sessions: sessions, TTL: time.Second, Logger: quiet, Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			s.lastAlive.Store(now.Add(-2 * time.Second).UnixNano())
			if tc.leader != "" {
				// The tenure clock belongs to "n2"; a status naming another
				// leader is a new tenure and trackLeader restarts the clock.
				s.leaderKnownSince.Store(now.Add(-tc.known).UnixNano())
				s.leaderKnown.Store("n2")
			}
			s.checkFence(s.trackLeader())
			if got := sessions.fenceCount() == 1; got != tc.fence {
				t.Fatalf("fenced=%v, want %v (%+v)", got, tc.fence, s.LivenessStats())
			}
			if deferred := s.LivenessStats().FencesDeferred; (deferred == 1) == tc.fence {
				t.Fatalf("fences_deferred=%d with fence=%v", deferred, tc.fence)
			}
		})
	}
}

// TestNoFenceWhileLeaderless: in a live cluster whose group loses its
// leader for longer than the ttl — two of three cores stopped, every
// transport left up — the remaining node defers its fence rather than
// disconnecting its clients; once a core returns and a leader is elected it
// renews again. The partition test is the negative control: no quorum, so
// the isolated node fences and never defers.
func TestNoFenceWhileLeaderless(t *testing.T) {
	ttl := 1500 * time.Millisecond
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	var others []*tnode
	for _, name := range c.names {
		if n := c.nodes[name]; n != lead {
			others = append(others, n)
		}
	}
	keep, stopped := others[0], others[1]
	if _, err := acquire(t, keep, "x", nil, 60, true); err != nil {
		t.Fatal(err)
	}
	// Two cores stop; their transports stay up, so keep sees a quorum of
	// links and no leader.
	lead.svc.Stop()
	lead.core.Stop()
	stopped.svc.Stop()
	stopped.core.Stop()
	waitFor(t, "a deferral", 2*ttl+2*time.Second, func() bool { return keep.svc.LivenessStats().FencesDeferred > 0 })
	time.Sleep(ttl)
	if keep.sessions.fenceCount() != 0 {
		t.Fatalf("fenced while the group had no leader and a reachable quorum: %+v", keep.svc.LivenessStats())
	}
	if _, ok := keep.svc.Table().Lookup("default", "x"); !ok {
		t.Fatal("the lease was lost while leaderless")
	}
	// One core returns: a leader is elected, keep renews.
	c.startNode(stopped, "")
	before := keep.svc.LivenessStats().RenewOK
	waitFor(t, "renewals after the election", 20*time.Second, func() bool { return keep.svc.LivenessStats().RenewOK > before && !keep.svc.Fenced() })
	if keep.sessions.fenceCount() != 0 {
		t.Fatalf("fenced after the election: %+v", keep.svc.LivenessStats())
	}
}

// unheardFor makes s a node that has watched the log for d and last applied
// an alive record from node that long ago.
func unheardFor(s *Service, node string, d time.Duration) {
	at := s.steady().Add(-d).UnixNano()
	s.observingSince.Store(at)
	s.heard.Store(node, at)
}

// TestFreshLeaderWaitsATtlBeforeAdopting: a reachable owner's stale lease
// is not adopted until this node has led for one ttl, whatever the
// record's age; at the boundary it is; and an owner whose link is down is
// adopted inside the grace. The flush loop is not started, so proposals
// sit in the pending batch where the test can count them.
func TestFreshLeaderWaitsATtlBeforeAdopting(t *testing.T) {
	ttl := 1500 * time.Millisecond
	now := time.Now()
	tr := &fakeTransport{up: []string{"dead"}}
	s, err := New(Options{Node: "me", Core: &fakeCore{role: "leader"}, Machine: consensus.NewMachine(), Transport: tr, Sessions: newFakeSessions(), TTL: ttl, Logger: quiet, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	// The dead owner owns a lease and was last heard from two ttls ago.
	if _, err := s.table.Apply(1, Encode([]Op{
		{Kind: OpAlive, Node: "dead", Now: now.Add(-2 * ttl)},
		{Kind: OpAcquire, Namespace: "default", ClientID: "x", Node: "dead", Now: now.Add(-2 * ttl)},
	})); err != nil {
		t.Fatal(err)
	}
	unheardFor(s, "dead", 2*ttl)
	pending := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.pending)
	}
	s.leaderSince.Store(now.Add(-ttl + time.Millisecond).UnixNano()) // led for just under a ttl
	s.adopt()
	if n := pending(); n != 0 {
		t.Fatalf("adopted %d leases inside the leader grace", n)
	}
	// The owner's link resets for a moment: still reachable, still deferred.
	tr.up, tr.flapping = nil, []string{"dead"}
	s.adopt()
	if n := pending(); n != 0 {
		t.Fatalf("adopted %d leases of an owner whose link just reset, inside the grace", n)
	}
	// The owner's link has been down for long: nothing it could renew with, adopted now.
	tr.flapping, tr.down = nil, []string{"dead"}
	s.adopt()
	if n := pending(); n != 1 {
		t.Fatalf("adopted %d leases of an unreachable owner inside the grace (want 1)", n)
	}
	s.mu.Lock()
	s.pending = nil
	s.mu.Unlock()
	tr.up, tr.down = []string{"dead"}, nil
	s.leaderSince.Store(now.Add(-ttl).UnixNano()) // led for exactly a ttl: the control
	s.adopt()
	if n := pending(); n != 1 {
		t.Fatalf("adopted %d leases after the grace (want 1)", n)
	}
	if s.pending[0].op.Kind != OpReassign || s.pending[0].op.Node != "dead" {
		t.Fatalf("proposed %+v", s.pending[0].op)
	}
}

// TestNewLeaderAdoptsAfterItsGrace: in a live cluster, when the leader's
// core and lease service stop but its transport stays up — reachable and
// silent, the storm's shape — its successor adopts the stale lease no
// sooner than one ttl after it took the lead, and does adopt it.
func TestNewLeaderAdoptsAfterItsGrace(t *testing.T) {
	ttl := 1500 * time.Millisecond
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	if _, err := acquire(t, lead, "x", nil, 60, true); err != nil {
		t.Fatal(err)
	}
	lead.svc.Stop()
	lead.core.Stop()
	var next *tnode
	waitFor(t, "a new leader", 10*time.Second, func() bool {
		for _, n := range c.nodes {
			if n != lead && n.core.Status().Role == "leader" {
				next = n
				return true
			}
		}
		return false
	})
	waitFor(t, "leader-since recorded", 2*time.Second, func() bool { return next.svc.LivenessStats().LeaderSinceMs > 0 })
	ledAt := time.Now().Add(-time.Duration(next.svc.LivenessStats().LeaderSinceMs * float64(time.Millisecond)))
	waitFor(t, "adoption", 3*ttl+5*time.Second, func() bool {
		l, ok := next.svc.Table().Lookup("default", "x")
		return ok && l.Owner == next.name
	})
	if led := time.Since(ledAt); led < ttl {
		t.Fatalf("adopted %s after taking the lead, inside the ttl %s", led, ttl)
	}
}

// TestTakeoverAttributionCounts: the previous owner counts a takeover whose
// requester was accepted after the owner's live connection, and separately
// one whose CONNECT was prebuffered; the takeover itself is served as before.
func TestTakeoverAttributionCounts(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 5*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	take := func(id string, liveAge, reqAge time.Duration, prebuffered bool) {
		t.Helper()
		if _, err := acquire(t, b, id, nil, 300, true); err != nil {
			t.Fatal(err)
		}
		b.sessions.put("default", id, &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}})
		b.sessions.setConnAge("default", id, liveAge)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := a.svc.AcquireFrom(ctx, "default", id, nil, 300, false, Requester{ConnAge: reqAge, Prebuffered: prebuffered})
		if err != nil || res.State == nil || res.PrevOwner != b.name {
			t.Fatalf("takeover of %s: %+v %v", id, res, err)
		}
	}
	take("stale", 5*time.Second, time.Second, true) // live older, prebuffered: stale
	take("dup", 5*time.Second, time.Second, false)  // live older, fresh CONNECT: a second client
	take("moved", time.Second, 5*time.Second, true) // requester older: the client moved
	take("unattributed", 5*time.Second, 0, false)   // Requester{} is not attributed
	waitFor(t, "counts", 2*time.Second, func() bool {
		st := b.svc.LivenessStats()
		return st.OlderTakeovers == 2 && st.StaleTakeovers == 1
	})
}

func TestAdmission(t *testing.T) {
	for _, c := range []struct {
		recovering, belowLevel, fenced bool
		want                           admissionVerdict
	}{
		{false, false, false, admitProceed},
		{false, false, true, admitRenewFirst},
		{true, false, false, admitRefuse},
		{true, false, true, admitRefuse},
		{false, true, false, admitRefuse},
		{false, true, true, admitRefuse},
		{true, true, false, admitRefuse},
	} {
		if got := admission(c.recovering, c.belowLevel, c.fenced); got != c.want {
			t.Errorf("admission(recovering=%v, belowLevel=%v, fenced=%v) = %v, want %v", c.recovering, c.belowLevel, c.fenced, got, c.want)
		}
	}
}

// TestRecoveringNodeAdmitsNobody: a restarted node's table still names it
// owner of the leases it held, whatever the cluster has done with them since.
// Until recovery ends a CONNECT for such a lease is refused and nothing is
// proposed for it; afterwards it is a local acquire as before.
func TestRecoveringNodeAdmitsNobody(t *testing.T) {
	core := &fakeCore{role: "follower", leader: "other"}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := s.table.Apply(1, Encode([]Op{
		{Kind: OpAlive, Node: "me", Now: now},
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "me", Now: now},
	})); err != nil {
		t.Fatal(err)
	}
	pending := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.pending)
	}

	core.recovering.Store(true)
	res, err := s.AcquireFrom(context.Background(), "default", "c", nil, 60, false, Requester{})
	var refusal *Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("admitted while recovering: result %+v, err %v", res, err)
	}
	if n, calls := pending(), core.calls.Load(); n != 0 || calls != 0 {
		t.Fatalf("a refused CONNECT left %d operations pending and made %d proposals", n, calls)
	}

	core.recovering.Store(false)
	res, err = s.AcquireFrom(context.Background(), "default", "c", nil, 60, false, Requester{})
	if err != nil || !res.Local {
		t.Fatalf("after recovery: result %+v, err %v, want a local acquire", res, err)
	}
}

// TestLocalAcquireRenewsAtTheEpochItSaw: the acquire proposed in the
// background after a local admit names the epoch the node read, so the table
// can tell it from a takeover when it arrives late.
func TestLocalAcquireRenewsAtTheEpochItSaw(t *testing.T) {
	s, err := New(Options{Node: "me", Core: &fakeCore{role: "follower", leader: "other"}, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	// Two commands: one carries one node's operations, and a command granting
	// a client to two nodes is refused.
	if _, err := s.table.Apply(1, Encode([]Op{
		{Kind: OpAlive, Node: "me", Now: now},
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "other", Now: now},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.table.Apply(2, Encode([]Op{{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "me", Now: now}})); err != nil {
		t.Fatal(err)
	}
	res, err := s.AcquireFrom(context.Background(), "default", "c", nil, 60, false, Requester{})
	if err != nil || !res.Local || res.Epoch != 2 {
		t.Fatalf("result %+v, err %v, want a local acquire at epoch 2", res, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) != 1 || s.pending[0].op.Kind != OpAcquire || s.pending[0].op.Epoch != 2 {
		t.Fatalf("pending %+v, want one acquire at epoch 2", s.pending)
	}
}

// TestLateBackgroundRenewalDoesNotTakeTheLeaseBack: a node cut off from the
// cluster goes on admitting its clients until it fences, each admit queueing
// a renewal that is retried until a leader answers. The leader reassigns the
// lease; the partition heals; the renewals arrive. The lease must stay where
// the leader put it: the node it went to holds the session now, and nobody
// would fetch it back.
func TestLateBackgroundRenewalDoesNotTakeTheLeaseBack(t *testing.T) {
	ttl := 2 * time.Second
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	v := c.follower(lead)
	if _, err := acquire(t, v, "c", nil, 60, true); err != nil {
		t.Fatal(err)
	}
	current := func() Lease {
		l, _ := lead.svc.table.Lookup("default", "c")
		return l
	}
	isolate := func(on bool) {
		for _, n := range c.nodes {
			if n != v {
				n.tr.Isolate(v.name, on)
				v.tr.Isolate(n.name, on)
			}
		}
	}
	// The acquire was answered from the leader's apply. The node must have
	// applied it too before it is cut off, or it never learns it holds the
	// lease and admits nobody.
	waitFor(t, "the owner to see its own lease", 5*time.Second, func() bool {
		l, ok := v.svc.table.Lookup("default", "c")
		return ok && l.Owner == v.name
	})
	isolate(true)
	admitted := 0
	waitFor(t, "the leader to reassign the cut-off node's lease", 4*ttl, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if res, err := v.svc.AcquireFrom(ctx, "default", "c", nil, 60, false, Requester{}); err == nil && res.Local {
			admitted++
		}
		return current().Owner != v.name
	})
	if admitted == 0 {
		l, ok := v.svc.table.Lookup("default", "c")
		t.Fatalf("the cut-off node admitted nobody, so no renewal was queued and nothing was tested (its own table: %+v %v, fenced %v)", l, ok, v.svc.Fenced())
	}
	moved := current()
	isolate(false)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if l := current(); l.Owner != moved.Owner || l.Epoch != moved.Epoch {
			t.Fatalf("after the heal the lease went from %s at epoch %d to %s at epoch %d (%d local admits were queued)", moved.Owner, moved.Epoch, l.Owner, l.Epoch, admitted)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTakeoverFromAnOwnerWithNoSessionDoesNotWaitForItsLease: a previous
// owner that answers the pull with no session — its adoption imported
// nothing — is holding nothing back, so the new owner goes straight to the
// replica fallback with its connect budget intact instead of waiting for
// the owner's lease to lapse.
func TestTakeoverFromAnOwnerWithNoSessionDoesNotWaitForItsLease(t *testing.T) {
	var mu sync.Mutex
	calls, live := 0, false
	fixtureFallback = func(ctx context.Context, ns, id, _ string) (*store.SessionState, bool) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		live = ctx.Err() == nil
		return &store.SessionState{SessionMeta: store.SessionMeta{Namespace: ns, ClientID: id}, Subscriptions: []store.SubscriptionRecord{{Filter: "t/x", QoS: 1}}}, true
	}
	t.Cleanup(func() { fixtureFallback = nil })
	ttl := 2 * time.Second
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	// The lease is n1's; n1's sessions hold nothing for x.
	if _, err := acquire(t, a, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	res, err := acquire(t, b, "x", nil, 300, false)
	took := time.Since(began)
	if err != nil || res.PrevOwner != "n1" || res.State == nil || len(res.State.Subscriptions) != 1 {
		t.Fatalf("takeover: %+v %v", res, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 || !live {
		t.Fatalf("fallback called %d times, context live %v; want once with a live context", calls, live)
	}
	if took > ttl/4 {
		t.Fatalf("the takeover took %s: it waited for the previous owner's lease although the owner answered", took)
	}
}

func TestCommandKindForwardable(t *testing.T) {
	for kind, want := range map[consensus.Kind]bool{
		consensus.KindConfig: false, consensus.KindEpoch: false, Kind: false,
		consensus.KindReserved + 1: true, consensus.KindReserved + 2: true,
	} {
		if got := CommandKindForwardable(kind); got != want {
			t.Errorf("kind %d: %v", kind, got)
		}
	}
}

// sentTransport records what the service sends.
type sentTransport struct {
	fakeTransport
	mu   sync.Mutex
	sent [][]byte
}

func (s *sentTransport) Send(_ context.Context, _ string, _ transport.Class, payload []byte) error {
	s.mu.Lock()
	s.sent = append(s.sent, append([]byte(nil), payload...))
	s.mu.Unlock()
	return nil
}

// The leader proposes a forwarded command only of a kind the lane may carry:
// a peer's command of the lease table's kind is answered with an error and
// never proposed.
func TestServeCommandRefusesAKind(t *testing.T) {
	core := &fakeCore{role: "leader"}
	tr := &sentTransport{}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Transport: tr, Sessions: newFakeSessions(), TTL: time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	s.serveCommand("n2", commandMsg{Req: 1, Kind: Kind, Payload: []byte("[]")}, transport.LeaseNotification)
	s.serveCommand("n2", commandMsg{Req: 2, Kind: consensus.KindReserved + 2, Payload: []byte("{}")}, transport.LeaseNotification)
	if n := core.calls.Load(); n != 1 {
		t.Fatalf("%d proposals, want 1 (the records kind's only)", n)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var first, second forwardResult
	if len(tr.sent) != 2 || json.Unmarshal(tr.sent[0][1:], &first) != nil || json.Unmarshal(tr.sent[1][1:], &second) != nil {
		t.Fatalf("sent %q", tr.sent)
	}
	if first.Req != 1 || !strings.Contains(first.Error, "not forwarded") || second.Req != 2 || second.Error != "" || len(second.Raw) == 0 {
		t.Fatalf("answers %+v and %+v", first, second)
	}
}

// A command whose context has already ended is not proposed: a leader's core
// would otherwise choose at random between taking the proposal and the
// ended context.
func TestProposeCommandWithAnEndedContext(t *testing.T) {
	core := &fakeCore{role: "leader"}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ProposeCommand(ctx, consensus.KindReserved+2, []byte("{}")); !errors.Is(err, context.Canceled) {
		t.Fatalf("an ended context: %v", err)
	}
	if n := core.calls.Load(); n != 0 {
		t.Fatalf("%d proposals made for an ended context", n)
	}
	if _, err := s.ProposeCommand(context.Background(), consensus.KindReserved+2, []byte("{}")); err != nil || core.calls.Load() != 1 {
		t.Fatalf("a live context: %v, %d proposals", err, core.calls.Load())
	}
}
