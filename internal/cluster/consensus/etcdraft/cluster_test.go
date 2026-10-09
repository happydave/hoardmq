package etcdraft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const secret = "cluster-test-secret-of-enough-bytes"

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func init() {
	// HOARDMQ_TEST_DEBUG=1 turns the node logs on for a failing run.
	if os.Getenv("HOARDMQ_TEST_DEBUG") != "" {
		quiet = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
}

// tnode is one in-process node: transport, store, machine and core.
type tnode struct {
	name  string
	dir   string
	tr    *transport.Transport
	store *raftlog.Store
	core  *Core
	sync  func(*os.File) error // WAL sync seam, replaceable per test
	// dropAccepts swallows this node's *accepting* join answers, leaving its
	// redirects alone. It is how a test holds a joiner in the state WI 1504
	// was filed for: a group exists and says so, and the admission is late.
	// dropJoinAnswers swallows every join answer, which is how a test makes
	// one peer the only one that answers at all.
	dropAccepts     atomic.Bool
	dropJoinAnswers atomic.Bool
	mu              sync.Mutex
	sends           []sendRecord // what left this node and how durable the store was
}

// sendRecord is one Send seen by the recording transport: the store's
// durable sequence at that moment and whether the frame carried log entries
// (a MsgApp with entries), which is the kind that must follow a sync.
type sendRecord struct {
	seq     uint64
	entries bool
}

// tcluster holds nodes by name and their transport addresses.
type tcluster struct {
	t     *testing.T
	dir   string
	nodes map[string]*tnode
	addrs map[string]string
	tweak func(o *Options)
}

func newTCluster(t *testing.T, tweak func(o *Options)) *tcluster {
	return &tcluster{t: t, dir: t.TempDir(), nodes: map[string]*tnode{}, addrs: map[string]string{}, tweak: tweak}
}

// addTransport starts a transport for name so its address is known before
// any core starts. peers are the names this node will know.
func (c *tcluster) addTransport(name string, peers []string) *tnode {
	c.t.Helper()
	n := &tnode{name: name, dir: filepath.Join(c.dir, name)}
	n.sync = func(f *os.File) error { return f.Sync() }
	var ps []config.Peer
	for _, p := range peers {
		ps = append(ps, config.Peer{Name: p, Address: "127.0.0.1:1"})
	}
	tr, err := transport.New(transport.Options{Node: name, ClusterID: "t", Listen: "127.0.0.1:0", Peers: ps, Auth: transport.Auth{Mode: "secret", Secret: []byte(secret)},
		Heartbeat: 50 * time.Millisecond, HeartbeatMisses: 4, HandshakeTimeout: 2 * time.Second, Logger: quiet})
	if err != nil {
		c.t.Fatal(err)
	}
	if err := tr.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(tr.Stop)
	n.tr = tr
	c.nodes[name] = n
	c.addrs[name] = tr.Addr().String()
	return n
}

// peersFor builds the peer list with real addresses.
func (c *tcluster) peersFor(name string, names []string) []config.Peer {
	var ps []config.Peer
	for _, p := range names {
		if p != name {
			ps = append(ps, config.Peer{Name: p, Address: c.addrs[p]})
		}
	}
	return ps
}

// startCore opens the store and starts a core for a node whose transport is
// already up; peers is the list it is configured with.
func (c *tcluster) startCore(n *tnode, peers []string, extra func(o *Options)) {
	c.t.Helper()
	store, err := raftlog.Open(n.dir, "raft", wal.Options{Sync: func(f *os.File) error { return n.syncFn()(f) }, Logger: quiet})
	if err != nil {
		c.t.Fatal(err)
	}
	n.store = store
	o := Options{Node: n.name, Peers: c.peersFor(n.name, peers), Store: store, Transport: &recordingTransport{n: n}, Machine: consensus.NewMachine(),
		Tick: 10 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2, SnapshotEntries: 1000, SnapshotTrailing: 10, PromoteLag: 50, JoinTimeout: 2 * time.Second, Logger: quiet}
	if c.tweak != nil {
		c.tweak(&o)
	}
	if extra != nil {
		extra(&o)
	}
	core, err := New(o)
	if err != nil {
		c.t.Fatal(err)
	}
	if err := core.Start(); err != nil {
		c.t.Fatal(err)
	}
	n.core = core
	c.t.Cleanup(func() { core.Stop() })
}

// recordingTransport wraps a node's transport and records the store's
// durable sequence at every send, for the persist-before-send test.
type recordingTransport struct{ n *tnode }

func (r *recordingTransport) Send(ctx context.Context, peer string, cl transport.Class, payload []byte) error {
	if cl == transport.Consensus && len(payload) > 1 && payload[0] == envJoinResponse {
		if r.n.dropJoinAnswers.Load() {
			return nil
		}
		var resp joinResponse
		if r.n.dropAccepts.Load() && json.Unmarshal(payload[1:], &resp) == nil && resp.Accepted {
			return nil
		}
	}
	if r.n.store != nil {
		rec := sendRecord{seq: r.n.store.LastDurable().Seq()}
		if cl == transport.Consensus && len(payload) > 1 && payload[0] == envRaft {
			m := &pb.Message{}
			if proto.Unmarshal(payload[1:], m) == nil && m.GetType() == pb.MsgApp && len(m.GetEntries()) > 0 {
				rec.entries = true
			}
		}
		r.n.mu.Lock()
		r.n.sends = append(r.n.sends, rec)
		r.n.mu.Unlock()
	}
	return r.n.tr.Send(ctx, peer, cl, payload)
}
func (r *recordingTransport) Handle(cl transport.Class, h transport.Handler) { r.n.tr.Handle(cl, h) }
func (r *recordingTransport) SetPeers(peers []config.Peer)                   { r.n.tr.SetPeers(peers) }
func (r *recordingTransport) Peers() []transport.PeerInfo                    { return r.n.tr.Peers() }

// syncFn reads the sync seam under the lock.
func (n *tnode) syncFn() func(*os.File) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sync
}

// entrySendsAtOrBelow counts sends of log entries, from position from on,
// made while the store's durable sequence was at or below seq.
func (n *tnode) entrySendsAtOrBelow(from int, seq uint64) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	k := 0
	for _, r := range n.sends[from:] {
		if r.entries && r.seq <= seq {
			k++
		}
	}
	return k
}

// full builds a cluster of names where everyone knows everyone, and starts
// every core.
func full(t *testing.T, names []string, tweak func(o *Options)) *tcluster {
	t.Helper()
	c := newTCluster(t, tweak)
	for _, name := range names {
		c.addTransport(name, others(name, names))
	}
	for _, name := range names {
		c.startCore(c.nodes[name], names, nil)
	}
	return c
}

func others(name string, names []string) []string {
	var out []string
	for _, n := range names {
		if n != name {
			out = append(out, n)
		}
	}
	return out
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

// leader waits for one leader that every node agrees on.
func (c *tcluster) leader() *tnode {
	c.t.Helper()
	var lead *tnode
	waitFor(c.t, "an agreed leader", 10*time.Second, func() bool {
		var name string
		for _, n := range c.nodes {
			if n.core == nil {
				continue
			}
			l, ok := n.core.Leader()
			if !ok {
				return false
			}
			if name == "" {
				name = l
			} else if name != l {
				return false
			}
		}
		lead = c.nodes[name]
		// Survivors can still name a leader that was just stopped.
		return lead != nil && lead.core != nil && lead.core.Status().Role == "leader"
	})
	return lead
}

// settled waits until the leader sees n voters, so every node can take
// part in elections before a test disturbs the cluster.
func (c *tcluster) settled(n int) *tnode {
	c.t.Helper()
	lead := c.leader()
	waitFor(c.t, fmt.Sprintf("%d voters", n), 15*time.Second, func() bool {
		k := 0
		for _, m := range lead.core.Members() {
			if m.Role == consensus.Voter {
				k++
			}
		}
		return k == n
	})
	return lead
}

func setConfig(t *testing.T, n *tnode, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := n.core.Propose(ctx, consensus.KindConfig, consensus.ConfigCommand(key, value)[1:]); err != nil {
		t.Fatalf("%s: propose: %v", n.name, err)
	}
}

// proposeOnLeader proposes through the node every node currently agrees
// leads, and proposes again through the new leader when leadership moved
// under it. At these test timings an election follows any heartbeat gap over
// 100-200 ms, which a loaded host produces (WI 1489); a refusal naming
// another leader is that and nothing else, so it alone is retried.
func proposeOnLeader(t *testing.T, c *tcluster, kind consensus.Kind, payload []byte) any {
	t.Helper()
	return proposeVia(t, c.leader, kind, payload)
}

// proposeVia is proposeOnLeader with the leader lookup given, so a test can
// hand it a node that has just lost the lead.
func proposeVia(t *testing.T, find func() *tnode, kind consensus.Kind, payload []byte) any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		lead := find()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		v, err := lead.core.Propose(ctx, kind, payload)
		cancel()
		var nl *consensus.NotLeaderError
		switch {
		case err == nil:
			return v
		case errorsAs(err, &nl) && time.Now().Before(deadline):
			t.Logf("%s refused the proposal naming %q as leader; proposing through the current leader", lead.name, nl.Leader)
		default:
			t.Fatalf("%s: propose: %v", lead.name, err)
		}
	}
}

func TestClustersElectReplicateAndSnapshot(t *testing.T) {
	for _, size := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("%d-nodes", size), func(t *testing.T) {
			var names []string
			for i := 1; i <= size; i++ {
				names = append(names, fmt.Sprintf("n%d", i))
			}
			c := full(t, names, func(o *Options) { o.SnapshotEntries = 20 })
			c.settled(size)
			proposeOnLeader(t, c, consensus.KindConfig, consensus.ConfigCommand("replicas", "3")[1:])
			for _, n := range c.nodes {
				n := n
				waitFor(t, n.name+" applies", 5*time.Second, func() bool {
					v, ok := n.core.machine.ConfigValue("replicas")
					return ok && v == "3"
				})
			}
			// A follower refuses proposals with the current leader's name. If
			// leadership moves between finding the leader and asking, the hint
			// names the new one: find it again and ask again.
			if size > 1 {
				var lastLead, lastHint string
				refused := false
				for try := 0; try < 3 && !refused; try++ {
					lead := c.leader()
					for _, n := range c.nodes {
						if n == lead {
							continue
						}
						_, err := n.core.Propose(context.Background(), consensus.KindConfig, consensus.ConfigCommand("x", "y")[1:])
						var nl *consensus.NotLeaderError
						if !errorsAs(err, &nl) {
							t.Fatalf("%s: follower proposal: %v", n.name, err)
						}
						lastLead, lastHint = lead.name, nl.Leader
						refused = nl.Leader == lead.name
						break
					}
				}
				if !refused {
					t.Fatalf("a follower's refusal named %q while every node agreed %q leads, three times", lastHint, lastLead)
				}
			}
			// Enough proposals for a snapshot, then a restart recovers from it.
			for i := 0; i < 30; i++ {
				proposeOnLeader(t, c, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("k%d", i), "v")[1:])
			}
			waitFor(t, "snapshot on the leader", 5*time.Second, func() bool { return c.leader().core.Status().SnapshotIndex > 0 })
			victim := c.nodes[names[size-1]]
			victim.core.Stop()
			c.startCore(victim, names, nil)
			waitFor(t, "restarted node recovers", 10*time.Second, func() bool {
				v, ok := victim.core.machine.ConfigValue("k29")
				return ok && v == "v" && victim.core.Ready()
			})
			if victim.core.Status().Bootstrapped {
				t.Fatal("a restart bootstrapped")
			}
			if epoch := proposeOnLeader(t, c, consensus.KindEpoch, nil); epoch.(uint64) != 1 {
				t.Fatalf("epoch: %v", epoch)
			}
		})
	}
}

// TestProposalRetriesThroughTheNewLeader: a proposal that reaches a node which
// is no longer the leader is refused with the leader's name, and proposed
// again through the leader it names (WI 1489). The first lookup returns a
// follower, which is the state after leadership moves between finding the
// leader and proposing.
func TestProposalRetriesThroughTheNewLeader(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := full(t, names, nil)
	lead := c.settled(3)
	var follower *tnode
	for _, n := range c.nodes {
		if n != lead {
			follower = n
			break
		}
	}
	calls := 0
	find := func() *tnode {
		calls++
		if calls == 1 {
			return follower
		}
		return c.leader()
	}
	proposeVia(t, find, consensus.KindConfig, consensus.ConfigCommand("moved", "yes")[1:])
	if calls < 2 {
		t.Fatalf("the proposal was not retried: %d lookups", calls)
	}
	for _, n := range c.nodes {
		n := n
		waitFor(t, n.name+" applies", 5*time.Second, func() bool {
			v, ok := n.core.machine.ConfigValue("moved")
			return ok && v == "yes"
		})
	}
}

// TestProposalsSurviveALeaderChange: proposals made through proposeOnLeader
// all commit when the leader stops midway (WI 1489).
func TestProposalsSurviveALeaderChange(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := full(t, names, nil)
	first := c.settled(3)
	for i := 0; i < 30; i++ {
		if i == 10 {
			first.core.Stop()
			first.core = nil
		}
		proposeOnLeader(t, c, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("k%d", i), "v")[1:])
	}
	for _, n := range c.nodes {
		if n.core == nil {
			continue
		}
		n := n
		waitFor(t, n.name+" applies k29", 10*time.Second, func() bool {
			v, ok := n.core.machine.ConfigValue("k29")
			return ok && v == "v"
		})
	}
}

func errorsAs(err error, target **consensus.NotLeaderError) bool {
	for err != nil {
		if e, ok := err.(*consensus.NotLeaderError); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestSingleNodePersistsAcrossRestart(t *testing.T) {
	c := newTCluster(t, nil)
	n := &tnode{name: "solo", dir: filepath.Join(c.dir, "solo")}
	n.sync = func(f *os.File) error { return f.Sync() }
	store, err := raftlog.Open(n.dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	core, err := New(Options{Node: "solo", Store: store, Machine: consensus.NewMachine(), Tick: 10 * time.Millisecond, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(); err != nil {
		t.Fatal(err)
	}
	n.core = core
	waitFor(t, "single node leads", 5*time.Second, func() bool { return core.Status().Role == "leader" && core.Ready() })
	if !core.Status().Bootstrapped {
		t.Fatal("single node did not bootstrap")
	}
	setConfig(t, n, "mode", "solo")
	core.Stop()
	store2, err := raftlog.Open(n.dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	core2, err := New(Options{Node: "solo", Store: store2, Machine: consensus.NewMachine(), Tick: 10 * time.Millisecond, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if err := core2.Start(); err != nil {
		t.Fatal(err)
	}
	defer core2.Stop()
	waitFor(t, "restarted single node", 5*time.Second, func() bool {
		v, ok := core2.machine.ConfigValue("mode")
		return ok && v == "solo" && core2.Status().Role == "leader"
	})
	if core2.Status().Bootstrapped {
		t.Fatal("restart bootstrapped again")
	}
}
