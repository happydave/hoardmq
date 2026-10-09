package etcdraft

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

func (c *tcluster) members(n *tnode) int { return len(n.core.Members()) }

func (c *tcluster) voters(n *tnode) int {
	k := 0
	for _, m := range n.core.Members() {
		if m.Role == consensus.Voter {
			k++
		}
	}
	return k
}

// TestLearnerJoinsAndIsPromoted: a fourth node whose name everyone knows
// starts empty, asks to join, is added as a learner, catches up and is
// promoted to voter on every node's view.
func TestLearnerJoinsAndIsPromoted(t *testing.T) {
	names := []string{"n1", "n2", "n3", "n4"}
	c := newTCluster(t, nil)
	for _, name := range names {
		c.addTransport(name, others(name, names))
	}
	for _, name := range names[:3] {
		c.startCore(c.nodes[name], names, nil)
	}
	lead := c.settled(3)
	setConfig(t, lead, "before", "join")
	// n4 starts with an empty log and joins.
	c.startCore(c.nodes["n4"], names, nil)
	n4 := c.nodes["n4"]
	waitFor(t, "n4 becomes a voter everywhere", 15*time.Second, func() bool {
		for _, n := range c.nodes {
			m, ok := n.core.members.ByName("n4")
			if !ok || m.Role != consensus.Voter {
				return false
			}
		}
		return true
	})
	waitFor(t, "n4 has the state", 5*time.Second, func() bool {
		v, ok := n4.core.machine.ConfigValue("before")
		return ok && v == "join" && n4.core.Ready()
	})
	if n4.core.Status().Bootstrapped {
		t.Fatal("the joiner bootstrapped")
	}
	if c.voters(lead) != 4 {
		t.Fatalf("voters %d", c.voters(lead))
	}
}

// TestAddMemberAPI adds a node the peers did not know about through the
// leader's AddMember, which makes every node dial it, and removes one.
func TestAddMemberAPI(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := full(t, names, nil)
	lead := c.settled(3)
	// The transports of n1..n3 do not know n4 yet; AddMember carries its
	// address into the membership, which is pushed to the transports.
	n4 := c.addTransport("n4", names)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := lead.core.AddMember(ctx, "n4", c.addrs["n4"]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "n4 known as a learner", 5*time.Second, func() bool {
		m, ok := lead.core.members.ByName("n4")
		return ok && m.Role == consensus.Learner
	})
	c.startCore(n4, names, nil)
	waitFor(t, "n4 promoted", 15*time.Second, func() bool {
		m, ok := lead.core.members.ByName("n4")
		return ok && m.Role == consensus.Voter
	})
	// Remove n3.
	if err := lead.core.RemoveMember(ctx, "n3"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "n3 gone from the leader's view", 5*time.Second, func() bool { return c.members(lead) == 3 })
	waitFor(t, "n3 knows it was removed", 5*time.Second, func() bool { return c.nodes["n3"].core.Status().Role == "removed" })
	if _, err := c.nodes["n3"].core.Propose(context.Background(), consensus.KindEpoch, nil); err == nil {
		t.Fatal("removed node accepted a proposal")
	}
	// The remaining three still serve.
	setConfig(t, c.leader(), "after", "removal")
}

// countingMetrics sums a counter by name; the labels these counters carry
// are not what any test here asks about.
type countingMetrics struct {
	mu     sync.Mutex
	v      map[string]float64
	gauges map[string]float64
	// peak is each gauge's highest value set.
	peak map[string]float64
}

func (m *countingMetrics) Add(name string, _ map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[name] += delta
}

func (m *countingMetrics) Set(name string, _ map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gauges == nil {
		m.gauges, m.peak = map[string]float64{}, map[string]float64{}
	}
	m.gauges[name] = value
	m.peak[name] = max(m.peak[name], value)
}

func (m *countingMetrics) gauge(name string) (value, peak float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gauges[name], m.peak[name]
}

// warnings counts the warnings logged whose message contains match.
type warnings struct {
	match string
	n     atomic.Int64
}

func (w *warnings) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (w *warnings) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, w.match) {
		w.n.Add(1)
	}
	return nil
}

func (w *warnings) WithAttrs([]slog.Attr) slog.Handler { return w }
func (w *warnings) WithGroup(string) slog.Handler      { return w }

func (m *countingMetrics) get(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[name]
}

// TestAWipedNodeWaitsForAdmissionWhileAGroupExists is WI 1504's defect, made
// deterministic. The peers answer and name a leader, their admission is
// swallowed, and the join window passes in between. A bootstrap here is a
// second group — which is what happened 1 run in 40 under a loaded gate,
// where the answers were merely slow.
func TestAWipedNodeWaitsForAdmissionWhileAGroupExists(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := full(t, names, func(o *Options) { o.JoinTimeout = 2 * time.Second })
	lead := c.settled(3)
	setConfig(t, lead, "k", "v")
	n1 := c.nodes["n1"]

	// Redirects still arrive, so the joiner learns a group exists; nothing
	// admits it.
	for _, n := range c.nodes {
		n.dropAccepts.Store(true)
	}
	n1.core.Stop()
	if err := os.RemoveAll(n1.dir); err != nil {
		t.Fatal(err)
	}
	metrics := &countingMetrics{}
	warned := &warnings{match: "waiting to be admitted"}
	c.startCore(n1, names, func(o *Options) {
		o.JoinTimeout = 200 * time.Millisecond
		o.JoinWarnEvery = 200 * time.Millisecond
		o.Metrics = metrics
		o.Logger = slog.New(warned)
	})

	// The window passing is the event, not a duration this test waits out.
	// Either outcome ends the wait — the counter the node writes when it
	// decides to wait, or the bootstrap it must not do — so the assertion
	// that trips is the one about the behaviour and not a timeout.
	waitFor(t, "n1's join window to pass", 10*time.Second, func() bool {
		return metrics.get("hoardmq_raft_join_window_passed_total") >= 1 || n1.core.Status().Bootstrapped
	})
	if st := n1.core.Status(); st.Bootstrapped {
		t.Fatalf("a wiped node bootstrapped a second group while its peers were naming a leader: %+v", st)
	}
	// The wait is visible while it lasts: the warning repeats and the gauge
	// counts the time waited (WI 1735).
	waitFor(t, "the waiting warning to repeat", 10*time.Second, func() bool { return warned.n.Load() >= 2 })
	if v, _ := metrics.gauge("hoardmq_raft_join_wait_seconds"); v <= 0 {
		t.Fatalf("the join wait gauge reads %v while n1 waits", v)
	}
	if n := metrics.get("hoardmq_raft_join_window_passed_total"); n != 1 {
		t.Fatalf("the window-passed counter is %v, want 1", n)
	}

	// And it joins as soon as an admission is allowed through, so waiting is
	// not a wedge.
	for _, n := range c.nodes {
		n.dropAccepts.Store(false)
	}
	waitFor(t, "n1 admitted once an answer gets through", 15*time.Second, func() bool {
		v, ok := n1.core.machine.ConfigValue("k")
		return ok && v == "v" && n1.core.Ready()
	})
	if st := n1.core.Status(); st.Bootstrapped {
		t.Fatalf("n1 bootstrapped after all: %+v", st)
	}
	if v, peak := metrics.gauge("hoardmq_raft_join_wait_seconds"); v != 0 || peak <= 0 {
		t.Fatalf("the join wait gauge reads %v once admitted (peak %v), want 0", v, peak)
	}
	for _, n := range c.nodes {
		if c.members(n) != 3 {
			t.Fatalf("%s sees %d members after the rejoin", n.name, c.members(n))
		}
	}
}

// TestAWipedNodeWaitsWhenOnlyTheLeaderAnswers is the same defect where the
// leader's own answer is the only evidence a group exists — a two-node group,
// or one whose followers cannot be heard. A follower says who the leader is
// because it is redirecting; the leader has to say so itself, and before WI
// 1504 it did not.
func TestAWipedNodeWaitsWhenOnlyTheLeaderAnswers(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := full(t, names, func(o *Options) { o.JoinTimeout = 2 * time.Second })
	setConfig(t, c.settled(3), "k", "v")
	n1 := c.nodes["n1"]

	// n1 is the designated node, so it is the one that bootstraps and the one
	// that must not; the leadership has to be elsewhere for the leader to be
	// the peer answering it, which is what stopping n1 produces.
	n1.core.Stop()
	if err := os.RemoveAll(n1.dir); err != nil {
		t.Fatal(err)
	}
	var lead *tnode
	waitFor(t, "the survivors to elect a leader", 15*time.Second, func() bool {
		for _, name := range []string{"n2", "n3"} {
			if c.nodes[name].core.Status().Role == "leader" {
				lead = c.nodes[name]
				return true
			}
		}
		return false
	})

	// Only the leader answers at all, and its admission is swallowed.
	for _, n := range c.nodes {
		if n != lead {
			n.dropJoinAnswers.Store(true)
		}
	}
	lead.dropAccepts.Store(true)
	metrics := &countingMetrics{}
	c.startCore(n1, names, func(o *Options) {
		o.JoinTimeout = 200 * time.Millisecond
		o.Metrics = metrics
	})
	waitFor(t, "n1's join window to pass", 10*time.Second, func() bool {
		return metrics.get("hoardmq_raft_join_window_passed_total") >= 1 || n1.core.Status().Bootstrapped
	})
	if st := n1.core.Status(); st.Bootstrapped {
		t.Fatalf("a wiped node bootstrapped a second group while the leader was answering it: %+v", st)
	}

	lead.dropAccepts.Store(false)
	for _, n := range c.nodes {
		n.dropJoinAnswers.Store(false)
	}
	waitFor(t, "n1 admitted once an answer gets through", 15*time.Second, func() bool {
		v, ok := n1.core.machine.ConfigValue("k")
		return ok && v == "v" && n1.core.Ready()
	})
	if st := n1.core.Status(); st.Bootstrapped {
		t.Fatalf("n1 bootstrapped after all: %+v", st)
	}
}

// TestRestartRejoinsRatherThanRebootstrapping covers the ordinal-0 failure
// mode: the designated (lowest-named) node restarts with its data, then with
// its data wiped, and in neither case starts a second group.
func TestRestartRejoinsRatherThanRebootstrapping(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := full(t, names, func(o *Options) { o.JoinTimeout = 2 * time.Second })
	lead := c.settled(3)
	setConfig(t, lead, "k", "v")
	n1 := c.nodes["n1"]
	termBefore := n1.core.Status().Term

	// Restart with data intact.
	n1.core.Stop()
	c.startCore(n1, names, nil)
	waitFor(t, "n1 back with its state", 10*time.Second, func() bool {
		v, ok := n1.core.machine.ConfigValue("k")
		return ok && v == "v" && n1.core.Ready()
	})
	st := n1.core.Status()
	if st.Bootstrapped || st.Term < termBefore || c.members(n1) != 3 {
		t.Fatalf("restart with data: %+v members %d", st, c.members(n1))
	}

	// Restart with data wiped while the others run: it must join, not
	// bootstrap, even though it is the designated node and the join window
	// is short.
	n1.core.Stop()
	if err := os.RemoveAll(n1.dir); err != nil {
		t.Fatal(err)
	}
	quiet.Info("test: n1 wiped, restarting")
	metrics := &countingMetrics{}
	c.startCore(n1, names, func(o *Options) { o.Metrics = metrics })
	waitFor(t, "wiped n1 rejoins", 15*time.Second, func() bool {
		v, ok := n1.core.machine.ConfigValue("k")
		return ok && v == "v" && n1.core.Ready()
	})
	st = n1.core.Status()
	// A failure here carries which of the two cases it is: the window passing
	// with a group known is the condition WI 1504's rule acts on, and a
	// bootstrap after a window that named no leader is the case it does not
	// cover — nothing answered at all.
	waited, booted := metrics.get("hoardmq_raft_join_window_passed_total"), metrics.get("hoardmq_raft_bootstrapped_after_window_total")
	t.Logf("join window passed with a group known %v time(s); bootstrapped after the window %v time(s)", waited, booted)
	if st.Bootstrapped {
		t.Fatalf("wiped node bootstrapped a second group: %+v (window passed with a group known %v, bootstrapped after the window %v)", st, waited, booted)
	}
	for _, n := range c.nodes {
		if c.members(n) != 3 {
			t.Fatalf("%s sees %d members after the rejoin", n.name, c.members(n))
		}
	}
	// One leader across the cluster.
	c.leader()
}

// TestLoneDesignatedNodeBootstrapsAfterTheWindow: with no peer answering,
// only the designated node bootstraps, and only after the join window.
func TestLoneDesignatedNodeBootstrapsAfterTheWindow(t *testing.T) {
	c := newTCluster(t, nil)
	c.addTransport("n1", []string{"n2"})
	c.addTransport("n2", []string{"n1"})
	// Neither transport can reach the other: give each a dead address.
	c.addrs["n1"], c.addrs["n2"] = "127.0.0.1:1", "127.0.0.1:1"
	c.startCore(c.nodes["n1"], []string{"n1", "n2"}, func(o *Options) { o.JoinTimeout = 300 * time.Millisecond })
	c.startCore(c.nodes["n2"], []string{"n1", "n2"}, func(o *Options) { o.JoinTimeout = 300 * time.Millisecond })
	if c.nodes["n1"].core.Status().Bootstrapped {
		t.Fatal("bootstrapped before the window")
	}
	waitFor(t, "designated node bootstraps", 5*time.Second, func() bool { return c.nodes["n1"].core.Status().Bootstrapped })
	time.Sleep(500 * time.Millisecond)
	if st := c.nodes["n2"].core.Status(); st.Bootstrapped || st.Role != "joining" {
		t.Fatalf("non-designated node: %+v", st)
	}
}

// TestPersistBeforeSend holds the leader's sync on a barrier once a
// proposal is in flight. Log entries must leave only after that sync: any
// entry-carrying message recorded while the store's durable sequence is
// still the pre-sync one is a violation. The send-before-save seam makes
// the same check fail, which shows the check can fail.
func TestPersistBeforeSend(t *testing.T) {
	run := func(t *testing.T, seam bool) (violations int) {
		names := []string{"n1", "n2", "n3"}
		c := full(t, names, func(o *Options) { o.Tick = 20 * time.Millisecond })
		lead := c.settled(3)
		lead.core.mu.Lock()
		lead.core.sendBeforeSave = seam
		lead.core.mu.Unlock()
		// Let replication settle so the followers are in the replicating
		// state and the proposal's batch carries its append messages.
		time.Sleep(time.Second)
		barrier := make(chan struct{})
		armed := make(chan struct{}, 1)
		lead.mu.Lock()
		lead.sync = func(f *os.File) error {
			select {
			case armed <- struct{}{}:
				<-barrier
			default:
			}
			return f.Sync()
		}
		before := len(lead.sends)
		lead.mu.Unlock()
		preSync := lead.store.LastDurable().Seq()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = lead.core.Propose(ctx, consensus.KindConfig, consensus.ConfigCommand("pb", "s")[1:])
		}()
		select {
		case <-armed:
		case <-time.After(5 * time.Second):
			t.Fatal("the sync barrier was never reached")
		}
		// The loop is inside Save for the proposal's batch. Entries sent so
		// far with the pre-sync sequence were sent before they were durable.
		time.Sleep(100 * time.Millisecond)
		violations = lead.entrySendsAtOrBelow(before, preSync)
		close(barrier)
		return violations
	}
	if v := run(t, false); v != 0 {
		t.Fatalf("%d entry messages were sent before their batch was durable", v)
	}
	if v := run(t, true); v == 0 {
		t.Fatal("the check did not detect the send-before-save seam")
	}
}

// TestWipedNodeHasNoRaftNodeWhileJoining pins the first half of work item
// 1350: while a wiped node's join is outstanding it has no raft node at
// all, so there is nothing for a vote request to be stepped into. Start
// takes the join branch for an empty store with peers, and the node loop
// waits on the started channel, which only closes when the leader admits
// the joiner. The test exists so a later change to Start cannot silently
// open a window in which a node with no recorded vote can grant one.
func TestWipedNodeHasNoRaftNodeWhileJoining(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	c := full(t, names, nil)
	lead := c.settled(3)
	setConfig(t, lead, "k", "v")

	// Wipe a follower that is neither the leader nor the designated node.
	// Not the leader, so the group keeps its quorum and the joiner's
	// disposition is decided by a leader that is already up; not the
	// designated node ("n1", the lowest name), so the join window expiring
	// cannot turn the observation into a bootstrap.
	var victim *tnode
	for _, name := range names[1:] {
		if n := c.nodes[name]; n != lead {
			victim = n
			break
		}
	}
	if victim == nil {
		t.Fatal("no non-leader, non-designated node to wipe")
	}
	victim.core.Stop()
	if err := os.RemoveAll(victim.dir); err != nil {
		t.Fatal(err)
	}
	quiet.Info("test: wiped a follower, restarting it")
	c.startCore(victim, names, nil)

	waitFor(t, "the wiped node reports itself joining", 10*time.Second, func() bool {
		return victim.core.Status().Role == "joining"
	})
	// While joining it holds no raft state: no term, and so no vote it
	// could have granted or could grant.
	st := victim.core.Status()
	if st.Term != 0 || st.Bootstrapped || st.Joined {
		t.Fatalf("a joining node holds raft state: %+v", st)
	}
	// And it converges as before, so the assertion above is about a real
	// transient rather than a node that never joins.
	waitFor(t, "the wiped node rejoins with the state", 30*time.Second, func() bool {
		v, ok := victim.core.machine.ConfigValue("k")
		return ok && v == "v" && victim.core.Ready()
	})
	if victim.core.Status().Bootstrapped {
		t.Fatal("the wiped node bootstrapped a second group")
	}
}
