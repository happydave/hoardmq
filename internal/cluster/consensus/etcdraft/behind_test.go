package etcdraft

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
)

// gauges records each node's last gauge values and counter sums.
type gauges struct {
	mu sync.Mutex
	v  map[string]float64
}

func (g *gauges) Add(name string, _ map[string]string, delta float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.v[name] += delta
}

func (g *gauges) Set(name string, _ map[string]string, value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.v[name] = value
}

func (g *gauges) get(name string) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v[name]
}

// behindCluster builds three nodes that snapshot every 20 entries, with a
// stall timeout of 1 s and each node's metrics in metrics, settles them, and
// returns the leader and a follower.
func behindCluster(t *testing.T, tweak func(o *Options)) (*tcluster, *tnode, *tnode, map[string]*gauges) {
	t.Helper()
	names := []string{"n1", "n2", "n3"}
	metrics := map[string]*gauges{}
	for _, name := range names {
		metrics[name] = &gauges{v: map[string]float64{}}
	}
	c := full(t, names, func(o *Options) {
		o.SnapshotEntries = 20
		o.SnapshotTrailing = 10
		o.StallTimeout = time.Second
		o.Metrics = metrics[o.Node]
		if tweak != nil {
			tweak(o)
		}
	})
	c.settled(3)
	for i := 0; i < 30; i++ {
		proposeOnLeader(t, c, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("before%d", i), "v")[1:])
	}
	lead := c.leader()
	var victim *tnode
	for _, name := range names {
		if nd := c.nodes[name]; nd != lead {
			victim = nd
			break
		}
	}
	return c, lead, victim, metrics
}

// cutOff isolates victim from every other node, or ends the isolation.
func cutOff(c *tcluster, victim *tnode, on bool) {
	for name, nd := range c.nodes {
		if nd == victim {
			continue
		}
		nd.tr.Isolate(victim.name, on)
		victim.tr.Isolate(name, on)
	}
}

// holdData keeps victim's data connections down at both ends, or lets them
// come up. A raft snapshot travels on the data connection, so a follower
// held this way cannot receive one, while appends and heartbeats still reach
// it on the control connection.
func holdData(c *tcluster, victim *tnode, on bool) {
	for name, nd := range c.nodes {
		if nd == victim {
			continue
		}
		nd.tr.HoldData(victim.name, on)
		victim.tr.HoldData(name, on)
	}
}

// putBehind leaves victim a voter whose log ends below the leader's first
// index, unable to receive the snapshot it needs: cut off, its data
// connections held, the leader compacting twice past it, then the control
// connections let up. With restart the victim's core is stopped first and
// started again from its log once it can be reached.
func putBehind(t *testing.T, c *tcluster, victim *tnode, restart bool) {
	t.Helper()
	if restart {
		victim.core.Stop()
	}
	cutOff(c, victim, true)
	holdData(c, victim, true)
	t.Cleanup(func() { holdData(c, victim, false) })
	for i := 0; i < 45; i++ {
		proposeVia(t, func() *tnode { return liveLeader(t, c, victim) }, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("behind%d", i), "v")[1:])
	}
	cutOff(c, victim, false)
	if restart {
		c.startCore(victim, []string{"n1", "n2", "n3"}, nil)
	}
}

// liveLeader is the leader the nodes other than skip agree on.
func liveLeader(t *testing.T, c *tcluster, skip *tnode) *tnode {
	t.Helper()
	var lead *tnode
	waitFor(t, "a leader among the others", 10*time.Second, func() bool {
		for _, nd := range c.nodes {
			if nd != skip && nd.core != nil && nd.core.Status().Role == "leader" {
				lead = nd
				return true
			}
		}
		return false
	})
	return lead
}

// TestVoterBehindTheCompactedLogIsNotReady: a voter whose log ends below the
// leader's first index, and which cannot receive the snapshot it needs, has
// a leader and hears its heartbeats, but its applied index stays where its
// log ends. It must not report itself ready or joined, and must report so
// again once the snapshot reaches it.
func TestVoterBehindTheCompactedLogIsNotReady(t *testing.T) {
	for _, restart := range []bool{true, false} {
		name := "partitioned"
		if restart {
			name = "restarted"
		}
		t.Run(name, func(t *testing.T) {
			c, _, victim, metrics := behindCluster(t, nil)
			putBehind(t, c, victim, restart)
			lead := liveLeader(t, c, victim)
			waitFor(t, "the victim to know the leader and end any recovery", 10*time.Second, func() bool {
				l, ok := victim.core.Leader()
				return ok && l == lead.name && !victim.core.Recovering()
			})
			st, ls := victim.core.Status(), lead.core.Status()
			if st.Applied >= ls.SnapshotIndex {
				t.Fatalf("precondition not reached: the victim applied %d, at or past the leader's snapshot %d", st.Applied, ls.SnapshotIndex)
			}
			deadline := time.Now().Add(5 * time.Second)
			for victim.core.Ready() || victim.core.Status().Joined {
				if time.Now().After(deadline) {
					st, ls = victim.core.Status(), lead.core.Status()
					t.Fatalf("a voter that cannot apply past the leader's snapshot reports ready %v joined %v for 5 s: role %s applied %d; the leader's snapshot %d, applied %d",
						victim.core.Ready(), st.Joined, st.Role, st.Applied, ls.SnapshotIndex, ls.Applied)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if st := victim.core.Status(); st.Applied >= lead.core.Status().SnapshotIndex {
				t.Fatalf("precondition lost: the victim applied %d, past the leader's snapshot, while reported not ready", st.Applied)
			}
			if !victim.core.Behind() {
				t.Errorf("the victim is not ready but does not report itself behind")
			}
			gauge := metrics[victim.name]
			waitFor(t, "the stall gauge at 1 and one stall counted", 2*time.Second, func() bool {
				return gauge.get("hoardmq_raft_apply_stalled") == 1 && gauge.get("hoardmq_raft_apply_stalls_total") >= 1
			})

			holdData(c, victim, false)
			waitFor(t, "the victim, its snapshot received, to report ready", 15*time.Second, func() bool {
				return victim.core.Ready() && victim.core.Status().Joined && victim.core.Status().Applied >= lead.core.Status().SnapshotIndex
			})
			waitFor(t, "the stall gauge back at 0", 2*time.Second, func() bool { return gauge.get("hoardmq_raft_apply_stalled") == 0 })
			if n, counted := victim.core.Stalls(), gauge.get("hoardmq_raft_apply_stalls_total"); n != 1 || counted != 1 {
				t.Errorf("stalls %d, counted %v over one stuck period, want 1: the state was left and entered again", n, counted)
			}
		})
	}
}

// TestWipedMemberThatCannotReceiveItsSnapshotIsNotReady: a member restarted
// with an empty log is removed and re-added as a learner, which only a
// snapshot can fill. With every snapshot sent to it refused, it must report
// not ready and not joined throughout, and ready once one reaches it.
func TestWipedMemberThatCannotReceiveItsSnapshotIsNotReady(t *testing.T) {
	c, _, victim, _ := behindCluster(t, nil)
	refuse := func(on bool) {
		for _, nd := range c.nodes {
			if nd == victim {
				continue
			}
			if on {
				nd.refuseBulkTo.Store(&victim.name)
			} else {
				nd.refuseBulkTo.Store(nil)
			}
		}
	}
	victim.core.Stop()
	refuse(true)
	t.Cleanup(func() { refuse(false) })
	if err := os.RemoveAll(victim.dir); err != nil {
		t.Fatal(err)
	}
	c.startCore(victim, []string{"n1", "n2", "n3"}, nil)
	lead := liveLeader(t, c, victim)
	waitFor(t, "the leader to hold the wiped member as a learner", 15*time.Second, func() bool {
		for _, m := range lead.core.Members() {
			if m.Name == victim.name {
				return m.Role == consensus.Learner
			}
		}
		return false
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := victim.core.Status(); victim.core.Ready() || st.Joined {
			t.Fatalf("a wiped member that cannot receive its snapshot reports ready %v joined %v: role %s applied %d", victim.core.Ready(), st.Joined, st.Role, st.Applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
	refuse(false)
	waitFor(t, "the wiped member, its snapshot received, to report ready", 20*time.Second, func() bool {
		return victim.core.Ready() && victim.core.Status().Joined
	})
}

// TestApplyStalled: the predicate is true only behind the target, not past
// the mark, and with the mark at least the bound old.
func TestApplyStalled(t *testing.T) {
	at := time.Date(2026, 10, 22, 6, 0, 0, 0, time.UTC)
	bound := time.Second
	for _, tc := range []struct {
		name                         string
		applied, target, markApplied uint64
		age                          time.Duration
		want                         bool
	}{
		{"behind and still for the bound", 36, 81, 36, bound, true},
		{"behind and still past the bound", 36, 81, 36, 10 * bound, true},
		{"just under the bound", 36, 81, 36, bound - time.Millisecond, false},
		{"advanced past an old mark", 40, 81, 36, 10 * bound, false},
		{"at the target", 81, 81, 81, 10 * bound, false},
		{"past the target", 90, 81, 90, 10 * bound, false},
		{"no target yet", 36, 0, 36, 10 * bound, false},
	} {
		if got := applyStalled(tc.applied, tc.target, tc.markApplied, at, at.Add(tc.age), bound); got != tc.want {
			t.Errorf("%s: applyStalled(%d, %d, %d, age %v) = %v, want %v", tc.name, tc.applied, tc.target, tc.markApplied, tc.age, got, tc.want)
		}
	}
}

// TestAdvancingApplyIsNeverBehind: the loop's mark, driven tick by tick over
// a simulated 10 s with the target far ahead. An applied index that advances
// by one entry a tick is never behind; one that stops is behind once the
// bound has passed since it last moved, and no sooner; reaching the target
// clears it.
func TestAdvancingApplyIsNeverBehind(t *testing.T) {
	c, err := New(Options{Node: "n1", Store: &raftlog.Store{}, Machine: consensus.NewMachine(), StallTimeout: time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	c.members.Upsert(consensus.Meta{Name: "n1", ID: c.id})
	c.leadID.Store(c.id + 1)
	c.catchUpTarget.Store(1000)
	now := time.Date(2026, 10, 22, 6, 0, 0, 0, time.UTC)
	applied := uint64(10)
	for i := 0; i < 100; i++ {
		applied++
		c.machine.SetApplied(applied)
		c.markProgress(now)
		if c.behindAt(now) || c.Stalls() != 0 {
			t.Fatalf("tick %d: applied %d advancing by one a tick reported behind (stalls %d)", i, applied, c.Stalls())
		}
		now = now.Add(100 * time.Millisecond)
	}
	stoppedAt := now.Add(-100 * time.Millisecond)
	for ; now.Sub(stoppedAt) < time.Second; now = now.Add(100 * time.Millisecond) {
		c.markProgress(now)
		if c.behindAt(now) {
			t.Fatalf("behind %v after the applied index stopped, before the 1 s bound", now.Sub(stoppedAt))
		}
	}
	c.markProgress(now)
	if !c.behindAt(now) || c.Stalls() != 1 {
		t.Fatalf("not behind (stalls %d) %v after the applied index stopped below the target", c.Stalls(), now.Sub(stoppedAt))
	}
	c.machine.SetApplied(1000)
	c.markProgress(now)
	if c.behindAt(now) || c.Stalls() != 1 {
		t.Fatalf("still behind (stalls %d) at the target", c.Stalls())
	}
	// Idle at the target for a long while, then a higher target: the clock
	// starts at the last tick spent at the target, not at the last advance.
	for i := 0; i < 50; i++ {
		now = now.Add(100 * time.Millisecond)
		c.markProgress(now)
	}
	c.catchUpTarget.Store(1010)
	now = now.Add(100 * time.Millisecond)
	c.markProgress(now)
	if c.behindAt(now) {
		t.Fatal("behind at once when a new target arrived after an idle spell at the old one")
	}
}

// TestBehindOnlyAsAFollowingMember: a stalled applied index is reported
// behind only on a member that has a leader it does not lead and is past its
// recovery.
func TestBehindOnlyAsAFollowingMember(t *testing.T) {
	for _, tc := range []struct {
		name      string
		set       func(c *Core)
		notMember bool
		want      bool
	}{
		{"a following member", func(*Core) {}, false, true},
		{"no leader", func(c *Core) { c.leadID.Store(0) }, false, false},
		{"the leader", func(c *Core) { c.leadID.Store(c.id) }, false, false},
		{"recovering", func(c *Core) { c.machine.BeginRecovery() }, false, false},
		{"not a member", func(*Core) {}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(Options{Node: "n1", Store: &raftlog.Store{}, Machine: consensus.NewMachine(), StallTimeout: time.Second, Logger: quiet})
			if err != nil {
				t.Fatal(err)
			}
			if !tc.notMember {
				c.members.Upsert(consensus.Meta{Name: "n1", ID: c.id})
			}
			c.leadID.Store(c.id + 1)
			c.catchUpTarget.Store(100)
			c.machine.SetApplied(10)
			at := time.Date(2026, 10, 22, 6, 0, 0, 0, time.UTC)
			c.mark.Store(&applyMark{applied: 10, at: at})
			tc.set(c)
			if got := c.behindAt(at.Add(2 * time.Second)); got != tc.want {
				t.Fatalf("behind %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSlowFollowerUnderLoadIsNeverBehind: a follower whose every Ready is
// delayed 50 ms trails the leader's commit while proposals run from four
// goroutines for 5 s, but its applies advance, so it is never reported
// behind and no stall is counted.
//
// Both preconditions are the test's own doing, so a loaded host cannot leave
// them unmet: the follower's first Ready is held until the leader's commit is
// 20 past its applied index, for at most half the stall timeout so the hold is
// never itself a stall, and the follower's catch-up target is raised to
// the leader's commit as the run goes, as an answered catch-up request does.
// The request itself is timing-bound; TestVoterBehindTheCompactedLogIsNotReady
// holds that path.
func TestSlowFollowerUnderLoadIsNeverBehind(t *testing.T) {
	var slow atomic.Pointer[string]
	var hold atomic.Pointer[func()]
	_, lead, follower, _ := behindCluster(t, func(o *Options) {
		name := o.Node
		o.ReadyGate = func() {
			if p := slow.Load(); p != nil && *p == name {
				if h := hold.Swap(nil); h != nil {
					(*h)()
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	})
	held := func() {
		until := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(until) && lead.core.Status().Commit < follower.core.machine.Applied()+20 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	hold.Store(&held)
	slow.Store(&follower.name)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = lead.core.Propose(ctx, consensus.KindConfig, consensus.ConfigCommand(fmt.Sprintf("load%d-%d", g, i), "v")[1:])
				cancel()
			}
		}(g)
	}
	trailed := false
	target := follower.core.catchUpTarget.Load()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		commit := lead.core.Status().Commit
		raiseTo(&follower.core.catchUpTarget, commit)
		if follower.core.Behind() {
			close(stop)
			wg.Wait()
			t.Fatalf("a slow follower whose applies advance was reported behind: applied %d, target %d", follower.core.machine.Applied(), follower.core.catchUpTarget.Load())
		}
		if follower.core.machine.Applied() < commit {
			trailed = true
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if !trailed || follower.core.catchUpTarget.Load() <= target {
		t.Fatalf("precondition: not reached (the leader's commit did not advance past the follower's applied index): trailed %v, target %d from %d",
			trailed, follower.core.catchUpTarget.Load(), target)
	}
	if n := follower.core.Stalls(); n != 0 {
		t.Errorf("%d stalls counted on a slow follower whose applies advance", n)
	}
}

// TestRaiseToNeverLowers: the target only rises, so an answer arriving after
// a higher one leaves it where it was.
func TestRaiseToNeverLowers(t *testing.T) {
	var a atomic.Uint64
	raiseTo(&a, 81)
	raiseTo(&a, 40)
	if got := a.Load(); got != 81 {
		t.Fatalf("target %d after 81 then 40, want 81", got)
	}
	raiseTo(&a, 90)
	if got := a.Load(); got != 90 {
		t.Fatalf("target %d after 90, want 90", got)
	}
}
