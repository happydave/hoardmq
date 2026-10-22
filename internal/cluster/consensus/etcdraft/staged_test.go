package etcdraft

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"go.etcd.io/raft/v3"
)

// TestReceivedSnapshotIsStagedOffTheLoop: while a received snapshot is
// decoded, the follower's loop keeps taking the leader's heartbeats, and the
// install that follows completes.
func TestReceivedSnapshotIsStagedOffTheLoop(t *testing.T) {
	var victimCore atomic.Pointer[Core]
	var during atomic.Int64
	during.Store(-1)
	hooks := map[string]*transferHooks{}
	for _, name := range []string{"n1", "n2", "n3"} {
		hooks[name] = &transferHooks{stageDelay: func() {
			c := victimCore.Load()
			if c == nil || during.Load() >= 0 {
				return
			}
			before := c.heartbeats.Load()
			time.Sleep(3 * c.electionTimeout())
			during.Store(int64(c.heartbeats.Load() - before))
		}}
	}
	sc := newSnapCluster(t, snapOpts{hooks: hooks})
	sc.c.settled(3)
	sc.populate(3000)
	lead, victim, _ := sc.followers()
	// The leader's finish is reported only once the victim has staged the
	// snapshot and answered done: its receipt is counted before the report.
	var receivedAtFinish atomic.Int64
	receivedAtFinish.Store(-1)
	sc.hooks[lead.name].onReport = func(_ uint64, st raft.SnapshotStatus) {
		if st == raft.SnapshotFinish && receivedAtFinish.Load() < 0 {
			receivedAtFinish.Store(int64(sc.metrics[victim.name].sum("hoardmq_raft_snapshot_transfers_total", map[string]string{"direction": "in", "outcome": "ok"})))
		}
	}
	victim.core.Stop()
	victim.core = nil
	sc.needSnapshotStopped(victim, func(c *Core) { victimCore.Store(c) })
	waitFor(t, "the victim rejoins", 30*time.Second, func() bool { return sc.rejoined(victim, 3000, 0) })
	waitFor(t, "the leader's finish report", 5*time.Second, func() bool { return receivedAtFinish.Load() >= 0 })
	if got := receivedAtFinish.Load(); got < 1 {
		t.Fatalf("finish reported with %d transfers received", got)
	}
	if d := during.Load(); d <= 0 {
		t.Fatalf("the victim's loop took %d heartbeats while the snapshot was staged", d)
	}
	victim.core.xferMu.Lock()
	left := victim.core.staged
	victim.core.xferMu.Unlock()
	if left != nil {
		t.Fatal("staged state kept after the install")
	}
}

// TestStagedStateIsDroppedOnceApplied: staged state at or below the applied
// index goes; above it, it stays.
func TestStagedStateIsDroppedOnceApplied(t *testing.T) {
	c := &Core{}
	c.staged = &stagedSnapshot{desc: snapDesc{Index: 10}, st: &consensus.StagedMachine{}}
	c.dropStaged(9)
	if c.staged == nil {
		t.Fatal("dropped below its index")
	}
	c.dropStaged(10)
	if c.staged != nil {
		t.Fatal("kept at its index")
	}
}
