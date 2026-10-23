package etcdraft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// holdingRecorder is a recorder that holds the machine while hold is set,
// standing in for a build below the cluster's level.
type holdingRecorder struct {
	recorder
	hold *atomic.Bool
}

func (h *holdingRecorder) Holds() (bool, string) {
	if h.hold.Load() {
		return true, "test hold"
	}
	return false, ""
}

// holdCluster starts three nodes, each with a holding recorder whose switch
// is in holds, snapshotting every 10 entries, and settles them.
func holdCluster(t *testing.T) (*tcluster, []string, map[string]*holdingRecorder, map[string]*atomic.Bool, map[string]*gauges) {
	t.Helper()
	return holdClusterWith(t, nil)
}

// holdClusterWith is holdCluster with each node's options changed by tweak.
func holdClusterWith(t *testing.T, tweak func(o *Options)) (*tcluster, []string, map[string]*holdingRecorder, map[string]*atomic.Bool, map[string]*gauges) {
	t.Helper()
	names := []string{"n1", "n2", "n3"}
	recs := map[string]*holdingRecorder{}
	holds := map[string]*atomic.Bool{}
	metrics := map[string]*gauges{}
	for _, n := range names {
		holds[n] = &atomic.Bool{}
		metrics[n] = &gauges{v: map[string]float64{}}
	}
	c := newTCluster(t, func(o *Options) {
		o.SnapshotEntries = 10
		o.SnapshotTrailing = 5
		o.StallTimeout = 300 * time.Millisecond
		o.Metrics = metrics[o.Node]
		if tweak != nil {
			tweak(o)
		}
	})
	for _, name := range names {
		c.addTransport(name, others(name, names))
	}
	for _, name := range names {
		c.startCore(c.nodes[name], names, registerHolding(recs, holds, name))
	}
	c.settled(3)
	return c, names, recs, holds, metrics
}

func registerHolding(recs map[string]*holdingRecorder, holds map[string]*atomic.Bool, name string) func(o *Options) {
	return func(o *Options) {
		recs[name] = &holdingRecorder{hold: holds[name]}
		o.Machine.Register(recKind, recs[name])
	}
}

func proposeRec(t *testing.T, c *tcluster, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		proposeOnLeader(t, c, recKind, []byte{'a'})
	}
}

// followerOf is a node that does not lead.
func followerOf(c *tcluster, names []string) *tnode {
	lead := c.leader()
	for _, n := range names {
		if c.nodes[n] != lead {
			return c.nodes[n]
		}
	}
	return nil
}

// TestHeldFollowerAppliesNothingButKeepsTheLog: a follower whose machine
// holds applies no command and moves no applied index, through commands and
// a membership change; it still stores and acknowledges every entry (with
// the other follower stopped the group commits only with it), follows the
// membership, builds no snapshot, and reports its hold.
func TestHeldFollowerAppliesNothingButKeepsTheLog(t *testing.T) {
	c, names, recs, holds, metrics := holdCluster(t)
	proposeRec(t, c, 5)
	f := followerOf(c, names)
	waitFor(t, "the follower applied the first five", 5*time.Second, func() bool { return len(recs[f.name].snapshot().state) == 5 })
	holds[f.name].Store(true)
	proposeRec(t, c, 1)
	waitFor(t, "the follower holds", 5*time.Second, func() bool { _, ok := f.core.Held(); return ok })
	at := f.core.machine.Applied()
	snap := f.store.SnapshotIndex()
	lead := c.leader()
	// The third node stopped: a commit now needs the held follower's
	// acknowledgement.
	for _, n := range names {
		if nd := c.nodes[n]; nd != f && nd != lead {
			nd.core.Stop()
		}
	}
	for i := 0; i < 25; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := lead.core.Propose(ctx, recKind, []byte{'b'})
		cancel()
		if err != nil {
			t.Fatalf("a commit needing the held follower's acknowledgement failed: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := lead.core.AddMember(ctx, "n4", "127.0.0.1:1"); err != nil {
		cancel()
		t.Fatalf("a membership change needing the held follower failed: %v", err)
	}
	cancel()
	waitFor(t, "the held follower follows the membership", 5*time.Second, func() bool {
		for _, m := range f.core.Members() {
			if m.Name == "n4" {
				return true
			}
		}
		return false
	})
	if got := f.core.machine.Applied(); got != at {
		t.Fatalf("the held follower's applied index moved from %d to %d", at, got)
	}
	if got := len(recs[f.name].snapshot().state); got != 5 {
		t.Fatalf("the held follower applied %d commands, want the 5 before its hold", got)
	}
	if got := f.store.SnapshotIndex(); got != snap {
		t.Fatalf("the held follower built a snapshot: index %d, was %d", got, snap)
	}
	if h, _ := f.core.Held(); h.Index <= at {
		t.Fatalf("held at %d with applied %d", h.Index, at)
	}
	g := metrics[f.name]
	waitFor(t, "the held gauges", 5*time.Second, func() bool {
		return g.get("hoardmq_cluster_feature_level_held") == 1 && g.get("hoardmq_raft_held_entries") >= 25
	})
	if f.core.Ready() {
		t.Fatal("a held node reads ready")
	}
	time.Sleep(600 * time.Millisecond)
	if f.core.Behind() || f.core.Stalls() != 0 {
		t.Fatalf("a hold read as a stall: behind %v, stalls %d", f.core.Behind(), f.core.Stalls())
	}
}

// TestHeldLeaderRefusesAtOnceAndReplaysAfterRestart: leadership moved to a
// held node by a transfer, its proposals fail at once with ErrHeld; moved
// away again; restarted with its holder off it applies every command it
// held, and with the holder on it holds again.
func TestHeldLeaderRefusesAtOnceAndReplaysAfterRestart(t *testing.T) {
	c, names, recs, holds, _ := holdCluster(t)
	f := followerOf(c, names)
	holds[f.name].Store(true)
	proposeRec(t, c, 20)
	waitFor(t, "the follower holds", 5*time.Second, func() bool { _, ok := f.core.Held(); return ok })
	held, _ := f.core.Held()

	lead := c.leader()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := lead.core.TransferLeadership(ctx, f.name); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	waitFor(t, "leadership on the held node", 5*time.Second, func() bool { return f.core.Status().Role == "leader" })
	begin := time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	_, err := f.core.Propose(ctx, recKind, []byte{'c'})
	cancel()
	if !errors.Is(err, consensus.ErrHeld) || time.Since(begin) > 100*time.Millisecond {
		t.Fatalf("a held leader's proposal: %v after %v, want ErrHeld at once", err, time.Since(begin))
	}
	var other *tnode
	for _, n := range names {
		if c.nodes[n] != f {
			other = c.nodes[n]
			break
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	if err := f.core.TransferLeadership(ctx, other.name); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	waitFor(t, "leadership off the held node", 5*time.Second, func() bool { return f.core.Status().Role != "leader" })
	// Its term as leader appended an empty entry, and the new leader another:
	// neither moves the applied index past the command it held at.
	if got := f.core.machine.Applied(); got >= held.Index {
		t.Fatalf("the held node's applied index reached %d, at or past its hold at %d", got, held.Index)
	}
	proposeRec(t, c, 10)
	want := 30
	waitFor(t, "the leader applied everything", 5*time.Second, func() bool { return len(recs[c.leader().name].snapshot().state) == want })

	// The same build restarted: it holds again at the same entry.
	f.core.Stop()
	c.startCore(f, names, registerHolding(recs, holds, f.name))
	waitFor(t, "held again after a restart on the same build", 10*time.Second, func() bool { _, ok := f.core.Held(); return ok })
	if again, _ := f.core.Held(); again.Index != held.Index {
		t.Fatalf("held at %d after the restart, %d before", again.Index, held.Index)
	}

	// A build that can apply it: every held command replayed.
	f.core.Stop()
	holds[f.name].Store(false)
	c.startCore(f, names, registerHolding(recs, holds, f.name))
	waitFor(t, "the upgraded node applied every command", 10*time.Second, func() bool { return len(recs[f.name].snapshot().state) == want })
	if _, ok := f.core.Held(); ok {
		t.Fatal("the upgraded node holds")
	}
}

// TestLogBytesSumsTheLogsFiles: the size a held node reports is every file
// under its raft log's directory, and nothing for one it cannot read.
func TestLogBytesSumsTheLogsFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), make([]byte, 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "snap"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "snap", "b"), make([]byte, 23), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := logBytes(dir); got != 123 {
		t.Fatalf("logBytes = %d, want 123", got)
	}
	if got := logBytes(filepath.Join(dir, "missing")); got != 0 {
		t.Fatalf("logBytes of a missing directory = %d, want 0", got)
	}
}

// TestHoldIsLoggedAuditedAndWarnedEachMinute: entering the hold logs one
// Error naming the entry and the raft log's size and writes one audit
// record; while held, a tick a minute after the last report warns again with
// the log's size and growth, and an earlier tick says nothing.
func TestHoldIsLoggedAuditedAndWarnedEachMinute(t *testing.T) {
	var mu sync.Mutex
	logs := map[string]*warnLog{}
	audits := map[string][]map[string]string{}
	c, names, _, holds, _ := holdClusterWith(t, func(o *Options) {
		node := o.Node
		mu.Lock()
		logs[node] = &warnLog{node: node}
		mu.Unlock()
		o.Logger = slog.New(logs[node])
		o.Audit = func(action, target, outcome string, detail map[string]string) {
			mu.Lock()
			defer mu.Unlock()
			rec := map[string]string{"action": action, "target": target, "outcome": outcome}
			for k, v := range detail {
				rec[k] = v
			}
			audits[node] = append(audits[node], rec)
		}
	})
	proposeRec(t, c, 3)
	f := followerOf(c, names)
	holds[f.name].Store(true)
	proposeRec(t, c, 1)
	waitFor(t, "the follower holds", 5*time.Second, func() bool { _, ok := f.core.Held(); return ok })
	h, _ := f.core.Held()
	f.core.Stop()

	held := logs[f.name].matching("consensus holds")
	if len(held) != 1 || held[0]["entry"] != fmt.Sprint(h.Index) || held[0]["logBytes"] == "" || held[0]["logBytes"] == "0" {
		t.Fatalf("the hold's Error lines: %v, want one naming entry %d and the log's size", held, h.Index)
	}
	mu.Lock()
	var recs []map[string]string
	for _, r := range audits[f.name] {
		if r["action"] == "consensus.held" {
			recs = append(recs, r)
		}
	}
	mu.Unlock()
	if len(recs) != 1 || recs[0]["target"] != fmt.Sprint(h.Index) || recs[0]["outcome"] != "held" || recs[0]["logBytes"] == "" {
		t.Fatalf("the hold's audit records: %v, want one for entry %d", recs, h.Index)
	}

	at := f.core.heldWarnAt
	f.core.warnHeld(at.Add(heldWarnEvery - time.Second))
	if got := logs[f.name].matching("consensus still holds"); len(got) != 0 {
		t.Fatalf("warned before a minute had passed: %v", got)
	}
	f.core.warnHeld(at.Add(heldWarnEvery))
	still := logs[f.name].matching("consensus still holds")
	if len(still) != 1 || still[0]["logBytes"] == "" || still[0]["grewBytes"] == "" || still[0]["heldFrom"] != fmt.Sprint(h.Index) {
		t.Fatalf("the minute's Warn: %v, want one with the log's size and growth", still)
	}
	f.core.warnHeld(at.Add(heldWarnEvery + time.Second))
	if got := logs[f.name].matching("consensus still holds"); len(got) != 1 {
		t.Fatalf("warned again a second after the last: %d warnings", len(got))
	}
}
