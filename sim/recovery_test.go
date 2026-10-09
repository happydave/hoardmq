package sim

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
)

// TestRestartNeedsSnapshot: a node crashed for long enough that the leader
// compacts past its log rejoins through a snapshot, and no acknowledged
// message is lost across the restart. This is the case an index guard on
// the snapshot install starved (WI 1291); the fixed schedule keeps it in
// the gate.
func TestRestartNeedsSnapshot(t *testing.T) {
	var logFile *os.File
	if path := os.Getenv("HOARDMQ_SIM_LOG"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		logFile = f
		defer func() { _ = f.Close() }()
	}
	r := Run(t, RunOptions{
		Seed:     1291,
		Window:   9 * time.Second,
		Settle:   12 * time.Second,
		Cluster:  Options{SnapshotEntries: 40, Log: logWriter(logFile)},
		Workload: WorkloadOptions{Rate: 30},
		Schedule: []Step{{At: time.Second, Kind: "crash", Node: "n3", Dur: 5 * time.Second, UntilSnapshot: true}},
	})
	t.Log(r.Describe())
	if len(r.Violations) > 0 {
		t.Fatalf("violations: %v", r.Violations)
	}
	if r.Stats["publications"] == 0 || r.Stats["deliveries"] == 0 {
		t.Fatalf("no traffic: %v", r.Stats)
	}
	// The restarted node must have installed a snapshot and recovered.
	srv := r.Cluster.Node("n3").Server()
	if srv == nil {
		t.Fatal("n3 not running")
	}
	snap, ok := srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
	if !ok {
		t.Fatal("no metrics snapshot")
	}
	var in, recoveries float64
	for _, m := range snap.Snapshot() {
		switch m.Name {
		case "hoardmq_raft_snapshots_total":
			if m.Labels["direction"] == "in" {
				in += m.Value
			}
		case "hoardmq_raft_recoveries_total":
			recoveries += m.Value
		}
	}
	if in == 0 {
		t.Fatalf("n3 did not install a snapshot; the scenario did not exercise the path (recoveries %v)", recoveries)
	}
	if recoveries == 0 {
		t.Fatal("n3 recorded no recovery")
	}
	if srv.Consensus().Recovering() {
		t.Fatal("n3 still recovering after the settle")
	}
}

// TestRestartInsideTheLeaseTTL: a node that crashes and is back within about
// one lease ttl returns while its leases may still name it, holding sessions
// as they were when it crashed. Nothing acknowledged may be lost and nothing
// completed delivered again.
func TestRestartInsideTheLeaseTTL(t *testing.T) {
	var logFile *os.File
	if path := os.Getenv("HOARDMQ_SIM_LOG"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		logFile = f
		defer func() { _ = f.Close() }()
	}
	r := Run(t, RunOptions{
		Seed:     1291,
		Window:   9 * time.Second,
		Settle:   12 * time.Second,
		Cluster:  Options{SnapshotEntries: 40, Log: logWriter(logFile)},
		Workload: WorkloadOptions{Rate: 30},
		Schedule: []Step{{At: time.Second, Kind: "crash", Node: "n3", Dur: 2 * time.Second}},
	})
	t.Log(r.Describe())
	if len(r.Violations) > 0 {
		t.Fatalf("violations: %v", r.Violations)
	}
	if r.Stats["publications"] == 0 || r.Stats["deliveries"] == 0 {
		t.Fatalf("no traffic: %v", r.Stats)
	}
	// The restarted node refuses clients while it recovers; that must end.
	n3 := r.Cluster.Node("n3")
	if srv := n3.Server(); srv == nil || srv.Consensus().Recovering() {
		t.Fatal("n3 is down or still recovering after the settle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probe := NewClient("after-recovery", []string{n3.TCP}, 0, 1)
	if err := probe.Run(ctx); err != nil {
		t.Fatalf("n3 admits nobody after its recovery: %v", err)
	}
	probe.Close()
}

// logWriter returns nil for a nil file, so the cluster logs nothing.
func logWriter(f *os.File) io.Writer {
	if f == nil {
		return nil
	}
	return f
}
