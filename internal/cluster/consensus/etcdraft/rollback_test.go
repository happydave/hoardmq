package etcdraft

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/wal"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TestStartOverANewerSnapshotFormatRefusesAndKeepsItsFiles: a node put back
// on this build over a disk where a newer build stored its snapshot in a
// format this build does not know refuses to start, naming the format, and
// leaves the snapshot directory as it found it, so the newer build can start
// on it again.
func TestStartOverANewerSnapshotFormatRefusesAndKeepsItsFiles(t *testing.T) {
	dir := t.TempDir()
	core := solo(t, dir, nil)
	waitFor(t, "single node leads", 5*time.Second, func() bool { return core.Status().Role == "leader" && core.Ready() })
	proposeConfig(t, core, "k", "v")
	snapDir := core.snapDir
	core.Stop()

	store, err := raftlog.Open(dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	last, _ := store.LastIndex()
	snap, _ := store.Snapshot()
	cs := snap.GetMetadata().GetConfState()
	if cs == nil {
		cs = &pb.ConfState{Voters: []uint64{consensus.NodeID("solo")}}
	}
	newer := consensus.FileFormat + 1
	kept := []string{"newer-format.snap", "newer-format.snap.part"}
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range kept {
		if err := os.WriteFile(filepath.Join(snapDir, name), []byte("a newer build's snapshot"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	data := append(append([]byte(nil), consensus.SnapshotMagic...), byte(newer))
	data = append(data, make([]byte, 64)...)
	if _, err := store.CreateSnapshot(last, cs, data); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	store, err = raftlog.Open(dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	c, err := New(Options{Node: "solo", Store: store, Machine: consensus.NewMachine(), Tick: 10 * time.Millisecond, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Start()
	if err == nil {
		c.Stop()
		t.Fatal("started over a snapshot stored in a newer format")
	}
	if want := fmt.Sprintf("format %d", newer); !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not name %q", err, want)
	}
	for _, name := range kept {
		if _, err := os.Stat(filepath.Join(snapDir, name)); err != nil {
			t.Errorf("the newer build's %s is gone after the refused start: %v", name, err)
		}
	}
}
