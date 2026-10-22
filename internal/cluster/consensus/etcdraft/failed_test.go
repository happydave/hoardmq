package etcdraft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/wal"
)

// soloCore starts a single-voter core over a raft log whose fsyncs fail
// while fail is set.
func soloCore(t *testing.T, fail *atomic.Bool) *Core {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "solo")
	store, err := raftlog.Open(dir, "raft", wal.Options{Logger: quiet, Sync: func(f *os.File) error {
		if fail.Load() {
			return errors.New("injected fsync failure")
		}
		return f.Sync()
	}})
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
	t.Cleanup(core.Stop)
	waitFor(t, "single node leads", 5*time.Second, func() bool { return core.Status().Role == "leader" && core.Ready() })
	return core
}

// TestFailedSaveIsReported: a save whose fsync fails stops the loop; the
// core then reports the error, is not ready, and says so in its role.
func TestFailedSaveIsReported(t *testing.T) {
	var fail atomic.Bool
	core := soloCore(t, &fail)
	if err := core.Failed(); err != nil {
		t.Fatalf("precondition: a running core reports %v", err)
	}
	fail.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := core.Propose(ctx, consensus.KindConfig, consensus.ConfigCommand("k", "v")[1:]); err == nil {
		t.Fatal("precondition: a proposal committed although every fsync fails")
	}
	waitFor(t, "the failure reported", 5*time.Second, func() bool { return core.Failed() != nil })
	if err := core.Failed(); !strings.Contains(err.Error(), "raft log save") {
		t.Errorf("Failed() = %v, want the save's error", err)
	}
	if core.Ready() {
		t.Error("a core whose loop stopped on a failed save reports ready")
	}
	if role := core.Status().Role; role != "failed" {
		t.Errorf("role %q, want failed", role)
	}
	core.Stop()
	if core.Failed() == nil {
		t.Error("the failure was forgotten when the core stopped")
	}
}

// TestStoppedCoreHasNotFailed: a core stopped normally reports no failure.
func TestStoppedCoreHasNotFailed(t *testing.T) {
	var fail atomic.Bool
	core := soloCore(t, &fail)
	core.Stop()
	if err := core.Failed(); err != nil {
		t.Fatalf("a stopped core reports %v", err)
	}
}
