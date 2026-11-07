package datadir_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	. "github.com/happydave/hoardmq/internal/datadir"
	"github.com/happydave/hoardmq/internal/datadir/datadirtest"
)

func TestMain(m *testing.M) {
	datadirtest.MaybeHold()
	os.Exit(m.Run())
}

// A lock another process holds refuses both kinds of attempt and names its
// PID; once that process ends the lock is free.
func TestLockHeldByAnotherProcess(t *testing.T) {
	dir := t.TempDir()
	pid, release := datadirtest.Hold(t, dir)
	for name, l := range map[string]*Lock{"exclusive": Exclusive(dir), "shared": Shared(dir)} {
		if l.Outcome != HeldElsewhere || l.PID != pid {
			t.Errorf("%s while another process holds it: %v PID %d, want held-elsewhere PID %d", name, l.Outcome, l.PID, pid)
		}
		_ = l.Release()
	}
	release()
	l := Exclusive(dir)
	if l.Outcome != Held {
		t.Fatalf("after the holder ended: %v (%v)", l.Outcome, l.Err)
	}
	b, err := os.ReadFile(filepath.Join(dir, LockName))
	if err != nil || string(b) != strconv.Itoa(os.Getpid())+"\n" {
		t.Fatalf("the lock file reads %q %v, want this process's PID", b, err)
	}
	if s := Shared(dir); s.Outcome != HeldElsewhere {
		t.Errorf("a shared attempt beside this process's exclusive lock: %v", s.Outcome)
	}
	_ = l.Release()
	a, b2 := Shared(dir), Shared(dir)
	if a.Outcome != Held || b2.Outcome != Held {
		t.Errorf("two shared holders: %v, %v", a.Outcome, b2.Outcome)
	}
	if e := Exclusive(dir); e.Outcome != HeldElsewhere {
		t.Errorf("an exclusive attempt beside shared holders: %v", e.Outcome)
	}
	_, _ = a.Release(), b2.Release()
}

// A shared attempt on a directory without a lock file creates nothing.
func TestSharedLockCreatesNothing(t *testing.T) {
	dir := t.TempDir()
	if l := Shared(dir); l.Outcome != Absent {
		t.Fatalf("no lock file: %v, want absent", l.Outcome)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a shared attempt created %d files", len(entries))
	}
}

// An exclusive attempt creates a missing data directory, as the logs would.
func TestExclusiveCreatesTheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	l := Exclusive(dir)
	defer func() { _ = l.Release() }()
	if l.Outcome != Held {
		t.Fatalf("%v (%v)", l.Outcome, l.Err)
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o750 {
		t.Fatalf("the directory: %v %v", info, err)
	}
}
