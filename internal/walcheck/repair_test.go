package walcheck

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/datadir"
	"github.com/happydave/hoardmq/internal/datadir/datadirtest"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	datadirtest.MaybeHold()
	os.Exit(m.Run())
}

// node builds a data directory with a state log of 12 records over several
// segments and a raft log of two entries; no replica log.
func node(t *testing.T) Paths {
	t.Helper()
	data := filepath.Join(t.TempDir(), "data")
	paths := NodePaths(data, filepath.Join(data, "logs"))
	st, err := store.Open(paths.Logs[0].Parent, store.Options{SegmentBytes: 256, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if err := st.SessionUpsert(store.SessionMeta{Namespace: "ns", ClientID: "c" + strconv.Itoa(i), Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := raftlog.Open(paths.Logs[2].Parent, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	e := func(i uint64) *pb.Entry {
		return &pb.Entry{Index: proto.Uint64(i), Term: proto.Uint64(1), Type: pb.EntryNormal.Enum(), Data: []byte("e")}
	}
	if _, err := r.Save(&pb.HardState{Term: proto.Uint64(1), Vote: proto.Uint64(1)}, []*pb.Entry{e(1), e(2)}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return paths
}

func segs(t *testing.T, p LogPath) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(p.Dir(), "*.seg"))
	if err != nil || len(m) == 0 {
		t.Fatalf("no segments in %s: %v", p.Dir(), err)
	}
	sort.Strings(m)
	return m
}

func cutTail(t *testing.T, path string, n int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-n); err != nil {
		t.Fatal(err)
	}
}

// tree reads every file under dir but the lock file.
func tree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() == datadir.LockName {
			return err
		}
		b, err := os.ReadFile(path)
		out[path] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sameTree(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}
	return true
}

func TestVerifyCleanNode(t *testing.T) {
	p := node(t)
	lock := datadir.Shared(p.DataDir)
	r := Verify(p, lock)
	if r.Verdict != VerdictClean || r.Lock.State != "absent" {
		t.Fatalf("verdict %s, lock %s; findings %+v", r.Verdict, r.Lock.State, r.Findings())
	}
	if !r.Logs[0].Present || r.Logs[1].Present || !r.Logs[2].Present {
		t.Fatalf("present: state %v replica %v raft %v", r.Logs[0].Present, r.Logs[1].Present, r.Logs[2].Present)
	}
	if n := len(r.Logs[0].Segments); n < 3 || r.Logs[0].Segments[n-1].Last != 12 {
		t.Fatalf("state segments %+v", r.Logs[0].Segments)
	}
	if _, err := os.Stat(filepath.Join(p.DataDir, "replica")); !os.IsNotExist(err) {
		t.Fatalf("verify created the replica directory: %v", err)
	}
}

// A dry run reports the repair and writes nothing.
func TestRepairDryRunWritesNothing(t *testing.T) {
	p := node(t)
	s := segs(t, p.Logs[0])
	cutTail(t, s[len(s)-1], 5)
	before := tree(t, p.DataDir)
	r := Repair(p, datadir.Shared(p.DataDir), Options{})
	if r.Verdict != VerdictStartRepairs || !r.Repair.DryRun || len(r.Repair.Actions) != 1 || r.Repair.Actions[0].Repair != RepairTruncateTail {
		t.Fatalf("verdict %s, repair %+v", r.Verdict, r.Repair)
	}
	if !sameTree(before, tree(t, p.DataDir)) {
		t.Fatal("a dry run changed the data directory")
	}
	if _, err := os.Stat(filepath.Join(p.DataDir, datadir.LockName)); !os.IsNotExist(err) {
		t.Fatalf("a dry run created the lock file: %v", err)
	}
}

// -apply on a torn tail keeps the dropped bytes, truncates the segment the
// start's way and leaves the log clean.
func TestRepairTornTailKeepsWhatItDrops(t *testing.T) {
	p := node(t)
	s := segs(t, p.Logs[0])
	last := s[len(s)-1]
	whole, _ := os.ReadFile(last)
	cutTail(t, last, 5)
	cut, _ := os.ReadFile(last)
	lock := datadir.Exclusive(p.DataDir)
	defer func() { _ = lock.Release() }()
	at := time.Date(2026, 11, 2, 19, 4, 5, 0, time.UTC)
	r := Repair(p, lock, Options{Apply: true, Now: func() time.Time { return at }})
	part := r.Repair
	if part.Refused != "" || part.Error != "" || part.DryRun || len(part.Actions) != 1 {
		t.Fatalf("repair %+v", part)
	}
	a := part.Actions[0]
	if want := filepath.Join(p.DataDir, "wal-repair", "20261102T190405Z", "state", filepath.Base(last)+".tail"); a.Copy != want {
		t.Fatalf("kept as %q, want %q", a.Copy, want)
	}
	kept, err := os.ReadFile(a.Copy)
	if err != nil || !bytes.Equal(kept, cut[a.Offset:]) || int64(len(kept)) != a.Bytes {
		t.Fatalf("the kept tail %q (%v), want the %d bytes after offset %d", kept, err, a.Bytes, a.Offset)
	}
	now, _ := os.ReadFile(last)
	if !bytes.Equal(now, cut[:a.Offset]) || !bytes.HasPrefix(whole, now) {
		t.Fatalf("the segment after the repair: %d bytes, want the %d before the torn record", len(now), a.Offset)
	}
	if part.After == nil || part.After.Verdict != VerdictClean {
		t.Fatalf("after the repair: %+v", part.After)
	}
}

// -apply on a torn segment moves it aside whole; -discard keeps nothing.
func TestRepairTornSegment(t *testing.T) {
	for _, discard := range []bool{false, true} {
		p := node(t)
		raft := p.Logs[2]
		torn := filepath.Join(raft.Dir(), "00000000000000000099.seg")
		if err := os.WriteFile(torn, []byte("short"), 0o640); err != nil {
			t.Fatal(err)
		}
		lock := datadir.Exclusive(p.DataDir)
		r := Repair(p, lock, Options{Apply: true, Discard: discard})
		_ = lock.Release()
		part := r.Repair
		if part.Refused != "" || part.Error != "" || len(part.Actions) != 1 || part.Actions[0].Repair != RepairRemoveSegment {
			t.Fatalf("discard %v: repair %+v", discard, part)
		}
		if _, err := os.Stat(torn); !os.IsNotExist(err) {
			t.Fatalf("discard %v: the torn segment is still there: %v", discard, err)
		}
		_, err := os.Stat(filepath.Join(p.DataDir, "wal-repair"))
		switch {
		case discard && !os.IsNotExist(err):
			t.Fatalf("-discard kept a copy: %v", err)
		case !discard:
			if b, err := os.ReadFile(part.Actions[0].Copy); err != nil || string(b) != "short" {
				t.Fatalf("the kept segment %q %v", b, err)
			}
		}
	}
}

// A finding a start refuses on refuses the repair and changes nothing; the
// recovery is named by log.
func TestRepairRefusesDamage(t *testing.T) {
	for _, c := range []struct {
		log      int
		recovery string
	}{{0, "held for the owner"}, {2, "hoardmq cluster remove"}} {
		p := node(t)
		s := segs(t, p.Logs[c.log])
		f, err := os.OpenFile(s[0], os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte{'X'}, 32+9+1); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		// A torn tail beside the damage: repair would have something to do.
		if c.log == 0 {
			cutTail(t, s[len(s)-1], 3)
		}
		before := tree(t, p.DataDir)
		lock := datadir.Exclusive(p.DataDir)
		r := Repair(p, lock, Options{Apply: true})
		_ = lock.Release()
		if r.Verdict != VerdictStartRefuses || !strings.Contains(r.Repair.Refused, c.recovery) || len(r.Repair.Actions) != 0 {
			t.Fatalf("log %s: verdict %s, repair %+v", p.Logs[c.log].Name, r.Verdict, r.Repair)
		}
		if !sameTree(before, tree(t, p.DataDir)) {
			t.Fatalf("log %s: a refused repair changed the data directory", p.Logs[c.log].Name)
		}
	}
}

// Another process holding the lock refuses the repair, naming its PID.
func TestRepairRefusesAHeldDirectory(t *testing.T) {
	p := node(t)
	s := segs(t, p.Logs[0])
	cutTail(t, s[len(s)-1], 5)
	pid, _ := datadirtest.Hold(t, p.DataDir)
	before := tree(t, p.DataDir)
	lock := datadir.Exclusive(p.DataDir)
	r := Repair(p, lock, Options{Apply: true})
	if lock.Outcome != datadir.HeldElsewhere || r.Lock.PID != pid || !strings.Contains(r.Repair.Refused, "not held by this command") {
		t.Fatalf("lock %v PID %d (want %d), repair %+v", lock.Outcome, r.Lock.PID, pid, r.Repair)
	}
	if !sameTree(before, tree(t, p.DataDir)) {
		t.Fatal("a refused repair changed the data directory")
	}
}

// When what a repair drops cannot be kept, it repairs nothing.
func TestRepairThatCannotKeepRepairsNothing(t *testing.T) {
	p := node(t)
	s := segs(t, p.Logs[0])
	cutTail(t, s[len(s)-1], 5)
	if err := os.WriteFile(filepath.Join(p.DataDir, "wal-repair"), []byte("in the way"), 0o640); err != nil {
		t.Fatal(err)
	}
	before := tree(t, p.DataDir)
	lock := datadir.Exclusive(p.DataDir)
	defer func() { _ = lock.Release() }()
	r := Repair(p, lock, Options{Apply: true})
	if !strings.Contains(r.Repair.Error, "nothing was repaired") {
		t.Fatalf("repair %+v", r.Repair)
	}
	if !sameTree(before, tree(t, p.DataDir)) {
		t.Fatal("a repair that could not keep its drop changed the logs")
	}
}

// Each refusal verify reports past the wal survey, with the recovery its log
// names.
func TestVerifyRefusals(t *testing.T) {
	t.Run("a gap in the state log", func(t *testing.T) {
		p := node(t)
		s := segs(t, p.Logs[0])
		if err := os.Remove(s[1]); err != nil {
			t.Fatal(err)
		}
		f := Verify(p, datadir.Shared(p.DataDir)).Findings()
		if len(f) != 1 || f[0].Kind != FindGap || f[0].Log != "state" || f[0].File != filepath.Base(s[2]) || f[0].Recovery != recoverGap {
			t.Fatalf("findings %+v", f)
		}
	})
	t.Run("a raft record of an unknown type", func(t *testing.T) {
		p := node(t)
		l, err := wal.Open(p.Logs[2].Parent, "raft", wal.Options{Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Append(99, []byte("x")); err != nil {
			t.Fatal(err)
		}
		_ = l.Close()
		f := Verify(p, datadir.Shared(p.DataDir)).Findings()
		if len(f) != 1 || f[0].Kind != FindRecovery || f[0].Log != "raft" || !strings.Contains(f[0].Detail, "unknown record type 99") || f[0].Recovery != recoverRaft {
			t.Fatalf("findings %+v", f)
		}
	})
	t.Run("a stored snapshot this build cannot read", func(t *testing.T) {
		p := node(t)
		r, err := raftlog.Open(p.Logs[2].Parent, "raft", wal.Options{Logger: quiet})
		if err != nil {
			t.Fatal(err)
		}
		snap := &pb.Snapshot{Data: []byte("a snapshot of a form nobody knows"), Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(2), Term: proto.Uint64(1),
			ConfState: &pb.ConfState{Voters: []uint64{1}}}}
		if _, err := r.Save(nil, nil, snap, true); err != nil {
			t.Fatal(err)
		}
		_ = r.Close()
		f := Verify(p, datadir.Shared(p.DataDir)).Findings()
		if len(f) != 1 || f[0].Kind != FindSnapshot || f[0].Log != "raft" || f[0].Recovery != recoverRaft {
			t.Fatalf("findings %+v", f)
		}
	})
}

// The operation acts only while the log still asks for exactly the decided
// repairs.
func TestUnchanged(t *testing.T) {
	p := node(t)
	s := segs(t, p.Logs[0])
	cutTail(t, s[len(s)-1], 5)
	f := Verify(p, datadir.Shared(p.DataDir)).Findings()
	d := Decide(datadir.Held, true, f)
	if err := unchanged(p.Logs[0], d.Actions); err != nil {
		t.Fatalf("the decided repair: %v", err)
	}
	other := append([]Action(nil), d.Actions...)
	other[0].Offset--
	if err := unchanged(p.Logs[0], other); err == nil {
		t.Fatal("a repair at another offset passed")
	}
	if err := unchanged(p.Logs[0], nil); err == nil {
		t.Fatal("no decided repair passed over a torn tail")
	}
}

// The notes verify reports: a checkpoint passed over with every record held,
// a checkpoint past the log's end, and no segment left. A start proceeds.
func TestVerifyNotes(t *testing.T) {
	p := node(t)
	l, err := wal.Open(p.Logs[0].Parent, "state", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.WriteCheckpoint(3, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	ckpt := filepath.Join(p.Logs[0].Dir(), "checkpoint-00000000000000000003.ckpt")
	b, _ := os.ReadFile(ckpt)
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(ckpt, b, 0o640); err != nil {
		t.Fatal(err)
	}
	r := Verify(p, datadir.Shared(p.DataDir))
	f := r.Findings()
	if r.Verdict != VerdictClean || len(f) != 1 || f[0].Kind != FindSkippedCheckpoint || f[0].Start != StartNotes || r.Logs[0].Checkpoint != nil {
		t.Fatalf("a checkpoint passed over: %s %+v", r.Verdict, f)
	}

	p = node(t)
	l, err = wal.Open(p.Logs[0].Parent, "state", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.WriteCheckpoint(100, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	r = Verify(p, datadir.Shared(p.DataDir))
	f = r.Findings()
	if r.Verdict != VerdictClean || len(f) != 1 || f[0].Kind != FindCheckpointAhead || f[0].Index != 100 || r.Logs[0].Checkpoint.Index != 100 {
		t.Fatalf("a checkpoint ahead: %s %+v", r.Verdict, f)
	}

	p = node(t)
	if err := os.RemoveAll(p.Logs[2].Dir()); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(p.Logs[2].Dir(), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Logs[2].Dir(), "00000000000000000001.seg"), []byte("cut"), 0o640); err != nil {
		t.Fatal(err)
	}
	r = Verify(p, datadir.Shared(p.DataDir))
	f = r.Findings()
	if r.Verdict != VerdictStartRepairs || len(f) != 2 || f[0].Kind != FindTornSegment || f[1].Kind != FindNoSegment || f[1].Index != 1 {
		t.Fatalf("no segment left: %s %+v", r.Verdict, f)
	}
}
