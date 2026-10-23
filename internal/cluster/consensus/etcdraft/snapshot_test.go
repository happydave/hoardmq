package etcdraft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// solo starts a single-node core in dir with the options changed by tweak.
func solo(t *testing.T, dir string, tweak func(o *Options)) *Core {
	t.Helper()
	store, err := raftlog.Open(dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	o := Options{Node: "solo", Store: store, Machine: consensus.NewMachine(), Tick: 10 * time.Millisecond, Logger: quiet}
	if tweak != nil {
		tweak(&o)
	}
	core, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(core.Stop)
	return core
}

func proposeConfig(t *testing.T, core *Core, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := core.Propose(ctx, consensus.KindConfig, consensus.ConfigCommand(key, value)[1:]); err != nil {
		t.Fatalf("propose %s: %v", key, err)
	}
}

func snapFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// TestSnapshotBuildRunsOffTheLoop: while a build is held, the loop keeps
// committing proposals; once released the snapshot is stored as a file and
// the log compacted behind it.
func TestSnapshotBuildRunsOffTheLoop(t *testing.T) {
	dir := t.TempDir()
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	entered := make(chan struct{}, 1)
	core := solo(t, dir, func(o *Options) {
		o.SnapshotEntries = 5
		o.SnapshotTrailing = 2
		o.BuildGate = func() {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-hold
		}
	})
	// Registered after the core's Stop so it runs first: a held build must be
	// released before Stop waits for the loop.
	t.Cleanup(release)
	waitFor(t, "single node leads", 5*time.Second, func() bool { return core.Status().Role == "leader" && core.Ready() })
	for i := 0; i < 6; i++ {
		proposeConfig(t, core, fmt.Sprintf("k%d", i), "v")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no build started")
	}
	// The build is held: proposals still commit.
	for i := 0; i < 20; i++ {
		proposeConfig(t, core, fmt.Sprintf("held%d", i), "v")
	}
	if idx := core.Status().SnapshotIndex; idx != 0 {
		t.Fatalf("a snapshot was stored while its build was held: %d", idx)
	}
	release()
	waitFor(t, "the snapshot stored", 5*time.Second, func() bool { return core.Status().SnapshotIndex > 0 })
	files := snapFiles(t, core.snapDir)
	named := core.storedSnapshotName()
	if named == "" || len(files) == 0 {
		t.Fatalf("stored snapshot names %q; directory holds %v", named, files)
	}
	if _, err := os.Stat(filepath.Join(core.snapDir, named)); err != nil {
		t.Fatalf("the named file: %v", err)
	}
	// The compaction follows the snapshot's storing on the loop.
	waitFor(t, "the log compacted behind the snapshot", 5*time.Second, func() bool {
		first, _ := core.o.Store.FirstIndex()
		return first > 1
	})
}

// TestRestartFromAFileSnapshot: a restart installs the stored snapshot from
// its file, then replays what follows.
func TestRestartFromAFileSnapshot(t *testing.T) {
	dir := t.TempDir()
	tweak := func(o *Options) { o.SnapshotEntries = 5; o.SnapshotTrailing = 1 }
	core := solo(t, dir, tweak)
	waitFor(t, "single node leads", 5*time.Second, func() bool { return core.Status().Role == "leader" && core.Ready() })
	for i := 0; i < 12; i++ {
		proposeConfig(t, core, fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}
	waitFor(t, "a snapshot", 5*time.Second, func() bool { return core.Status().SnapshotIndex > 0 })
	proposeConfig(t, core, "last", "x")
	snapDir := core.snapDir
	named := core.storedSnapshotName()
	core.Stop()
	for _, stray := range []string{"recv-1.part", "00000000000000000001-00000000000000000001.snap", "build-x.snap.tmp"} {
		if err := os.WriteFile(filepath.Join(snapDir, stray), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	again := solo(t, dir, tweak)
	if files := snapFiles(t, snapDir); len(files) != 1 || files[0] != named {
		t.Fatalf("after the start the directory holds %v, want only %s", files, named)
	}
	waitFor(t, "restarted node holds every value", 5*time.Second, func() bool {
		for i := 0; i < 12; i++ {
			if v, ok := again.machine.ConfigValue(fmt.Sprintf("k%d", i)); !ok || v != fmt.Sprint(i) {
				return false
			}
		}
		v, ok := again.machine.ConfigValue("last")
		return ok && v == "x"
	})
	if again.storedSnapshotName() == "" {
		t.Fatal("restart does not hold a file snapshot")
	}
}

// TestRestartFromAFormatOneCheckpoint: a checkpoint an older build wrote,
// with the machine document inline, still restores.
func TestRestartFromAFormatOneCheckpoint(t *testing.T) {
	dir := t.TempDir()
	core := solo(t, dir, nil)
	waitFor(t, "single node leads", 5*time.Second, func() bool { return core.Status().Role == "leader" && core.Ready() })
	proposeConfig(t, core, "k", "v")
	core.Stop()

	store, err := raftlog.Open(dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	last, _ := store.LastIndex()
	term, _ := store.Term(last)
	old := consensus.NewMachine()
	if _, err := old.Apply(last, consensus.ConfigCommand("fromv1", "yes")); err != nil {
		t.Fatal(err)
	}
	old.Membership().Upsert(consensus.Meta{Name: "solo", ID: consensus.NodeID("solo")})
	doc, err := old.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := store.Snapshot()
	cs := snap.GetMetadata().GetConfState()
	if cs == nil {
		cs = &pb.ConfState{Voters: []uint64{consensus.NodeID("solo")}}
	}
	if _, err := store.CreateSnapshot(last, cs, wrapSnapshot(last, term, doc)); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	again := solo(t, dir, nil)
	waitFor(t, "the inline snapshot restored", 5*time.Second, func() bool {
		v, ok := again.machine.ConfigValue("fromv1")
		return ok && v == "yes"
	})
}

// TestStartRefusedWhenTheSnapshotFileIsMissing: a start whose stored
// snapshot names a file that is not there fails, naming it.
func TestStartRefusedWhenTheSnapshotFileIsMissing(t *testing.T) {
	dir := t.TempDir()
	tweak := func(o *Options) { o.SnapshotEntries = 3; o.SnapshotTrailing = 1 }
	core := solo(t, dir, tweak)
	waitFor(t, "single node leads", 5*time.Second, func() bool { return core.Status().Role == "leader" && core.Ready() })
	for i := 0; i < 6; i++ {
		proposeConfig(t, core, fmt.Sprintf("k%d", i), "v")
	}
	waitFor(t, "a snapshot", 5*time.Second, func() bool { return core.Status().SnapshotIndex > 0 })
	named := core.storedSnapshotName()
	snapDir := core.snapDir
	core.Stop()
	if err := os.Remove(filepath.Join(snapDir, named)); err != nil {
		t.Fatal(err)
	}
	store, err := raftlog.Open(dir, "raft", wal.Options{Logger: quiet})
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
		t.Fatal("started without its snapshot file")
	}
	if !strings.Contains(err.Error(), named) {
		t.Fatalf("error %q does not name %s", err, named)
	}
}

// TestSnapshotFileRemovable: the file the stored snapshot names, one a
// transfer reads and one a build owns are never removed; at start every
// other file goes, while running only finished snapshot files go.
func TestSnapshotFileRemovable(t *testing.T) {
	inUse := map[string]bool{"pinned.snap": true, "build-x.snap.tmp": true}
	cases := []struct {
		name     string
		starting bool
		want     bool
	}{
		{"current.snap", false, false},
		{"current.snap", true, false},
		{"pinned.snap", false, false},
		{"pinned.snap", true, false},
		{"build-x.snap.tmp", true, false},
		{"old.snap", false, true},
		{"old.snap", true, true},
		{"recv-y.snap.part", false, false},
		{"recv-y.snap.part", true, true},
		{"build-z.snap.tmp", false, false},
		{"", true, false},
	}
	for _, tc := range cases {
		if got := snapshotFileRemovable(tc.name, storedFile{name: "current.snap", readable: true}, inUse, tc.starting); got != tc.want {
			t.Errorf("removable(%q, starting %v) = %v, want %v", tc.name, tc.starting, got, tc.want)
		}
		if snapshotFileRemovable(tc.name, storedFile{}, nil, tc.starting) {
			t.Errorf("removable(%q, starting %v) under an unreadable stored snapshot", tc.name, tc.starting)
		}
	}
}

// TestStoredSnapshotFile: what each form of stored snapshot says of the
// snapshot directory.
func TestStoredSnapshotFile(t *testing.T) {
	d := snapDesc{Index: 7, Term: 2, Size: 99, Name: builtName(7, 2)}
	desc := d.encode()
	newer := append([]byte(nil), desc...)
	newer[4] = consensus.FileFormat + 1
	cases := []struct {
		name  string
		empty bool
		data  []byte
		want  storedFile
	}{
		{"empty", true, nil, storedFile{readable: true}},
		{"format 2 descriptor", false, desc, storedFile{name: d.Name, readable: true}},
		{"format 1 inline", false, wrapSnapshot(7, 2, []byte("{}")), storedFile{readable: true}},
		{"a newer format", false, newer, storedFile{}},
		{"a truncated descriptor", false, desc[:descHeader-1], storedFile{}},
		{"no magic", false, append([]byte("XXXX"), desc[4:]...), storedFile{}},
	}
	for _, tc := range cases {
		if got := storedSnapshotFile(tc.empty, tc.data); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// TestCompactTarget: the trailing window behind the stored snapshot, lowered
// to the lowest snapshot a transfer still sends.
func TestCompactTarget(t *testing.T) {
	cases := []struct {
		stored, trailing uint64
		floors           []uint64
		want             uint64
	}{
		{100, 10, nil, 90},
		{10, 10, nil, 0},
		{5, 10, nil, 0},
		{100, 10, []uint64{50}, 50},
		{100, 10, []uint64{95}, 90},
		{100, 10, []uint64{70, 40, 80}, 40},
	}
	for _, tc := range cases {
		if got := compactTarget(tc.stored, tc.trailing, tc.floors); got != tc.want {
			t.Errorf("compactTarget(%d, %d, %v) = %d, want %d", tc.stored, tc.trailing, tc.floors, got, tc.want)
		}
	}
}

// TestDescriptorRoundTrip: a descriptor reads back, and an older build's
// check of the format byte names the format.
func TestDescriptorRoundTrip(t *testing.T) {
	d := snapDesc{Index: 7, Term: 2, Size: 99, Name: builtName(7, 2)}
	d.Digest[0] = 1
	got, rest, err := decodeDesc(append(d.encode(), 'x'))
	if err != nil || got != d || string(rest) != "x" {
		t.Fatalf("decoded %+v rest %q err %v", got, rest, err)
	}
	if _, _, _, err := unwrapSnapshot(d.encode()); err == nil || !strings.Contains(err.Error(), "format 2 not supported") {
		t.Fatalf("an older reader's error: %v", err)
	}
	bad := d
	bad.Name = "../escape"
	if _, _, err := decodeDesc(bad.encode()); err == nil {
		t.Fatal("a name outside the directory was accepted")
	}
	if _, _, err := decodeDesc([]byte("HQRS\x01")); err == nil {
		t.Fatal("a format 1 document read as a descriptor")
	}
}

// TestStageRefusesAFileNotMatchingItsDescriptor: a file whose digest, size or
// position differs from what the descriptor names is refused.
func TestStageRefusesAFileNotMatchingItsDescriptor(t *testing.T) {
	m := consensus.NewMachine()
	if _, err := m.Apply(3, consensus.ConfigCommand("k", "v")); err != nil {
		t.Fatal(err)
	}
	capture, err := m.Capture()
	if err != nil {
		t.Fatal(err)
	}
	c := &Core{snapDir: t.TempDir(), stop: make(chan struct{}), machine: consensus.NewMachine()}
	d, err := c.writeSnapshotFile(capture, consensus.FileHeader{Index: 3, Term: 1, Applied: 3}, builtName(3, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.stageSnapshot(d); err != nil {
		t.Fatalf("the matching descriptor: %v", err)
	}
	digest, size, index := d, d, d
	digest.Digest[0] ^= 1
	size.Size++
	index.Index++
	for name, bad := range map[string]snapDesc{"digest": digest, "size": size, "index": index} {
		if _, err := c.stageSnapshot(bad); err == nil {
			t.Errorf("a descriptor with another %s staged", name)
		}
	}
}

// TestReceivedFileIsKeptUntilApplied: a received file is not removed while
// its message waits for the loop, and is once the applied index reaches it.
func TestReceivedFileIsKeptUntilApplied(t *testing.T) {
	dir := t.TempDir()
	store, err := raftlog.Open(dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	c, err := New(Options{Node: "solo", Store: store, Machine: consensus.NewMachine(), Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(c.snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	d := snapDesc{Index: 9, Term: 1, Name: receivedName(9, 1, time.Now())}
	if err := os.WriteFile(filepath.Join(c.snapDir, d.Name), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	c.holdReceived(d)
	c.removeSnapshotFiles(false)
	if _, err := os.Stat(filepath.Join(c.snapDir, d.Name)); err != nil {
		t.Fatalf("a held file was removed: %v", err)
	}
	c.releaseReceived(8)
	if _, err := os.Stat(filepath.Join(c.snapDir, d.Name)); err != nil {
		t.Fatalf("released below its index: %v", err)
	}
	c.releaseReceived(9)
	if _, err := os.Stat(filepath.Join(c.snapDir, d.Name)); !os.IsNotExist(err) {
		t.Fatalf("not removed once applied and unnamed: %v", err)
	}
}

// TestBuildOvertakenByANewerSnapshotIsSuperseded: a build at or below the
// stored snapshot is not stored; it is counted superseded and its file goes.
func TestBuildOvertakenByANewerSnapshotIsSuperseded(t *testing.T) {
	dir := t.TempDir()
	store, err := raftlog.Open(dir, "raft", wal.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	var ents []*pb.Entry
	for i := uint64(1); i <= 5; i++ {
		ents = append(ents, &pb.Entry{Index: proto.Uint64(i), Term: proto.Uint64(1)})
	}
	if _, err := store.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(5)}, ents, nil, true); err != nil {
		t.Fatal(err)
	}
	metrics := &xferCounts{}
	c, err := New(Options{Node: "solo", Store: store, Machine: consensus.NewMachine(), Logger: quiet, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	newer := snapDesc{Index: 5, Term: 1, Name: builtName(5, 1)}
	if _, err := store.CreateSnapshot(5, &pb.ConfState{}, newer.encode()); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(c.snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	older := snapDesc{Index: 3, Term: 1, Name: builtName(3, 1)}
	path := filepath.Join(c.snapDir, older.Name)
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	c.pinFile(older.Name)
	if err := c.finishBuild(buildResult{desc: older, cs: &pb.ConfState{}}); err != nil {
		t.Fatal(err)
	}
	if got := store.SnapshotIndex(); got != 5 {
		t.Fatalf("stored snapshot %d, want the newer 5", got)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the superseded file is still there: %v", err)
	}
	if got := metrics.get("hoardmq_raft_snapshot_builds_total", "superseded"); got != 1 {
		t.Fatalf("superseded builds %v", got)
	}
}

// xferCounts sums counters by name and outcome.
type xferCounts struct {
	mu sync.Mutex
	v  map[string]float64
}

func (m *xferCounts) Add(name string, l map[string]string, d float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[name+"/"+l["outcome"]] += d
}
func (m *xferCounts) Set(string, map[string]string, float64) {}
func (m *xferCounts) get(name, outcome string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[name+"/"+outcome]
}
