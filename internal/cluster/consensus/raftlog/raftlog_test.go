package raftlog

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/happydave/hoardmq/internal/wal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func entry(index, term uint64, data string) *pb.Entry {
	return &pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(term), Type: pb.EntryNormal.Enum(), Data: []byte(data)}
}

func hs(term, vote, commit uint64) *pb.HardState {
	return &pb.HardState{Term: proto.Uint64(term), Vote: proto.Uint64(vote), Commit: proto.Uint64(commit)}
}

func open(t *testing.T, dir string, opts wal.Options) *Store {
	t.Helper()
	s, err := Open(dir, "raft", opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// same compares the store with a MemoryStorage oracle over the whole index
// range and the initial state.
func same(t *testing.T, s *Store, m *raft.MemoryStorage) {
	t.Helper()
	sf, _ := s.FirstIndex()
	mf, _ := m.FirstIndex()
	sl, _ := s.LastIndex()
	ml, _ := m.LastIndex()
	if sf != mf || sl != ml {
		t.Fatalf("index range: store %d..%d, oracle %d..%d", sf, sl, mf, ml)
	}
	for i := sf - 1; i <= sl; i++ {
		st, serr := s.Term(i)
		mt, merr := m.Term(i)
		if st != mt || (serr == nil) != (merr == nil) {
			t.Fatalf("term(%d): store %d %v, oracle %d %v", i, st, serr, mt, merr)
		}
	}
	if sf <= sl {
		se, err := s.Entries(sf, sl+1, 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		me, _ := m.Entries(sf, sl+1, 1<<30)
		if len(se) != len(me) {
			t.Fatalf("entries: %d vs %d", len(se), len(me))
		}
		for i := range se {
			if !proto.Equal(se[i], me[i]) {
				t.Fatalf("entry %d differs", i)
			}
		}
	}
	shs, scs, _ := s.InitialState()
	mhs, mcs, _ := m.InitialState()
	if !proto.Equal(shs, mhs) || fmt.Sprint(scs.GetVoters(), scs.GetLearners()) != fmt.Sprint(mcs.GetVoters(), mcs.GetLearners()) {
		t.Fatalf("initial state: %v %v vs %v %v", shs, scs, mhs, mcs)
	}
	ss, _ := s.Snapshot()
	ms, _ := m.Snapshot()
	if ss.Metadata.GetIndex() != ms.Metadata.GetIndex() {
		t.Fatalf("snapshot index %d vs %d", ss.Metadata.GetIndex(), ms.Metadata.GetIndex())
	}
}

func TestStoreMatchesMemoryStorageAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, wal.Options{})
	m := raft.NewMemoryStorage()
	apply := func(h *pb.HardState, ents []*pb.Entry) {
		t.Helper()
		if _, err := s.Save(h, ents, nil, true); err != nil {
			t.Fatal(err)
		}
		if h != nil {
			_ = m.SetHardState(h)
		}
		_ = m.Append(ents)
	}
	apply(hs(1, 1, 0), []*pb.Entry{entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")})
	same(t, s, m)
	// Conflicting suffix: a new leader overwrites 2 and 3.
	apply(hs(2, 2, 1), []*pb.Entry{entry(2, 2, "B"), entry(3, 2, "C"), entry(4, 2, "D")})
	same(t, s, m)
	// Entries at or below the compaction point are ignored.
	cs := &pb.ConfState{Voters: []uint64{1}}
	if _, err := s.CreateSnapshot(2, cs, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateSnapshot(2, cs, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(2); err != nil {
		t.Fatal(err)
	}
	if err := m.Compact(2); err != nil {
		t.Fatal(err)
	}
	same(t, s, m)
	if _, err := s.Entries(1, 3, 1<<20); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted range: %v", err)
	}
	if _, err := s.Entries(3, 9, 1<<20); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("beyond last: %v", err)
	}
	apply(hs(2, 2, 4), []*pb.Entry{entry(5, 2, "e")})
	same(t, s, m)
	// Size limit returns at least one entry and stops at the bound.
	ents, _ := s.Entries(3, 6, 1)
	if len(ents) != 1 {
		t.Fatalf("size-limited entries: %d", len(ents))
	}
	if !s.HasState() {
		t.Fatal("state not reported")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen: the checkpoint plus the records after it rebuild the index.
	s2 := open(t, dir, wal.Options{})
	defer func() { _ = s2.Close() }()
	same(t, s2, m)
	snap, _ := s2.Snapshot()
	if string(snap.Data) != "snap" || snap.Metadata.GetIndex() != 2 {
		t.Fatalf("snapshot after reopen: %v", snap)
	}
}

func TestIncomingSnapshotResetsTheLog(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, wal.Options{})
	if _, err := s.Save(hs(1, 1, 3), []*pb.Entry{entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")}, nil, true); err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Data: []byte("state"), Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(10), Term: proto.Uint64(3), ConfState: &pb.ConfState{Voters: []uint64{1, 2}}}}
	if _, err := s.Save(hs(3, 2, 10), nil, snap, false); err != nil {
		t.Fatal(err)
	}
	if first, _ := s.FirstIndex(); first != 11 {
		t.Fatalf("first index after snapshot %d", first)
	}
	if _, err := s.Save(nil, []*pb.Entry{entry(11, 3, "k")}, nil, true); err != nil {
		t.Fatal(err)
	}
	// An older snapshot is refused.
	old := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(3)}}
	if _, err := s.Save(nil, nil, old, false); !errors.Is(err, raft.ErrSnapOutOfDate) {
		t.Fatalf("stale snapshot: %v", err)
	}
	_ = s.Close()
	s2 := open(t, dir, wal.Options{})
	defer func() { _ = s2.Close() }()
	first, _ := s2.FirstIndex()
	last, _ := s2.LastIndex()
	if first != 11 || last != 11 {
		t.Fatalf("after reopen %d..%d", first, last)
	}
	h, cs, _ := s2.InitialState()
	if h.GetCommit() != 10 || len(cs.GetVoters()) != 2 {
		t.Fatalf("initial state %v %v", h, cs)
	}
}

// TestSaveReturnsOnlyAfterSync uses the write-ahead log's sync seam: while
// the barrier holds, Save has not returned; the token it returns afterwards
// is newer than the one before.
func TestSaveReturnsOnlyAfterSync(t *testing.T) {
	dir := t.TempDir()
	barrier := make(chan struct{})
	var armed atomic.Bool
	var once sync.Once
	s := open(t, dir, wal.Options{Sync: func(f *os.File) error {
		if armed.Load() {
			once.Do(func() { <-barrier })
		}
		return f.Sync()
	}})
	defer func() { _ = s.Close() }()
	armed.Store(true)
	before := s.LastDurable()
	returned := make(chan Durable, 1)
	go func() {
		tok, err := s.Save(hs(1, 1, 1), []*pb.Entry{entry(1, 1, "x")}, nil, true)
		if err != nil {
			t.Error(err)
		}
		returned <- tok
	}()
	select {
	case <-returned:
		t.Fatal("Save returned before the sync completed")
	default:
	}
	close(barrier)
	tok := <-returned
	if tok.Seq() <= before.Seq() {
		t.Fatalf("token did not advance: %d then %d", before.Seq(), tok.Seq())
	}
	// A save without sync returns the same token as before: nothing new is
	// durable, and a sender holding it may only send what was already
	// covered.
	tok2, _ := s.Save(nil, []*pb.Entry{entry(2, 1, "y")}, nil, false)
	if tok2.Seq() != tok.Seq() {
		t.Fatalf("unsynced save advanced the token")
	}
}

// segmentExt is the extension internal/wal gives a log segment. It is
// unexported there, so this copy can drift; the failure below names every
// file it found, which is what makes a drift diagnosable rather than a skip.
const segmentExt = ".seg"

func TestTornTailKeepsThePrefix(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, wal.Options{})
	if _, err := s.Save(hs(1, 1, 2), []*pb.Entry{entry(1, 1, "a"), entry(2, 1, "b")}, nil, true); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	// Damage the tail of the active segment by truncating a few bytes.
	segs, _ := os.ReadDir(dir + "/raft")
	var seg string
	for _, e := range segs {
		if !e.IsDir() && strings.HasSuffix(e.Name(), segmentExt) {
			seg = dir + "/raft/" + e.Name()
		}
	}
	if seg == "" {
		// The store was just written and closed, so a segment exists; not
		// finding one means the name or the layout changed, which is a
		// finding rather than a reason to pass.
		names := make([]string, 0, len(segs))
		for _, e := range segs {
			names = append(names, e.Name())
		}
		t.Fatalf("no %s segment under %s/raft after writing and closing the store; found %v", segmentExt, dir, names)
	}
	info, _ := os.Stat(seg)
	if err := os.Truncate(seg, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	s2 := open(t, dir, wal.Options{})
	defer func() { _ = s2.Close() }()
	last, _ := s2.LastIndex()
	if last != 1 && last != 2 {
		t.Fatalf("unexpected last index %d after a torn tail", last)
	}
}
