package raftlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// appendRaw appends one record of typ to dir's raft log, as a damaged or
// foreign writer would.
func appendRaw(t *testing.T, dir string, typ byte, payload []byte) {
	t.Helper()
	l, err := wal.Open(dir, "raft", wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(typ, payload); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func saved(t *testing.T, dir string) {
	t.Helper()
	s := open(t, dir, wal.Options{})
	if _, err := s.Save(hs(1, 1, 0), []*pb.Entry{entry(1, 1, "a"), entry(2, 1, "b")}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestVerifyLogAgreesWithOpen: for each load a start refuses, and a clean
// one, VerifyLog over an inspected log refuses with Open's error.
func TestVerifyLogAgreesWithOpen(t *testing.T) {
	cases := []struct {
		name    string
		build   func(t *testing.T, dir string)
		refuses string
	}{
		{"clean", saved, ""},
		{"an unknown record type", func(t *testing.T, dir string) {
			saved(t, dir)
			appendRaw(t, dir, 99, []byte("x"))
		}, "unknown record type 99"},
		{"an entry that does not decode", func(t *testing.T, dir string) {
			saved(t, dir)
			appendRaw(t, dir, recEntry, []byte{0xFF, 0xFF, 0xFF})
		}, "entry record"},
		{"an entry past a gap in the raft index", func(t *testing.T, dir string) {
			saved(t, dir)
			b, err := proto.Marshal(entry(5, 1, "e"))
			if err != nil {
				t.Fatal(err)
			}
			appendRaw(t, dir, recEntry, b)
		}, "raftlog: gap"},
		{"a checkpoint that does not decode", func(t *testing.T, dir string) {
			saved(t, dir)
			l, err := wal.Open(dir, "raft", wal.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err := l.WriteCheckpoint(2, []byte{1, 2}); err != nil {
				t.Fatal(err)
			}
			_ = l.Close()
		}, "short checkpoint"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			c.build(t, dir)
			sv, l := wal.Inspect(dir, "raft", wal.Options{})
			if sv.Err != nil {
				t.Fatalf("precondition: the survey refuses: %v", sv.Err)
			}
			v := VerifyLog(l)
			s, err := Open(dir, "raft", wal.Options{})
			if err == nil {
				_ = s.Close()
			}
			switch {
			case c.refuses == "" && (v.Err != nil || err != nil):
				t.Fatalf("verify %v, start %v; want both to load", v.Err, err)
			case c.refuses != "" && (v.Err == nil || err == nil || v.Err.Error() != err.Error() || !strings.Contains(err.Error(), c.refuses)):
				t.Fatalf("verify %v, start %v; want the same refusal naming %q", v.Err, err, c.refuses)
			}
		})
	}
}

// A replay point past the log's end is what Open appends after; VerifyLog
// names it and writes nothing.
func TestVerifyLogNamesTheReplayPointAhead(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, wal.Options{})
	if _, err := s.Save(hs(1, 1, 0), []*pb.Entry{entry(1, 1, "a"), entry(2, 1, "b")}, nil, true); err != nil {
		t.Fatal(err)
	}
	seg := filepath.Join(dir, "raft", "00000000000000000001.seg")
	early, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Save(hs(1, 1, 2), nil, nil, false); err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Data: []byte("x"), Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(10), Term: proto.Uint64(1),
		ConfState: &pb.ConfState{Voters: []uint64{1}}}}
	if _, err := s.Save(nil, nil, snap, true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seg, early, 0o640); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "raft"))
	_, l := wal.Inspect(dir, "raft", wal.Options{})
	v := VerifyLog(l)
	if v.Err != nil || v.AppendAfter == 0 || v.AppendAfter <= l.LastIndex() {
		t.Fatalf("VerifyLog %+v over a log ending at %d, want a replay point past it", v, l.LastIndex())
	}
	if v.Snapshot.GetMetadata().GetIndex() != 10 {
		t.Fatalf("the stored snapshot's index %d, want 10", v.Snapshot.GetMetadata().GetIndex())
	}
	if after, _ := os.ReadDir(filepath.Join(dir, "raft")); len(after) != len(entries) {
		t.Fatalf("verify wrote: %d files, was %d", len(after), len(entries))
	}
}
