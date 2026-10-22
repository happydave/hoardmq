package raftlog

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/happydave/hoardmq/internal/store/faultdisk"
	"github.com/happydave/hoardmq/internal/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// snapshotCutBeforeItsSync saves entries 1 and 2 with a synced hard state,
// then a commit-only hard state left unsynced, then an incoming snapshot at
// 10 with entry 11. The power is cut at the save's first sync of the log, and
// what the cut leaves is copied into crashed. A checkpoint written before that
// sync would replay from past the log's end.
func snapshotCutBeforeItsSync(t *testing.T, dir, crashed string) {
	t.Helper()
	d := faultdisk.New()
	var armed, cut atomic.Bool
	d.Before = func(_ int, path string) {
		if armed.Load() && strings.HasSuffix(path, ".seg") && cut.CompareAndSwap(false, true) {
			d.CrashCopy(t, filepath.Join(dir, "raft"), filepath.Join(crashed, "raft"))
		}
	}
	s := open(t, dir, wal.Options{Sync: d.Sync})
	if _, err := s.Save(hs(1, 1, 0), []*pb.Entry{entry(1, 1, "a"), entry(2, 1, "b")}, nil, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Save(hs(1, 1, 2), nil, nil, false); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	snap := &pb.Snapshot{Data: []byte("x"), Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(10), Term: proto.Uint64(1),
		ConfState: &pb.ConfState{Voters: []uint64{1}}}}
	if _, err := s.Save(nil, []*pb.Entry{entry(11, 1, "c")}, snap, true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !cut.Load() {
		t.Fatal("precondition: the save's sync of the log was never reached")
	}
}

// TestVoteSyncedAfterARestartOverANewerCheckpointSurvives: after the cut the
// node restarts from what the disk kept; what it saves and syncs then, a vote
// alone or an entry with a vote, is there at the next restart.
func TestVoteSyncedAfterARestartOverANewerCheckpointSurvives(t *testing.T) {
	for _, withEntry := range []bool{false, true} {
		name := "a vote alone"
		if withEntry {
			name = "an entry and a vote"
		}
		t.Run(name, func(t *testing.T) {
			dir, crashed := t.TempDir(), t.TempDir()
			snapshotCutBeforeItsSync(t, dir, crashed)

			r := open(t, crashed, wal.Options{})
			want, err := r.LastIndex()
			if err != nil {
				t.Fatal(err)
			}
			var ents []*pb.Entry
			if withEntry {
				want++
				ents = []*pb.Entry{entry(want, 2, "d")}
			}
			if _, err := r.Save(hs(2, 2, 0), ents, nil, true); err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}

			again, err := Open(crashed, "raft", wal.Options{})
			if err != nil {
				t.Fatalf("restart after the save was synced: %v", err)
			}
			defer func() { _ = again.Close() }()
			h, _, _ := again.InitialState()
			if h.GetTerm() != 2 || h.GetVote() != 2 {
				t.Errorf("hard state term %d vote %d at the next restart, want the synced term 2 vote 2", h.GetTerm(), h.GetVote())
			}
			if last, _ := again.LastIndex(); last != want {
				t.Errorf("last entry %d at the next restart, want %d", last, want)
			}
		})
	}
}

// TestOpenAppendsAtTheCheckpointsReplayPoint: a log that ends below its
// checkpoint's replay point (records the disk lost though they were synced
// before the checkpoint) continues at the replay point, so a vote saved after
// the restart is there at the next one.
func TestOpenAppendsAtTheCheckpointsReplayPoint(t *testing.T) {
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

	r := open(t, dir, wal.Options{})
	if r.log.LastIndex() < r.ckptWal {
		t.Fatalf("opened appending at %d, below the checkpoint's replay point %d", r.log.LastIndex()+1, r.ckptWal+1)
	}
	if _, err := r.Save(hs(2, 2, 10), nil, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	again := open(t, dir, wal.Options{})
	defer func() { _ = again.Close() }()
	if h, _, _ := again.InitialState(); h.GetTerm() != 2 || h.GetVote() != 2 {
		t.Fatalf("hard state term %d vote %d at the next restart, want the synced term 2 vote 2", h.GetTerm(), h.GetVote())
	}
}

// TestDurableTokenWaitsForTheLogUnderACheckpoint: a snapshot taken over a
// record not yet synced syncs the log before its checkpoint, so the durable
// token it issues covers that record; when that sync fails, no checkpoint is
// written and the token does not advance.
func TestDurableTokenWaitsForTheLogUnderACheckpoint(t *testing.T) {
	for _, failing := range []bool{true, false} {
		name := "the log syncs"
		if failing {
			name = "the log's sync fails"
		}
		t.Run(name, func(t *testing.T) {
			var failSeg atomic.Bool
			var order []string
			dir := t.TempDir()
			s := open(t, dir, wal.Options{Sync: func(f *os.File) error {
				isSeg := strings.HasSuffix(f.Name(), ".seg")
				if isSeg && failSeg.Load() {
					return errors.New("injected fsync failure")
				}
				if isSeg {
					order = append(order, "log")
				} else {
					order = append(order, "checkpoint")
				}
				return f.Sync()
			}})
			defer func() { _ = s.Close() }()
			if _, err := s.Save(hs(1, 1, 0), []*pb.Entry{entry(1, 1, "a"), entry(2, 1, "b")}, nil, true); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Save(hs(1, 1, 2), nil, nil, false); err != nil {
				t.Fatal(err)
			}
			before := s.LastDurable().Seq()
			failSeg.Store(failing)
			order = nil
			_, err := s.CreateSnapshot(2, &pb.ConfState{Voters: []uint64{1}}, []byte("x"))
			if failing {
				if err == nil {
					t.Fatal("a snapshot was taken although the log under it could not be synced")
				}
				if got := s.LastDurable().Seq(); got != before {
					t.Fatalf("the durable token moved from %d to %d over a record that was never synced", before, got)
				}
				if _, ok, _ := latest(t, dir); ok {
					t.Fatal("a checkpoint was written over a log that could not be synced")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(order) < 2 || order[0] != "log" || order[1] != "checkpoint" {
				t.Fatalf("syncs %v, want the log synced before the checkpoint", order)
			}
			if got := s.LastDurable().Seq(); got <= before {
				t.Fatalf("the durable token stayed at %d after a snapshot over a synced log", got)
			}
		})
	}
}

// latest reports whether the log under dir holds a checkpoint file.
func latest(t *testing.T, dir string) (uint64, bool, error) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "raft"))
	if err != nil {
		return 0, false, err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".ckpt") {
			return 0, true, nil
		}
	}
	return 0, false, nil
}
