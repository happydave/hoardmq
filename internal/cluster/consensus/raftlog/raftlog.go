// Package raftlog is the Raft log store on the write-ahead log: it
// implements the library's Storage over an in-memory index rebuilt from the
// log at open, persists hard state, entries and snapshots as records and
// checkpoints, and hands out a durability token that the node loop must hold
// before it may send messages.
package raftlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/happydave/hoardmq/internal/wal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Record types in the write-ahead log.
const (
	recEntry     byte = 1
	recHardState byte = 2
	recTruncate  byte = 3 // payload: the Raft index from which entries are dropped
)

// Durable is a token proving that everything saved before it was issued has
// been synced. The node loop obtains one from Save and must present it to
// the sender; there is no other way to construct one outside this package.
type Durable struct{ seq uint64 }

// Seq is the token's sequence, for tests and metrics.
func (d Durable) Seq() uint64 { return d.seq }

// Store is one node's Raft log.
type Store struct {
	mu      sync.Mutex
	log     *wal.Log
	ents    []*pb.Entry // ents[0] is the dummy entry at the snapshot index
	walIdx  []uint64    // write-ahead index of each entry; 0 for the dummy
	hs      *pb.HardState
	snap    *pb.Snapshot
	ckptWal uint64 // write-ahead index recorded by the latest checkpoint
	durable uint64
	logger  *slog.Logger
}

// Open reads the log under dir/name and rebuilds the index.
func Open(dir, name string, opts wal.Options) (*Store, error) {
	l, err := wal.Open(dir, name, opts)
	if err != nil {
		return nil, err
	}
	s := &Store{log: l, ents: []*pb.Entry{{Index: proto.Uint64(0), Term: proto.Uint64(0)}}, walIdx: []uint64{0}, hs: &pb.HardState{}, snap: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{}}, logger: opts.Logger}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	replayFrom := uint64(1)
	if idx, payload, ok := l.LatestCheckpoint(); ok {
		from, err := s.loadCheckpoint(payload)
		if err != nil {
			_ = l.Close()
			return nil, err
		}
		s.ckptWal = idx
		replayFrom = from
	}
	err = l.Iterate(replayFrom, func(r wal.Record) error { return s.replay(r) })
	if err != nil {
		_ = l.Close()
		return nil, err
	}
	return s, nil
}

// checkpoint payload: 8-byte replay-from write-ahead index, then the hard
// state and the snapshot as length-prefixed protobuf messages.
func (s *Store) checkpointPayload() ([]byte, error) {
	replayFrom := s.log.LastIndex() + 1
	if len(s.walIdx) > 1 {
		replayFrom = s.walIdx[1]
	}
	out := binary.BigEndian.AppendUint64(nil, replayFrom)
	for _, m := range []proto.Message{s.hs, s.snap} {
		b, err := proto.Marshal(m)
		if err != nil {
			return nil, err
		}
		out = binary.BigEndian.AppendUint32(out, uint32(len(b)))
		out = append(out, b...)
	}
	return out, nil
}

func (s *Store) loadCheckpoint(payload []byte) (uint64, error) {
	if len(payload) < 8 {
		return 0, errors.New("raftlog: short checkpoint")
	}
	replayFrom := binary.BigEndian.Uint64(payload)
	payload = payload[8:]
	next := func() ([]byte, error) {
		if len(payload) < 4 {
			return nil, errors.New("raftlog: short checkpoint section")
		}
		n := int(binary.BigEndian.Uint32(payload))
		payload = payload[4:]
		if len(payload) < n {
			return nil, errors.New("raftlog: short checkpoint section")
		}
		b := payload[:n]
		payload = payload[n:]
		return b, nil
	}
	hsb, err := next()
	if err != nil {
		return 0, err
	}
	snb, err := next()
	if err != nil {
		return 0, err
	}
	hs := &pb.HardState{}
	if err := proto.Unmarshal(hsb, hs); err != nil {
		return 0, fmt.Errorf("raftlog: checkpoint hard state: %w", err)
	}
	snap := &pb.Snapshot{}
	if err := proto.Unmarshal(snb, snap); err != nil {
		return 0, fmt.Errorf("raftlog: checkpoint snapshot: %w", err)
	}
	if snap.Metadata == nil {
		snap.Metadata = &pb.SnapshotMetadata{}
	}
	s.hs = hs
	s.snap = snap
	s.ents = []*pb.Entry{{Index: proto.Uint64(snap.Metadata.GetIndex()), Term: proto.Uint64(snap.Metadata.GetTerm())}}
	s.walIdx = []uint64{0}
	return replayFrom, nil
}

// replay applies one record to the in-memory index.
func (s *Store) replay(r wal.Record) error {
	switch r.Type {
	case recEntry:
		e := &pb.Entry{}
		if err := proto.Unmarshal(r.Payload, e); err != nil {
			return fmt.Errorf("raftlog: entry record %d: %w", r.Index, err)
		}
		if err := s.appendEntries([]*pb.Entry{e}, []uint64{r.Index}, false); err != nil {
			return err
		}
	case recHardState:
		hs := &pb.HardState{}
		if err := proto.Unmarshal(r.Payload, hs); err != nil {
			return fmt.Errorf("raftlog: hard state record %d: %w", r.Index, err)
		}
		s.hs = hs
	case recTruncate:
		if len(r.Payload) != 8 {
			return fmt.Errorf("raftlog: truncate record %d: length", r.Index)
		}
		s.truncateFrom(binary.BigEndian.Uint64(r.Payload))
	default:
		return fmt.Errorf("raftlog: unknown record type %d at %d", r.Type, r.Index)
	}
	return nil
}

func (s *Store) offset() uint64 { return s.ents[0].GetIndex() }
func (s *Store) last() uint64   { return s.offset() + uint64(len(s.ents)) - 1 }

// truncateFrom drops in-memory entries with index >= from.
func (s *Store) truncateFrom(from uint64) {
	if from <= s.offset() || from > s.last() {
		return
	}
	n := from - s.offset()
	s.ents = s.ents[:n]
	s.walIdx = s.walIdx[:n]
}

// appendEntries adds entries with their write-ahead indexes, dropping any
// already compacted and truncating a conflicting suffix. When write is set
// the truncation is also recorded in the log.
func (s *Store) appendEntries(ents []*pb.Entry, wals []uint64, write bool) error {
	if len(ents) == 0 {
		return nil
	}
	first := ents[0].GetIndex()
	if last := ents[len(ents)-1].GetIndex(); last <= s.offset() {
		return nil // all compacted
	}
	if first <= s.offset() {
		skip := s.offset() + 1 - first
		ents, wals = ents[skip:], wals[skip:]
		first = ents[0].GetIndex()
	}
	if first > s.last()+1 {
		return fmt.Errorf("raftlog: gap: appending %d after %d", first, s.last())
	}
	if first <= s.last() {
		if write {
			if _, err := s.log.Append(recTruncate, binary.BigEndian.AppendUint64(nil, first)); err != nil {
				return err
			}
		}
		s.truncateFrom(first)
	}
	s.ents = append(s.ents, ents...)
	s.walIdx = append(s.walIdx, wals...)
	return nil
}

// InitialState implements raft.Storage.
func (s *Store) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs := s.snap.Metadata.GetConfState()
	if cs == nil {
		cs = &pb.ConfState{}
	}
	return proto.Clone(s.hs).(*pb.HardState), proto.Clone(cs).(*pb.ConfState), nil
}

// Entries implements raft.Storage.
func (s *Store) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo <= s.offset() {
		return nil, raft.ErrCompacted
	}
	if hi > s.last()+1 {
		return nil, raft.ErrUnavailable
	}
	if len(s.ents) == 1 {
		return nil, raft.ErrUnavailable
	}
	// A fresh slice, with no spare capacity into the index: the library
	// appends its unstable entries to what Entries returns, which would
	// otherwise write into this store's backing array while the loop appends
	// to it.
	view := s.ents[lo-s.offset() : hi-s.offset()]
	ents := make([]*pb.Entry, len(view))
	copy(ents, view)
	return limitSize(ents, maxSize), nil
}

func limitSize(ents []*pb.Entry, maxSize uint64) []*pb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := uint64(proto.Size(ents[0]))
	var n int
	for n = 1; n < len(ents); n++ {
		size += uint64(proto.Size(ents[n]))
		if size > maxSize {
			break
		}
	}
	return ents[:n]
}

// Term implements raft.Storage.
func (s *Store) Term(i uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < s.offset() {
		return 0, raft.ErrCompacted
	}
	if i > s.last() {
		return 0, raft.ErrUnavailable
	}
	return s.ents[i-s.offset()].GetTerm(), nil
}

// LastIndex implements raft.Storage.
func (s *Store) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last(), nil
}

// FirstIndex implements raft.Storage.
func (s *Store) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offset() + 1, nil
}

// Snapshot implements raft.Storage.
func (s *Store) Snapshot() (*pb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(s.snap).(*pb.Snapshot), nil
}

// HasState reports whether the log holds anything a restart should resume
// from: a hard state, a snapshot or entries.
func (s *Store) HasState() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !raft.IsEmptyHardState(s.hs) || s.snap.Metadata.GetIndex() > 0 || len(s.ents) > 1
}

// Save persists a Ready batch: the snapshot (as a checkpoint), the entries
// and the hard state, in that order, and syncs when asked. The token it
// returns is what the sender requires.
func (s *Store) Save(hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot, sync bool) (Durable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap != nil && !raft.IsEmptySnap(snap) {
		if err := s.applySnapshot(snap); err != nil {
			return Durable{}, err
		}
		sync = true
	}
	for _, e := range ents {
		b, err := proto.Marshal(e)
		if err != nil {
			return Durable{}, err
		}
		// A conflicting suffix is recorded before the new entry.
		if e.GetIndex() <= s.last() && e.GetIndex() > s.offset() {
			if _, err := s.log.Append(recTruncate, binary.BigEndian.AppendUint64(nil, e.GetIndex())); err != nil {
				return Durable{}, err
			}
			s.truncateFrom(e.GetIndex())
		}
		idx, err := s.log.Append(recEntry, b)
		if err != nil {
			return Durable{}, err
		}
		if err := s.appendEntries([]*pb.Entry{e}, []uint64{idx}, false); err != nil {
			return Durable{}, err
		}
	}
	if hs != nil && !raft.IsEmptyHardState(hs) {
		b, err := proto.Marshal(hs)
		if err != nil {
			return Durable{}, err
		}
		if _, err := s.log.Append(recHardState, b); err != nil {
			return Durable{}, err
		}
		s.hs = proto.Clone(hs).(*pb.HardState)
	}
	if sync {
		if err := s.log.Sync(); err != nil {
			return Durable{}, err
		}
		s.durable++
	}
	return Durable{seq: s.durable}, nil
}

// applySnapshot installs an incoming snapshot: the index resets to it and a
// checkpoint is written so a restart starts from it.
func (s *Store) applySnapshot(snap *pb.Snapshot) error {
	if snap.Metadata.GetIndex() <= s.snap.Metadata.GetIndex() {
		return raft.ErrSnapOutOfDate
	}
	s.snap = proto.Clone(snap).(*pb.Snapshot)
	s.ents = []*pb.Entry{{Index: proto.Uint64(snap.Metadata.GetIndex()), Term: proto.Uint64(snap.Metadata.GetTerm())}}
	s.walIdx = []uint64{0}
	return s.writeCheckpoint()
}

func (s *Store) writeCheckpoint() error {
	payload, err := s.checkpointPayload()
	if err != nil {
		return err
	}
	idx := s.log.LastIndex()
	if err := s.log.WriteCheckpoint(idx, payload); err != nil {
		return err
	}
	s.ckptWal = idx
	s.durable++
	return nil
}

// CreateSnapshot records a snapshot of the state machine at index with the
// configuration state and data, as a checkpoint.
func (s *Store) CreateSnapshot(index uint64, cs *pb.ConfState, data []byte) (*pb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.snap.Metadata.GetIndex() {
		return nil, raft.ErrSnapOutOfDate
	}
	if index > s.last() {
		return nil, fmt.Errorf("raftlog: snapshot %d is beyond the last index %d", index, s.last())
	}
	term := s.ents[index-s.offset()].GetTerm()
	s.snap = &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(index), Term: proto.Uint64(term), ConfState: proto.Clone(cs).(*pb.ConfState)}}
	if err := s.writeCheckpoint(); err != nil {
		return nil, err
	}
	return proto.Clone(s.snap).(*pb.Snapshot), nil
}

// Compact drops entries below index from memory and removes the log
// segments that only hold records the latest checkpoint supersedes.
func (s *Store) Compact(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.offset() {
		return raft.ErrCompacted
	}
	if index > s.last() {
		return fmt.Errorf("raftlog: compact %d is beyond the last index %d", index, s.last())
	}
	i := index - s.offset()
	dummy := &pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(s.ents[i].GetTerm())}
	s.ents = append([]*pb.Entry{dummy}, s.ents[i+1:]...)
	s.walIdx = append([]uint64{0}, s.walIdx[i+1:]...)
	// Segments wholly before the checkpoint's replay point are redundant.
	before := s.ckptWal
	if len(s.walIdx) > 1 && s.walIdx[1] < before {
		before = s.walIdx[1]
	}
	if before > 0 {
		return s.log.Truncate(before)
	}
	return nil
}

// LastDurable is the latest token issued.
func (s *Store) LastDurable() Durable {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Durable{seq: s.durable}
}

// SnapshotIndex is the index of the stored snapshot.
func (s *Store) SnapshotIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap.Metadata.GetIndex()
}

// Close closes the log.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}
