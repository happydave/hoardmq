// Package store is the log-backed store provider: it journals session,
// queue and retained state to the write-ahead log, recovers the state from
// the latest checkpoint plus the records after it, and writes checkpoints.
// The session manager is the live state; this package only records and
// rebuilds it.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	extstore "github.com/happydave/hoardmq/ext/store"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/wal"
)

// SyncPolicy names when appended records are fsynced.
type SyncPolicy string

// Policies. Commit syncs before every QoS 1 or 2 acknowledgement; Relaxed
// syncs on an interval and is opt-in.
const (
	Commit  SyncPolicy = "commit"
	Relaxed SyncPolicy = "relaxed"
)

// Options configure the store.
type Options struct {
	SyncPolicy   SyncPolicy
	SyncInterval time.Duration
	SegmentBytes int64
	// CheckpointEvery is the number of appended records between checkpoints
	// requested through the Checkpointer; 0 uses the default of 10,000.
	CheckpointEvery int
	Logger          *slog.Logger
	// Sync is passed to the log for tests.
	Sync func(*os.File) error
	// GroupMaxDelay is how long a commit-policy Sync may hold before its
	// fsync to let concurrent callers join the group; 0 holds not at all and
	// groups only what arrives while an fsync is already in flight.
	// GroupMaxSize fires a held group early once that many callers joined;
	// it only matters while a hold is in progress. Defaults: 0 and 256.
	GroupMaxDelay time.Duration
	GroupMaxSize  int
	// IgnoreChecksum is passed to the log; the simulation harness's
	// injected defect.
	IgnoreChecksum bool
	// Tap, when set, receives every record after it is appended, in append
	// order and under the store's lock: it must return quickly. The
	// replication layer uses it to fan records out to followers. What it
	// returns, when not nil, runs once the lock is released, on the
	// appending goroutine, and may take as long as it needs — work that
	// belongs to the record but not to the order it was appended in.
	Tap func(Record) func()
}

// The record's key, scope and the record itself are the extension API's.
type (
	Key     = extstore.Key
	Scope   = extstore.Scope
	Record  = extstore.Record
	Journal = extstore.Journal
)

// Store is the log-backed provider.
type Store struct {
	opts Options
	log  *slog.Logger
	wal  *wal.Log

	mu           sync.Mutex
	appended     int
	needCheckpt  bool
	stop         chan struct{}
	stopped      chan struct{}
	closed       bool
	relaxedError error

	syncer  groupSyncer
	appends appendCounters
	// syncFailures counts every fsync of the log that returned an error.
	syncFailures atomic.Int64

	// The journal is whole while syncFailures equals wholeAt: nothing a
	// failed fsync may have dropped lies in what recovery reads. Under the
	// commit policy kept holds every record appended after the last index a
	// clean fsync, a clean checkpoint sync or a repair covered; keptFrom is
	// the index of kept[0]. All guarded by mu.
	wholeAt     int64
	kept        []keptRecord
	keptFrom    uint64
	keptBytes   int
	repairAfter time.Time
	repairErr   error

	covered        atomic.Uint64 // the last index covered, as above
	keptSyncing    atomic.Bool   // a Sync started for the kept records is running
	repairs        atomic.Int64
	repairFailures atomic.Int64

	// appendedIdx is the index of the last record appended, stored under mu
	// and read without it by a checkpoint's snapshot (Snapshot).
	appendedIdx atomic.Uint64
	// checkpointMu serializes checkpoints; it is taken before mu. rewrites,
	// under mu, counts the repairs that wrote a checkpoint, so a checkpoint
	// whose state was read before one does not replace it.
	checkpointMu sync.Mutex
	rewrites     uint64

	checkpoints         atomic.Int64
	checkpointNanos     atomic.Int64
	checkpointHoldNanos atomic.Int64

	// The index of the last checkpoint written or recovered and when this
	// process last wrote one (at open, the open time); every fsync of the
	// log and the time they took. Read lock-free by JournalStats.
	checkpointIdx atomic.Uint64
	checkpointAt  atomic.Int64
	fsyncs        atomic.Int64
	fsyncNanos    atomic.Int64
}

// JournalStats is the extension API's journal statistics.
type JournalStats = extstore.JournalStats

// JournalStats reads the journal's counters without the store's lock.
func (s *Store) JournalStats() JournalStats {
	return JournalStats{
		LastIndex: s.appendedIdx.Load(), CheckpointIndex: s.checkpointIdx.Load(),
		CheckpointAt: time.Unix(0, s.checkpointAt.Load()), Fsyncs: s.fsyncs.Load(),
		FsyncSec: float64(s.fsyncNanos.Load()) / 1e9, Sync: s.syncer.stats(),
	}
}

// noteCheckpoint records a checkpoint written at idx.
func (s *Store) noteCheckpoint(idx uint64) {
	s.checkpointIdx.Store(idx)
	s.checkpointAt.Store(time.Now().UnixNano())
}

// keptRecord is an appended record held until the disk is known to have it.
type keptRecord struct {
	typ     byte
	payload []byte
}

// Past either bound the kept records are synced in the background, for the
// records no caller's Sync follows.
var (
	keepRecords = 4096
	keepBytes   = 16 << 20
)

// repairBackoff is how long after a failed repair the next one may start; a
// Sync meanwhile fails with the failed repair's error.
var repairBackoff = time.Second

// Open opens the state log under dir/logs and starts the relaxed sync loop
// when configured.
func Open(dir string, opts Options) (*Store, error) {
	if opts.SyncPolicy == "" {
		opts.SyncPolicy = Commit
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 100 * time.Millisecond
	}
	if opts.CheckpointEvery <= 0 {
		opts.CheckpointEvery = 10000
	}
	if opts.GroupMaxSize <= 0 {
		opts.GroupMaxSize = 256
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	s := &Store{opts: opts, log: opts.Logger.With("component", "store"), stop: make(chan struct{}), stopped: make(chan struct{})}
	l, err := wal.Open(dir, "state", wal.Options{SegmentBytes: opts.SegmentBytes, Sync: s.countFailures(opts.Sync), Logger: opts.Logger, IgnoreChecksum: opts.IgnoreChecksum})
	if err != nil {
		return nil, err
	}
	s.wal = l
	s.syncer.store = s
	s.keptFrom = l.LastIndex() + 1
	s.covered.Store(l.LastIndex())
	s.appendedIdx.Store(l.LastIndex())
	s.checkpointAt.Store(time.Now().UnixNano())
	if opts.SyncPolicy == Relaxed {
		go s.relaxedLoop()
	} else {
		close(s.stopped)
	}
	return s, nil
}

func (s *Store) relaxedLoop() {
	defer close(s.stopped)
	t := time.NewTicker(s.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.mu.Lock()
			if !s.closed {
				if err := s.wal.Sync(); err != nil {
					s.relaxedError = err
					s.log.Error("relaxed sync failed", "err", err)
				}
			}
			s.mu.Unlock()
		case <-s.stop:
			return
		}
	}
}

// Sync makes every journaled record durable under the commit policy; under
// the relaxed policy it returns at once. Under commit the fsync is shared:
// callers that arrive while one is in flight are covered by the next, and
// none of them holds the store's lock while the disk works (see
// groupSyncer).
func (s *Store) Sync() error {
	if s.opts.SyncPolicy == Relaxed {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.relaxedError
	}
	return s.syncer.sync()
}

// SyncStats reports the group-commit counters, lock-free.
func (s *Store) SyncStats() SyncStats { return s.syncer.stats() }

// SyncFailures is how many fsyncs of the log have failed, lock-free. A failed
// fsync may drop every page that was dirty when it ran, and a later one that
// succeeds does not write them again, so a caller that wrote before a failure
// and is released by a later fsync reads the count to learn that its records
// may not be on the disk.
func (s *Store) SyncFailures() int64 { return s.syncFailures.Load() }

// wholeLocked reports whether the journal is whole, given the failure count the
// caller read. The caller holds s.mu.
func (s *Store) wholeLocked(failures int64) bool { return failures == s.wholeAt }

// markCovered records that every record up to idx is on the disk or in a
// checkpoint, so it need not be kept.
func (s *Store) markCovered(idx uint64) {
	for {
		c := s.covered.Load()
		if idx <= c || s.covered.CompareAndSwap(c, idx) {
			return
		}
	}
}

// keepLocked holds an appended record until it is covered, and starts a Sync
// in the background when the kept records pass their bounds.
func (s *Store) keepLocked(typ byte, payload []byte) {
	s.trimKeptLocked()
	s.kept = append(s.kept, keptRecord{typ: typ, payload: payload})
	s.keptBytes += len(payload)
	if (len(s.kept) >= keepRecords || s.keptBytes >= keepBytes) && s.keptSyncing.CompareAndSwap(false, true) {
		go func() {
			defer s.keptSyncing.Store(false)
			_ = s.Sync()
		}()
	}
}

// trimKeptLocked drops the kept records that are covered.
func (s *Store) trimKeptLocked() {
	c := s.covered.Load()
	if c < s.keptFrom {
		return
	}
	n := int(c - s.keptFrom + 1)
	if n >= len(s.kept) {
		s.kept, s.keptBytes = nil, 0
		s.keptFrom = c + 1
		return
	}
	for i := 0; i < n; i++ {
		s.keptBytes -= len(s.kept[i].payload)
		s.kept[i] = keptRecord{}
	}
	s.kept = s.kept[n:]
	s.keptFrom = c + 1
}

var errStopReplay = errors.New("store: replay stopped")

// stateAtLocked rebuilds the state recovery would build from an undamaged log
// at its last index: the latest checkpoint, the logged records after it that
// clean fsyncs covered, and the kept records. The caller holds s.mu.
func (s *Store) stateAtLocked() (*State, error) {
	st := NewState()
	from := uint64(1)
	var held *heldRecords
	if idx, payload, ok := s.wal.LatestCheckpoint(); ok {
		if err := json.Unmarshal(payload, st); err != nil {
			return nil, fmt.Errorf("store: checkpoint %d: %w", idx, err)
		}
		from = idx + 1
		held = heldBy(st)
	}
	if from < s.keptFrom {
		err := s.wal.Iterate(from, func(r wal.Record) error {
			if r.Index >= s.keptFrom {
				return errStopReplay
			}
			return replayOnto(st, held, r.Index, r.Type, r.Payload)
		})
		if err != nil && !errors.Is(err, errStopReplay) {
			return nil, err
		}
	}
	for i, k := range s.kept {
		idx := s.keptFrom + uint64(i)
		if idx < from {
			continue
		}
		if err := replayOnto(st, held, idx, k.typ, k.payload); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// replayOnto applies one record after a checkpoint to the state recovered
// from it, unless the checkpoint's state already holds it.
func replayOnto(st *State, held *heldRecords, index uint64, typ byte, payload []byte) error {
	if held.holds(index, typ, payload) {
		return nil
	}
	if err := st.Apply(typ, payload); err != nil {
		return fmt.Errorf("store: record %d: %w", index, err)
	}
	return nil
}

// repairLocked makes the journal whole after a failed fsync. It writes the
// state at the last index as a checkpoint, which that file's own fsync makes
// durable, moves appends to a new segment without syncing the active one, and
// drops every segment before it, so recovery reads nothing a failed fsync may
// have dropped. The caller holds s.mu.
func (s *Store) repairLocked() error {
	if s.closed {
		return errClosed
	}
	now := time.Now()
	if now.Before(s.repairAfter) {
		return s.repairErr
	}
	failures := s.syncFailures.Load()
	last, err := s.rewriteLocked()
	if err == nil && s.syncFailures.Load() != failures {
		err = errors.New("an fsync failed during the repair")
	}
	if err != nil {
		s.repairFailures.Add(1)
		s.repairErr = fmt.Errorf("store: journal repair: %w", err)
		s.repairAfter = now.Add(repairBackoff)
		s.log.Error("journal repair failed", "err", err, "retry_after", repairBackoff)
		return s.repairErr
	}
	s.wholeAt = failures
	s.repairs.Add(1)
	s.log.Warn("journal rewritten after a failed fsync", "index", last)
	return nil
}

func (s *Store) rewriteLocked() (uint64, error) {
	last := s.wal.LastIndex()
	st, err := s.stateAtLocked()
	if err != nil {
		return last, err
	}
	payload, err := json.Marshal(st)
	if err != nil {
		return last, err
	}
	s.rewrites++
	if err := s.wal.WriteCheckpoint(last, payload); err != nil {
		return last, err
	}
	s.noteCheckpoint(last)
	if err := s.wal.StartSegment(); err != nil {
		return last, err
	}
	if err := s.wal.Truncate(last + 1); err != nil {
		return last, err
	}
	s.appended, s.needCheckpt = 0, false
	s.coverLocked(last)
	return last, nil
}

// coverLocked records idx as covered for the syncer and the kept records.
// The caller holds s.mu, which may take the syncer's lock.
func (s *Store) coverLocked(idx uint64) {
	g := &s.syncer
	g.mu.Lock()
	if idx > g.synced {
		g.synced = idx
	}
	g.mu.Unlock()
	s.markCovered(idx)
	s.trimKeptLocked()
}

// countFailures wraps the log's fsync so a failure is counted before the
// caller that made it, and every caller released after it, can observe the
// result.
func (s *Store) countFailures(sync func(*os.File) error) func(*os.File) error {
	if sync == nil {
		sync = func(f *os.File) error { return f.Sync() }
	}
	return func(f *os.File) error {
		began := time.Now()
		err := sync(f)
		s.fsyncNanos.Add(int64(time.Since(began)))
		s.fsyncs.Add(1)
		if err != nil {
			s.syncFailures.Add(1)
		}
		return err
	}
}

// NeedsCheckpoint reports whether enough records have accumulated since the
// last checkpoint; the owner of the live state then calls Checkpoint.
// While the journal is not whole it reports true, and Checkpoint repairs it.
func (s *Store) NeedsCheckpoint() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.needCheckpt || (s.opts.SyncPolicy != Relaxed && !s.wholeLocked(s.syncFailures.Load()))
}

// appendRecord appends an encoded record and taps it.
func (s *Store) appendRecord(r Record, err error) error {
	if err != nil {
		return err
	}
	after, err := s.appendAndTap(r.Type, r.Key, r.Payload)
	if err != nil {
		return err
	}
	if after != nil {
		after()
	}
	return nil
}

// appendAndTap writes the record and calls the tap, both under the lock, and
// returns what the tap asked to have run after the lock is released.
func (s *Store) appendAndTap(typ byte, k Key, payload []byte) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	locked := time.Now()
	if err := s.appendLocked(typ, payload, false); err != nil {
		return nil, err
	}
	var after func()
	var tap time.Duration
	if s.opts.Tap != nil {
		tapped := time.Now()
		after = s.opts.Tap(Record{Type: typ, Key: k, Payload: payload})
		tap = time.Since(tapped)
	}
	s.appends.note(time.Since(locked), tap)
	return after, nil
}

// appendLocked writes a record and, under the commit policy, keeps it; a
// payload the caller still owns is kept as a copy.
func (s *Store) appendLocked(typ byte, payload []byte, borrowed bool) error {
	if s.closed {
		return errors.New("store: closed")
	}
	idx, err := s.wal.Append(typ, payload)
	if err != nil {
		return err
	}
	s.appendedIdx.Store(idx)
	if s.opts.SyncPolicy != Relaxed {
		if borrowed {
			payload = append([]byte(nil), payload...)
		}
		s.keepLocked(typ, payload)
	}
	s.appended++
	if s.appended >= s.opts.CheckpointEvery {
		s.needCheckpt = true
	}
	return nil
}

// AppendRaw appends a record received from another node verbatim; the tap
// is not called. The type must be one this package writes.
func (s *Store) AppendRaw(typ byte, payload []byte) error {
	if typ < recSessionUpsert || typ > recRetainDelete {
		return fmt.Errorf("store: unknown record type %d", typ)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	locked := time.Now()
	err := s.appendLocked(typ, payload, true)
	s.appends.note(time.Since(locked), 0)
	return err
}

// SetTap installs or clears the tap after Open.
func (s *Store) SetTap(tap func(Record) func()) {
	s.mu.Lock()
	s.opts.Tap = tap
	s.mu.Unlock()
}

// Recover rebuilds the state from the latest checkpoint and the records
// after it. An unknown record type or a checksum failure is fatal.
func (s *Store) Recover() (*State, error) {
	state := NewState()
	from := uint64(1)
	var held *heldRecords
	if idx, payload, ok := s.wal.LatestCheckpoint(); ok {
		if err := json.Unmarshal(payload, state); err != nil {
			return nil, fmt.Errorf("store: checkpoint %d: %w", idx, err)
		}
		from = idx + 1
		held = heldBy(state)
		s.checkpointIdx.Store(idx)
		if idx > s.wal.LastIndex() {
			s.log.Warn("checkpoint is newer than the log; using the checkpoint", "checkpoint", idx, "log", s.wal.LastIndex())
		}
	}
	err := s.wal.Iterate(from, func(r wal.Record) error {
		return replayOnto(state, held, r.Index, r.Type, r.Payload)
	})
	if err != nil {
		return nil, err
	}
	state.Normalize()
	return state, nil
}

// Snapshot is the extension API's: the live state a checkpoint writes.
type Snapshot = extstore.Snapshot

// Checkpoint writes the state the snapshot returns at the index read before
// it, or at its State.At, and drops the segments before that. While the
// journal is not whole it repairs instead: the snapshot is the caller's view,
// and the repair's is the log's.
func (s *Store) Checkpoint(snapshot Snapshot) error {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	began := time.Now()
	commit := s.opts.SyncPolicy != Relaxed
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errClosed
	}
	idx := s.wal.LastIndex()
	rewrites := s.rewrites
	s.mu.Unlock()

	state := snapshot(s.appendedIdx.Load)
	if state.At != 0 {
		if state.At < idx {
			return fmt.Errorf("store: checkpoint: snapshot exact at %d, before the index %d read ahead of it", state.At, idx)
		}
		idx = state.At
	}
	// An entry at or below the index names records the checkpoint replaces;
	// it would skip nothing.
	var through []SessionIndex
	for _, t := range state.Through {
		if t.Index > idx {
			through = append(through, t)
		}
	}
	state.Through = through
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	held := time.Now()
	if s.closed {
		return errClosed
	}
	failures := s.syncFailures.Load()
	if commit && !s.wholeLocked(failures) {
		return s.repairLocked()
	}
	if s.rewrites != rewrites {
		// A repair wrote the log's state at a later index and dropped the
		// segments before it; this one would replace it.
		return nil
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	synced := s.wal.LastIndex()
	if commit {
		if s.syncFailures.Load() != failures {
			return errors.New("store: checkpoint: another fsync of the journal failed during its sync")
		}
		s.coverLocked(synced)
	}
	if err := s.wal.WriteCheckpoint(idx, payload); err != nil {
		return err
	}
	s.noteCheckpoint(idx)
	if err := s.wal.Truncate(idx + 1); err != nil {
		return err
	}
	s.appended = int(synced - idx)
	s.needCheckpt = s.appended >= s.opts.CheckpointEvery
	now := time.Now()
	s.checkpoints.Add(1)
	s.checkpointNanos.Add(int64(now.Sub(began)))
	s.checkpointHoldNanos.Add(int64(now.Sub(held)))
	return nil
}

// Close stops the sync loop and closes the log.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.stop)
	<-s.stopped
	// Under the store lock: Close rewrites the log's state that Flush reads
	// there, and the log's own rotation lock makes it wait for an fsync in
	// flight outside the lock.
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.Close()
}

// LastIndex exposes the log position, for tests and the admin surface.
func (s *Store) LastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wal.LastIndex()
}

// --- Journal methods (the session package's Journal interface) ---

// SessionUpsert records a session's metadata.
func (s *Store) SessionUpsert(meta SessionMeta) error {
	return s.appendRecord(extstore.EncodeSessionUpsert(meta))
}

// SessionDelete records a session's end.
func (s *Store) SessionDelete(k Key) error {
	return s.appendRecord(extstore.EncodeSessionDelete(k))
}

// Subscribe records a subscription add or replace.
func (s *Store) Subscribe(k Key, sub SubscriptionRecord) error {
	return s.appendRecord(extstore.EncodeSubscribe(k, sub))
}

// Unsubscribe records a subscription removal.
func (s *Store) Unsubscribe(k Key, filter string) error {
	return s.appendRecord(extstore.EncodeUnsubscribe(k, filter))
}

// InboundQoS2 records an inbound QoS 2 identifier added or removed.
func (s *Store) InboundQoS2(k Key, packetID uint16, add bool) error {
	return s.appendRecord(extstore.EncodeInboundQoS2(k, packetID, add))
}

// InboundAccepted records that the publish of an open inbound QoS 2
// identifier was accepted.
func (s *Store) InboundAccepted(k Key, packetID uint16) error {
	return s.appendRecord(extstore.EncodeInboundAccepted(k, packetID))
}

// Enqueue records a message entering a session's queue.
func (s *Store) Enqueue(k Key, e Entry) error {
	return s.appendRecord(extstore.EncodeEnqueue(k, e))
}

// Sent records an entry taking a packet identifier (queued to in flight).
func (s *Store) Sent(k Key, entrySeq uint64, packetID uint16) error {
	return s.appendRecord(extstore.EncodeSent(k, entrySeq, packetID))
}

// InflightState records PUBREC received for an in-flight entry.
func (s *Store) InflightState(k Key, packetID uint16, state byte) error {
	return s.appendRecord(extstore.EncodeInflightState(k, packetID, state))
}

// Complete records an in-flight entry's completion.
func (s *Store) Complete(k Key, packetID uint16) error {
	return s.appendRecord(extstore.EncodeComplete(k, packetID))
}

// Dropped records a queued entry dropped by the queue limit.
func (s *Store) Dropped(k Key, entrySeq uint64) error {
	return s.appendRecord(extstore.EncodeDropped(k, entrySeq))
}

// Retain records a retained message stored.
func (s *Store) Retain(m *message.Message) error {
	return s.appendRecord(extstore.EncodeRetain(m))
}

// RetainDelete records a retained message removed.
func (s *Store) RetainDelete(ns, topic string) error {
	return s.appendRecord(extstore.EncodeRetainDelete(ns, topic))
}
