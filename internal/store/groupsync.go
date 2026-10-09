package store

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	extstore "github.com/happydave/hoardmq/ext/store"
)

// groupSyncer implements group commit for the commit policy. One fsync is
// in flight at a time and it runs outside the store's lock; every caller
// whose append was flushed before that fsync started is released by it, and
// callers arriving while it runs join the next group, which starts the
// moment the current one finishes. A caller therefore waits at most two
// fsyncs, and the disk sees one fsync per group rather than one per
// acknowledgement — the floor WI 1292 round 3 measured at ~380 operations
// a second per node.
//
// Durability: a caller is released only by an fsync that was
// issued after its own append reached the file (the leader flushes under
// the store lock and records the covered position before it syncs), and
// only if no fsync of the store failed between the group's seal and its
// release. After a failed fsync the journal is not whole, and the next group
// repairs it instead of syncing (WI 1812).
type groupSyncer struct {
	store *Store

	mu     sync.Mutex
	synced uint64     // WAL index covered by the last completed fsync
	cur    *syncGroup // the group in flight or collecting, nil when idle
	next   *syncGroup // the group waiting for cur to finish

	// afterTarget, when set by a test, runs in lead after the leader has read
	// its covered index and released the store lock.
	afterTarget func()

	syncs      atomic.Int64
	ops        atomic.Int64
	callers    atomic.Int64
	waitNanos  atomic.Int64
	heldGroups atomic.Int64
}

// syncGroup is one shared fsync and the callers it releases.
type syncGroup struct {
	joined int
	sealed bool   // the leader has flushed; late arrivals go to the next group
	target uint64 // WAL index covered, known once sealed
	full   chan struct{}
	lead   chan struct{} // hands leadership to one waiter of a promoted group
	done   chan struct{}
	err    error
}

func newSyncGroup() *syncGroup {
	return &syncGroup{full: make(chan struct{}), lead: make(chan struct{}, 1), done: make(chan struct{})}
}

// SyncStats is the extension API's group-commit statistics.
type SyncStats = extstore.SyncStats

// coveredIndex is the WAL index the last completed fsync covered.
func (g *groupSyncer) coveredIndex() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.synced
}

func (g *groupSyncer) stats() SyncStats {
	s := g.store
	return SyncStats{Syncs: g.syncs.Load(), Ops: g.ops.Load(), Callers: g.callers.Load(),
		WaitSec: float64(g.waitNanos.Load()) / 1e9, HeldGroups: g.heldGroups.Load(),
		Failures: s.syncFailures.Load(), Repairs: s.repairs.Load(), RepairFailures: s.repairFailures.Load(),
		Checkpoints: s.checkpoints.Load(), CheckpointSec: float64(s.checkpointNanos.Load()) / 1e9,
		CheckpointHoldSec: float64(s.checkpointHoldNanos.Load()) / 1e9}
}

var errClosed = errors.New("store: closed")

// errCovered is a group's result when its fsync succeeded but another fsync
// of the store failed after the group was sealed: the failure may have dropped
// the group's pages, and the kernel reported it to the other call. Its
// callers sync again, and meet the repair.
var errCovered = errors.New("store: an fsync of the journal failed while the group's ran")

// sync is Store.Sync under the commit policy.
func (g *groupSyncer) sync() error {
	start := time.Now()
	g.callers.Add(1)
	defer func() { g.waitNanos.Add(int64(time.Since(start))) }()
	for {
		if err := g.syncOnce(); !errors.Is(err, errCovered) {
			return err
		}
	}
}

func (g *groupSyncer) syncOnce() error {
	s := g.store

	// Flush under the store lock so the caller's records are in the file,
	// and note how many appends that covers.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errClosed
	}
	if _, _, err := s.wal.Flush(); err != nil {
		s.mu.Unlock()
		return err
	}
	want := s.wal.LastIndex()
	s.mu.Unlock()

	g.mu.Lock()
	if g.synced >= want {
		g.mu.Unlock()
		return nil
	}
	if g.cur == nil {
		// Idle: this caller leads a new group.
		grp := newSyncGroup()
		grp.joined = 1
		g.cur = grp
		g.mu.Unlock()
		return g.lead(grp)
	}
	if !g.cur.sealed {
		// The leader is still holding to collect callers: join it.
		grp := g.cur
		grp.joined++
		if grp.joined >= s.opts.GroupMaxSize {
			select {
			case <-grp.full:
			default:
				close(grp.full)
			}
		}
		g.mu.Unlock()
		<-grp.done
		return grp.err
	}
	if want <= g.cur.target {
		// Sealed but covering this caller's append: its fsync suffices.
		grp := g.cur
		g.mu.Unlock()
		<-grp.done
		return grp.err
	}
	// In flight and not covering us: wait for the next group. One of its
	// waiters is handed leadership when the current group finishes.
	if g.next == nil {
		g.next = newSyncGroup()
	}
	grp := g.next
	grp.joined++
	g.mu.Unlock()
	select {
	case <-grp.lead:
		return g.lead(grp)
	case <-grp.done:
		return grp.err
	}
}

// lead runs one group: optional hold, seal (final flush + covered position
// under the store lock), fsync outside it, release, and hand-off. A group
// sealed while the journal is not whole repairs it under the store lock
// instead.
func (g *groupSyncer) lead(grp *syncGroup) error {
	s := g.store
	if d := s.opts.GroupMaxDelay; d > 0 {
		g.heldGroups.Add(1)
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-grp.full:
			t.Stop()
		}
	}

	s.mu.Lock()
	var f *os.File
	var gen uint64
	var err error
	failures := s.syncFailures.Load()
	whole := s.wholeLocked(failures)
	switch {
	case s.closed:
		err = errClosed
	case !whole:
		err = s.repairLocked()
	default:
		f, gen, err = s.wal.Flush()
	}
	target := s.wal.LastIndex()
	// Sealed before the store lock is released: a caller that appends after
	// this flush can only reach the syncer once the group is sealed, and then
	// waits for a later fsync instead of joining this one, which cannot cover
	// its record (WI 1490). The syncer lock is only ever taken inside the store
	// lock, never the reverse.
	g.mu.Lock()
	grp.sealed, grp.target = true, target
	g.mu.Unlock()
	s.mu.Unlock()
	if g.afterTarget != nil {
		g.afterTarget()
	}

	if whole && err == nil {
		err = s.wal.Fsync(f, gen)
		switch {
		case err != nil:
			s.log.Error("journal fsync failed", "index", target, "err", err)
		case s.syncFailures.Load() != failures:
			s.log.Error("another fsync of the journal failed while this one ran", "index", target)
			err = errCovered
		}
	}

	g.mu.Lock()
	if err == nil {
		if target > g.synced {
			g.ops.Add(int64(target - g.synced))
			g.synced = target
		}
		if whole {
			g.syncs.Add(1)
		}
		s.markCovered(target)
	}
	grp.err = err
	promoted := g.next
	g.cur, g.next = promoted, nil
	g.mu.Unlock()
	close(grp.done)
	if promoted != nil {
		// Exactly one waiter of the promoted group takes the lead; the
		// buffered channel makes the hand-off non-blocking.
		promoted.lead <- struct{}{}
	}
	return err
}
