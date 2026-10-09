package lease

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// An adopted lease's state is fetched once, by the adoption worker or by the
// client's own CONNECT, whichever claims it first. Importing it evicts what
// the node holds for the key, so a client admitted while the fetch is
// unfinished would be disconnected when it lands; and the import cannot be
// skipped to spare that connection, because the state carries the session's
// queued messages. So a CONNECT for such a key fetches the state itself, or
// waits for whoever is fetching it.

type adoptionState int

const (
	adoptionNone    adoptionState = iota // nothing to adopt, or already done
	adoptionQueued                       // waiting for the worker; nobody has it
	adoptionRunning                      // someone is fetching it
)

type adoptionAction int

const (
	adoptProceed adoptionAction = iota // connect as the same owner
	adoptFetch                         // claim the adoption and fetch its state in the handshake
	adoptDiscard                       // claim it and fetch nothing: a clean start discards the state
	adoptWait                          // wait for whoever has it
)

// decideAdoption is what a CONNECT does about its key's adoption.
func decideAdoption(st adoptionState, cleanStart bool) adoptionAction {
	switch st {
	case adoptionQueued:
		if cleanStart {
			return adoptDiscard
		}
		return adoptFetch
	case adoptionRunning:
		return adoptWait
	default:
		return adoptProceed
	}
}

// adoption is one unfinished adoption. done closes when it ends.
type adoption struct {
	job     adoptJob
	running bool
	done    chan struct{}
	// begun is when the adoption was recorded, for classifying a connection
	// its import finds already here. A newer job that replaces a queued one
	// keeps it: the key has had an adoption recorded since then.
	begun time.Time
	// attempts is how many of the worker's fetches for it went unanswered,
	// and spent how many of those waited on no node alive by the log: only
	// those count towards giving it up.
	attempts, spent int
	// past is set once a fetch finds it waiting on a previous owner alive by
	// the log after SealTimeout.
	past bool
	// late is the session's state from a takeover acknowledgement that arrived
	// after its pull had given up. The previous owner removed the session to
	// send it, so until someone takes it this is where the session is.
	late *store.SessionState
	// answeredEmpty is when a takeover pass here answered the key with no
	// state while this adoption was unfinished; the import trace reads it.
	answeredEmpty time.Time
}

type adoptionSet struct {
	mu sync.Mutex
	m  map[key]*adoption
	// idleNs is when the set last became empty, wall-clock nanoseconds, and
	// onIdle is told each time it does, after the lock is released.
	idleNs atomic.Int64
	onIdle func()
}

func newAdoptionSet() *adoptionSet { return &adoptionSet{m: map[key]*adoption{}} }

// begin records an adoption as queued. A newer adoption of a key still
// queued replaces its job; one whose fetch is running is left to finish, and
// the newer one queues behind it as its own record.
func (a *adoptionSet) begin(k key, job adoptJob) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if cur := a.m[k]; cur != nil && !cur.running {
		if cur.job.epoch != job.epoch {
			cur.late = nil
		}
		cur.job, cur.attempts, cur.spent = job, 0, 0
		return
	}
	a.m[k] = &adoption{job: job, done: make(chan struct{}), begun: time.Now()}
}

// claim reports the key's adoption and its state as found, and takes it when
// it was queued: the state and the taking are one step, or the worker and a
// CONNECT could both fetch.
func (a *adoptionSet) claim(k key) (*adoption, adoptionState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := a.m[k]
	switch {
	case cur == nil:
		return nil, adoptionNone
	case cur.running:
		return cur, adoptionRunning
	default:
		cur.running = true
		return cur, adoptionQueued
	}
}

// end finishes an adoption its caller claimed and releases the waiters. It
// returns a late state that reached the record after its fetcher last looked.
func (a *adoptionSet) end(k key, ad *adoption) *store.SessionState {
	a.mu.Lock()
	idle := false
	if a.m[k] == ad {
		delete(a.m, k)
		idle = a.emptiedLocked()
	}
	late := ad.late
	ad.late = nil
	a.mu.Unlock()
	close(ad.done)
	a.tellIdle(idle)
	return late
}

// emptiedLocked records the moment a removal left the set empty. Callers
// hold mu.
func (a *adoptionSet) emptiedLocked() bool {
	if len(a.m) != 0 {
		return false
	}
	a.idleNs.Store(time.Now().UnixNano())
	return true
}

// idleAt is when the set last became empty; zero if it never has, or for a
// service built without one.
func (a *adoptionSet) idleAt() int64 {
	if a == nil {
		return 0
	}
	return a.idleNs.Load()
}

func (a *adoptionSet) tellIdle(idle bool) {
	if idle && a.onIdle != nil {
		a.onIdle()
	}
}

// giveBack returns a claimed adoption to the queued state after a fetch that
// settled nothing, counting the attempt when the worker made it, and as spent
// when spend is set. Whoever waits on the claim is released to claim it
// afresh. mine is false when a newer adoption has taken the key: that one has
// its own queue entry.
func (a *adoptionSet) giveBack(k key, ad *adoption, count, spend bool) (attempts int, mine bool) {
	a.mu.Lock()
	if mine = a.m[k] == ad; mine {
		attempts = ad.attempts
		spent := ad.spent
		if count {
			attempts++
		}
		if spend {
			spent++
		}
		a.m[k] = &adoption{job: ad.job, done: make(chan struct{}), begun: ad.begun, attempts: attempts, spent: spent, past: ad.past, late: ad.late}
		ad.late = nil
	}
	a.mu.Unlock()
	close(ad.done)
	return attempts, mine
}

// deliver attaches a late state to the key's adoption at the state's token,
// recording one when there is none: whoever fetches the adoption next takes
// the state instead of asking for it.
func (a *adoptionSet) deliver(k key, job adoptJob, st *store.SessionState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := a.m[k]
	switch {
	case cur == nil, cur.running && cur.job.epoch != job.epoch:
		a.m[k] = &adoption{job: job, done: make(chan struct{}), begun: time.Now(), late: st}
	case cur.job.epoch != job.epoch:
		cur.job, cur.attempts, cur.spent, cur.late = job, 0, 0, st
	default:
		cur.late = st
	}
}

// takeLate removes and returns the late state of an adoption its caller has
// claimed.
func (a *adoptionSet) takeLate(ad *adoption) *store.SessionState {
	a.mu.Lock()
	defer a.mu.Unlock()
	st := ad.late
	ad.late = nil
	return st
}

// putLate returns a late state its caller could not use yet.
func (a *adoptionSet) putLate(ad *adoption, st *store.SessionState) {
	a.mu.Lock()
	if ad.late == nil {
		ad.late = st
	}
	a.mu.Unlock()
}

// takeLateFor removes and returns the key's late state for a node that has
// taken the lease over. The state leaves with it, so an adoption nobody has
// claimed is over; one being fetched finds the lease gone when it looks.
func (a *adoptionSet) takeLateFor(k key) *store.SessionState {
	a.mu.Lock()
	ad := a.m[k]
	if ad == nil || ad.late == nil {
		a.mu.Unlock()
		return nil
	}
	st := ad.late
	ad.late = nil
	idle := false
	if !ad.running {
		delete(a.m, k)
		idle = a.emptiedLocked()
	}
	running := ad.running
	a.mu.Unlock()
	if !running {
		close(ad.done)
	}
	a.tellIdle(idle)
	return st
}

// abandon ends an adoption nobody will finish — its work-queue key was
// dropped — releasing whoever waits on it.
func (a *adoptionSet) abandon(k key) {
	a.mu.Lock()
	ad := a.m[k]
	delete(a.m, k)
	idle := ad != nil && a.emptiedLocked()
	a.mu.Unlock()
	if ad != nil {
		close(ad.done)
	}
	a.tellIdle(idle)
}

// unfinishedFor reports whether the key has an adoption nobody has finished.
func (a *adoptionSet) unfinishedFor(k key) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.m[k] != nil
}

// lateFor reports whether the key has an unfinished adoption that a
// reassignment recorded: one a CONNECT can find only because it applied while
// the CONNECT's acquire was proposed.
func (a *adoptionSet) lateFor(k key) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	ad := a.m[k]
	return ad != nil && !ad.job.takeover
}

// markPast sets a claimed adoption's past mark, reporting whether it was
// unset.
func (a *adoptionSet) markPast(ad *adoption) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	was := ad.past
	ad.past = true
	return !was
}

// pastBound counts the unfinished adoptions with the past mark; zero for a
// service built without a set.
func (a *adoptionSet) pastBound() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, ad := range a.m {
		if ad.past {
			n++
		}
	}
	return n
}

func (a *adoptionSet) unfinished() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.m)
}

// AdoptionState is the log index this node has applied with its change hooks
// run, and how many adoptions it has not finished, sessions in transit
// (transit.go) included. Read together they say whether the sessions
// reassigned to this node up to that index are served here: an adoption ends
// after its state is imported, and the import advertises the session's routes
// before it returns.
func (s *Service) AdoptionState() (index uint64, unfinished int) {
	index = s.table.HookedIndex()
	return index, s.adopting.unfinished() + s.transit.count()
}

// ProposeServed puts this node's served fact in the log: its adoptions begun
// at or below index are finished, and its own route updates up to routes
// carry their routes. It returns once the fact is committed or ctx ends; a
// fact the table refuses as not a raise has been said already.
func (s *Service) ProposeServed(ctx context.Context, index, routes uint64) error {
	_, err := s.submit(ctx, Op{Kind: OpServed, Node: s.o.Node, Served: index, Routes: routes, Now: s.o.Clock()})
	return err
}

// ServedFact is node's served fact as this node's table holds it.
func (s *Service) ServedFact(node string) (index, routes uint64) { return s.table.ServedFact(node) }

// Cover is node's cover as this node's table holds it.
func (s *Service) Cover(node string) (grant, routes uint64) { return s.table.Cover(node) }

// LostAt is the log index at which node last lost a lease — for a node that
// owns nothing, the index it was emptied at, the same on every member.
func (s *Service) LostAt(node string) uint64 { return s.table.LostAt(node) }

// AppliedIndex is the log index of the last lease command applied here. A
// caller that has just read something from the table gets an index at or
// past the command that put it there.
func (s *Service) AppliedIndex() uint64 { return s.table.AppliedIndex() }
