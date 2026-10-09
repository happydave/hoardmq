package lease

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// A lost node's session that a survivor takes from the adopter before the
// adopter imported it is in transit: the adopter's adoption ends without an
// import, and the taker's CONNECT is not an adoption. A node forgets the lost
// node's routes once every member that owns leases has its served fact in the
// log and this node holds that member's routes up to the fact's mark
// (routing/served.go), so without these entries the session's filters could
// match nothing anywhere while the taker's import is still running (WI 1833).
//
// The taker counts its takeover until it holds the session, and then puts a
// newer fact in the log, whose route mark covers the session's routes. The
// adopter counts the adoption until its table shows the taker's fact at the
// grant the taker took the lease at, so the adopter's own fact comes after
// the taker's in the log, and every node that applies it already requires the
// taker's routes up to the newer mark.

// takenEntry is a takeover this node made of a session its previous owner
// was still adopting, counted until the session is held here at epoch.
type takenEntry struct {
	epoch uint64
	since time.Time
}

// handedEntry is an adoption this node ended because its lease left first,
// counted until owner's served fact reaches epoch. An empty owner is a grant
// this node has been told of and its table has not applied yet: epoch is
// that grant, and the owner is read once the table reaches it.
type handedEntry struct {
	owner string
	epoch uint64
	since time.Time
}

type transitSet struct {
	mu     sync.Mutex
	taken  map[key]takenEntry
	handed map[key]handedEntry
	// through is the highest epoch of a taken entry that ended with its
	// session held here.
	through atomic.Uint64
	// capped is the lowest epoch of a taken entry that ended at its bound,
	// zero for none: this node's cover claims no grant at or above it, since
	// a later take held above it raises through past a session never held
	// here. Guarded by mu.
	capped uint64
	wake   chan struct{}

	takenN, takenHeld, takenExpired                               atomic.Int64
	handedN, handedServed, handedMoved, handedDead, handedExpired atomic.Int64
	// handedCovered counts the handed entries of handedServed that ended on
	// the new owner's cover rather than its finished index.
	handedCovered atomic.Int64
}

func newTransitSet() *transitSet {
	return &transitSet{taken: map[key]takenEntry{}, handed: map[key]handedEntry{}, wake: make(chan struct{}, 1)}
}

func (t *transitSet) count() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.taken) + len(t.handed)
}

// TransitStats counts the entries of sessions in transit, by how each ended.
type TransitStats struct {
	Taken         int64 `json:"taken"`
	TakenHeld     int64 `json:"taken_held"`
	TakenExpired  int64 `json:"taken_expired"`
	Handed        int64 `json:"handed"`
	HandedServed  int64 `json:"handed_served"`
	HandedMoved   int64 `json:"handed_moved"`
	HandedDead    int64 `json:"handed_dead"`
	HandedExpired int64 `json:"handed_expired"`
	HandedCovered int64 `json:"handed_covered"`
	Open          int   `json:"open"`
}

// TransitStats reads the counters.
func (s *Service) TransitStats() TransitStats {
	t := s.transit
	if t == nil {
		return TransitStats{}
	}
	return TransitStats{Taken: t.takenN.Load(), TakenHeld: t.takenHeld.Load(), TakenExpired: t.takenExpired.Load(),
		Handed: t.handedN.Load(), HandedServed: t.handedServed.Load(), HandedMoved: t.handedMoved.Load(),
		HandedDead: t.handedDead.Load(), HandedExpired: t.handedExpired.Load(), HandedCovered: t.handedCovered.Load(), Open: t.count()}
}

// AdoptedThrough is the highest grant of a takeover of a session in transit
// that this node has finished: its served fact must reach it.
func (s *Service) AdoptedThrough() uint64 {
	if s.transit == nil {
		return 0
	}
	return s.transit.through.Load()
}

// noteTaken counts a takeover of a session its previous owner was still
// adopting, until the session is held here at epoch.
func (s *Service) noteTaken(k key, epoch uint64) {
	t := s.transit
	if t == nil {
		return
	}
	t.mu.Lock()
	t.taken[k] = takenEntry{epoch: epoch, since: time.Now()}
	t.mu.Unlock()
	t.takenN.Add(1)
	s.wakeTransit()
}

// handOn counts an adoption whose lease left this node before its import,
// until the lease's new owner has put a served fact in the log at the grant
// it holds the lease at. The caller records it before ending the adoption, so
// the count of unfinished adoptions never reads zero in between.
func (s *Service) handOn(k key) {
	if s.transit == nil {
		return
	}
	l, ok := s.table.Lookup(k.ns, k.id)
	e := handedEntry{owner: l.Owner, epoch: l.Epoch, since: time.Now()}
	if !ok || l.Owner == "" || l.Owner == s.o.Node {
		// A refused import can be decided on a later grant this node was told
		// of (its token registry) before its table applied it.
		seen := s.tokens.Seen(k.ns, k.id)
		if seen <= l.Epoch {
			return
		}
		e.owner, e.epoch = "", seen
	}
	t := s.transit
	t.mu.Lock()
	t.handed[k] = e
	t.mu.Unlock()
	t.handedN.Add(1)
	s.wakeTransit()
}

func (s *Service) wakeTransit() {
	if s.transit == nil {
		return
	}
	select {
	case s.transit.wake <- struct{}{}:
	default:
	}
}

// Reasons an entry ends.
const (
	transitOpen = iota
	transitServed
	transitMoved
	transitDead
	transitExpired
)

// handedSettles decides whether a handed-on entry ends: when the new owner's
// finished index or its cover reaches the epoch, when the lease is no longer
// that owner's at that epoch, when the owner is not alive, or at the bound.
func handedSettles(e handedEntry, cur Lease, held bool, ownerLive bool, fact, cover uint64, age, bound time.Duration) int {
	switch {
	case !held || cur.Owner != e.owner || cur.Epoch != e.epoch:
		return transitMoved
	case !ownerLive:
		return transitDead
	case fact >= e.epoch || cover >= e.epoch:
		return transitServed
	case age >= bound:
		return transitExpired
	}
	return transitOpen
}

// takenSettles decides whether a taken entry ends: when the session is held
// here at the entry's epoch or later, or at the bound.
func takenSettles(e takenEntry, heldAt uint64, held bool, age, bound time.Duration) int {
	switch {
	case held && heldAt >= e.epoch:
		return transitServed
	case age >= bound:
		return transitExpired
	}
	return transitOpen
}

// coverDue decides whether this node owes a raised cover, and through which
// grant: the highest takeover in transit held here, below the lowest one
// that ended at its bound unless this node's finished index has passed that
// one, and only with no takeover in transit still open.
func coverDue(open int, through, capped, held, finished uint64) (uint64, bool) {
	grant := through
	if capped != 0 && capped > finished && capped-1 < grant {
		grant = capped - 1
	}
	return grant, open == 0 && grant > held
}

// CoverDue reports whether this node's cover should be raised, and to which
// grant. A cap this node's own finished index has reached is
// cleared: the index ends the handed entries it guarded.
func (s *Service) CoverDue() (uint64, bool) {
	t := s.transit
	if t == nil {
		return 0, false
	}
	finished, _ := s.table.ServedFact(s.o.Node)
	held, _ := s.table.Cover(s.o.Node)
	t.mu.Lock()
	if t.capped != 0 && t.capped <= finished {
		t.capped = 0
	}
	open, capped := len(t.taken), t.capped
	t.mu.Unlock()
	return coverDue(open, t.through.Load(), capped, held, finished)
}

// ProposeCover puts this node's cover in the log: the sessions it took in
// transit through grant are held here, and its route updates up to routes
// carry them. It returns once the operation is committed or ctx ends.
func (s *Service) ProposeCover(ctx context.Context, grant, routes uint64) error {
	_, err := s.submit(ctx, Op{Kind: OpServed, Node: s.o.Node, Cover: grant, CoverRoutes: routes, Now: s.o.Clock()})
	return err
}

// settleTransit ends the entries whose condition holds, and tells the idle
// hook when that left nothing unfinished here.
func (s *Service) settleTransit() {
	t := s.transit
	now := time.Now()
	t.mu.Lock()
	if len(t.taken)+len(t.handed) == 0 {
		t.mu.Unlock()
		return
	}
	taken := make(map[key]takenEntry, len(t.taken))
	for k, e := range t.taken {
		taken[k] = e
	}
	handed := make(map[key]handedEntry, len(t.handed))
	for k, e := range t.handed {
		handed[k] = e
	}
	t.mu.Unlock()
	var endTaken, endHanded []key
	for k, e := range taken {
		at, held := s.o.Sessions.HeldAt(k.ns, k.id)
		switch takenSettles(e, at, held, now.Sub(e.since), s.o.SealTimeout) {
		case transitServed:
			t.takenHeld.Add(1)
			for {
				c := t.through.Load()
				if e.epoch <= c || t.through.CompareAndSwap(c, e.epoch) {
					break
				}
			}
			endTaken = append(endTaken, k)
		case transitExpired:
			t.takenExpired.Add(1)
			s.log.Warn("a takeover of a session in transit was not held here in time; this node's served fact no longer waits on it", "client", k.id, "token", e.epoch)
			t.mu.Lock()
			if t.capped == 0 || e.epoch < t.capped {
				t.capped = e.epoch
			}
			t.mu.Unlock()
			endTaken = append(endTaken, k)
		}
	}
	for k, e := range handed {
		cur, held := s.table.Lookup(k.ns, k.id)
		if e.owner == "" {
			if held && cur.Epoch >= e.epoch && cur.Owner != s.o.Node {
				e.owner, e.epoch = cur.Owner, cur.Epoch
				t.mu.Lock()
				if _, ok := t.handed[k]; ok {
					t.handed[k] = e
				}
				t.mu.Unlock()
				handed[k] = e
			} else {
				if now.Sub(e.since) >= s.o.SealTimeout {
					t.handedExpired.Add(1)
					endHanded = append(endHanded, k)
				}
				continue
			}
		}
		fact, _ := s.table.ServedFact(e.owner)
		cover, _ := s.table.Cover(e.owner)
		switch handedSettles(e, cur, held, s.table.Live(e.owner), fact, cover, now.Sub(e.since), s.o.SealTimeout) {
		case transitServed:
			t.handedServed.Add(1)
			if fact < e.epoch {
				t.handedCovered.Add(1)
			}
			endHanded = append(endHanded, k)
		case transitMoved:
			t.handedMoved.Add(1)
			endHanded = append(endHanded, k)
		case transitDead:
			t.handedDead.Add(1)
			endHanded = append(endHanded, k)
		case transitExpired:
			t.handedExpired.Add(1)
			s.log.Warn("an adoption whose lease left before its import stopped waiting for the new owner's served fact", "client", k.id, "owner", e.owner, "token", e.epoch)
			endHanded = append(endHanded, k)
		}
	}
	if len(endTaken)+len(endHanded) == 0 {
		return
	}
	t.mu.Lock()
	for _, k := range endTaken {
		if cur, ok := t.taken[k]; ok && cur == taken[k] {
			delete(t.taken, k)
		}
	}
	for _, k := range endHanded {
		if cur, ok := t.handed[k]; ok && cur == handed[k] {
			delete(t.handed, k)
		}
	}
	empty := len(t.taken)+len(t.handed) == 0
	t.mu.Unlock()
	if len(endTaken) > 0 && s.o.TakesEnded != nil {
		s.o.TakesEnded()
	}
	if empty && s.adopting.unfinished() == 0 && s.o.AdoptionsIdle != nil {
		s.o.AdoptionsIdle()
	}
}

// transitLoop settles the entries on a short tick while any stand, and when a
// fact or a declaration applies.
func (s *Service) transitLoop() {
	defer close(s.transitDone)
	tick := time.NewTicker(confirmPoll)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-s.transit.wake:
		case <-tick.C:
		}
		s.settleTransit()
	}
}
