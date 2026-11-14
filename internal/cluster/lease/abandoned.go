package lease

import (
	"context"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// An acquire whose CONNECT gave up stays queued, and can be proposed and
// granted after the client was refused. The grant moves the lease here, online,
// and nobody fetches the session for it: the client's next CONNECT here would
// find its own lease with nothing to adopt and be served a fresh session while
// a copy exists elsewhere. So the CONNECT that gives up leaves a note, and the
// next CONNECT for the client here turns it into an adoption and fetches first.

// abandonedAcquire is one key's entry: noted when a CONNECT gave up, with the
// owner its lookup saw, and the latest grant to this node that applied while
// the entry existed or a CONNECT of the client was in flight here: its token
// and the owner it took the lease from. The lookup can be stale, so the grant
// is the better witness of where the session was: a CONNECT whose own table
// still showed this node as owner saw itself. at is when the entry was last
// written.
type abandonedAcquire struct {
	prev    string
	at      time.Time
	noted   bool
	granted uint64
	from    string
}

type abandonedAcquires struct {
	mu        sync.Mutex
	m         map[key]abandonedAcquire
	lastSweep time.Time
}

func newAbandonedAcquires() *abandonedAcquires {
	return &abandonedAcquires{m: map[key]abandonedAcquire{}}
}

// note records an abandoned acquire, replacing an earlier note for the key
// and keeping its grant. It reports whether a sweep is due: at most one a ttl.
func (a *abandonedAcquires) note(k key, prev string, now time.Time, ttl time.Duration) (sweep bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.m[k]
	e.prev, e.at, e.noted = prev, now, true
	a.m[k] = e
	return a.sweepDueLocked(now, ttl)
}

// grant records a grant to this node at token, taken from from, on the key's
// entry, making one when inFlight. It reports whether a sweep is due.
func (a *abandonedAcquires) grant(k key, token uint64, from string, inFlight bool, now time.Time, ttl time.Duration) (sweep bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.m[k]
	if !ok && !inFlight {
		return false
	}
	if !ok {
		e.at = now
	}
	e.granted, e.from = token, from
	a.m[k] = e
	return a.sweepDueLocked(now, ttl)
}

func (a *abandonedAcquires) sweepDueLocked(now time.Time, ttl time.Duration) bool {
	if now.Sub(a.lastSweep) >= ttl {
		a.lastSweep = now
		return true
	}
	return false
}

// take removes and returns the key's entry.
func (a *abandonedAcquires) take(k key) (abandonedAcquire, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	n, ok := a.m[k]
	delete(a.m, k)
	return n, ok
}

// takeNoted removes and returns the key's entry when it holds a note.
func (a *abandonedAcquires) takeNoted(k key) (abandonedAcquire, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	n, ok := a.m[k]
	if !ok || !n.noted {
		return abandonedAcquire{}, false
	}
	delete(a.m, k)
	return n, true
}

// sweep drops the notes keepAbandoned does not keep. here reports whether the
// key's lease is this node's and online; it is called without the lock held.
func (a *abandonedAcquires) sweep(now time.Time, ttl time.Duration, here func(key) bool) {
	a.mu.Lock()
	old := map[key]abandonedAcquire{}
	for k, n := range a.m {
		if now.Sub(n.at) >= ttl {
			old[k] = n
		}
	}
	a.mu.Unlock()
	for k, n := range old {
		if n.noted && keepAbandoned(now.Sub(n.at), ttl, here(k)) {
			continue
		}
		a.mu.Lock()
		if a.m[k] == n {
			delete(a.m, k)
		}
		a.mu.Unlock()
	}
}

// len counts the notes; a grant without one is not a note.
func (a *abandonedAcquires) len() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, e := range a.m {
		if e.noted {
			n++
		}
	}
	return n
}

// keepAbandoned decides whether a note stays. A young one may still see its
// grant applied. An old one stays while the lease is this node's and online:
// the grant landed and the client has not come back for its session.
func keepAbandoned(age, ttl time.Duration, leaseHereOnline bool) bool {
	return age < ttl || leaseHereOnline
}

// noteAbandoned records that this CONNECT's acquire may still be granted after
// it gave up; prev is the owner it saw.
func (s *Service) noteAbandoned(k key, prev string) {
	s.acquiresAbandoned.Add(1)
	now := s.steady()
	if s.abandoned.note(k, prev, now, s.o.TTL) {
		s.sweepAbandoned(now)
	}
}

// noteGrant records, for a later abandoned CONNECT's fetch, the owner a grant
// to this node at token took the lease from. It runs on apply; only a key with
// a note, or with a CONNECT in flight here that may yet give up, is recorded.
func (s *Service) noteGrant(k key, token uint64, from string) {
	now := s.steady()
	if s.abandoned.grant(k, token, from, s.connecting.idle(k) != nil, now, s.o.TTL) {
		s.sweepAbandoned(now)
	}
}

func (s *Service) sweepAbandoned(now time.Time) {
	s.abandoned.sweep(now, s.o.TTL, func(k key) bool {
		l, ok := s.table.Lookup(k.ns, k.id)
		return ok && l.Owner == s.o.Node && l.Online
	})
}

// abandonedSource decides whom an abandoned acquire's session is fetched
// from, for a lease this node holds at current: the owner the grant at that
// token took it from; without a record of that grant, the owner the CONNECT's
// lookup saw. It is pure; the owner it names is asked for the session.
func abandonedSource(prev string, granted uint64, from string, current uint64) string {
	if granted != 0 && granted == current && from != "" {
		return from
	}
	return prev
}

// fetchAbandoned runs after awaitAdoption found nothing to settle for a lease
// that is this node's at token. When an acquire of this client here was
// abandoned, nothing has fetched the session the lease was granted for: it is
// adopted and fetched now, before the client is served, from the owner
// abandonedSource names. first is the owner an abandoned first acquire's grant
// took the lease from (see takenFrom), read when there is no note. A held
// session, a clean start or a result already in hand fetch nothing. The note
// goes either way; a grant recorded with no note stays for the CONNECT in
// flight that may yet write one. finish is awaitAdoption's, for a state
// already in hand, and is returned for the state this fetch returns.
func (s *Service) fetchAbandoned(ctx context.Context, k key, token uint64, cleanStart bool, st *store.SessionState, ev EmptyEvidence, finish func(), first string) (*store.SessionState, EmptyEvidence, func(), error) {
	e, noted := s.abandoned.takeNoted(k)
	prev := e.prev
	fromFirst := false
	if !noted && first != "" {
		prev, noted, fromFirst = first, true, true
	}
	if !noted || cleanStart || st != nil || ev.Reason != "" {
		return st, ev, finish, nil
	}
	if _, held := s.o.Sessions.Export(k.ns, k.id); held {
		return st, ev, finish, nil
	}
	if !fromFirst {
		prev = abandonedSource(prev, e.granted, e.from, token)
	}
	if fromFirst {
		s.firstsTakenAdopted.Add(1)
		s.log.Debug("lease taken from another node by a first acquire whose CONNECT gave up; fetching the session first", "client", k.id, "from", prev, "token", token)
	} else {
		s.unfetchedAdopted.Add(1)
		s.log.Debug("lease granted here after its CONNECT gave up; fetching the session first", "client", k.id, "from", prev, "token", token)
	}
	s.adopting.begin(k, adoptJob{epoch: token, prev: prev, takeover: true})
	return s.awaitAdoption(ctx, k, false)
}

// A first CONNECT, which found no lease, leaves no note when it gives up: no
// session can exist elsewhere for a lease it creates. But its grant may still
// apply after, and the client's next CONNECT here then finds an online lease
// with nothing fetched, which reads as a session served empty. So a first
// acquire is marked before it is proposed, the grant to this node that creates
// the lease records its token on the mark, and the next CONNECT here reads the
// mark. A grant that instead took the lease from another node, which the client
// may have reached meanwhile, records that node, and the next CONNECT here
// fetches from it.

// firstAcquire is one mark: when it was made, whether its CONNECT gave up, the
// token of the grant that created the lease here, and the token of a grant here
// that took the lease from another node, with that node (0 until one applies).
type firstAcquire struct {
	at        time.Time
	abandoned bool
	created   uint64
	taken     uint64
	from      string
}

type firstAcquires struct {
	mu        sync.Mutex
	m         map[key]*firstAcquire
	lastSweep time.Time
}

func newFirstAcquires() *firstAcquires {
	return &firstAcquires{m: map[key]*firstAcquire{}}
}

// begin marks a first acquire about to be proposed.
func (f *firstAcquires) begin(k key, now time.Time) {
	f.mu.Lock()
	f.m[k] = &firstAcquire{at: now}
	f.mu.Unlock()
}

// end drops the mark of an acquire whose CONNECT got its result.
func (f *firstAcquires) end(k key) {
	f.mu.Lock()
	delete(f.m, k)
	f.mu.Unlock()
}

// abandon keeps the mark of an acquire whose CONNECT gave up. It returns the
// node a grant already recorded on the mark took the lease from, and reports
// whether a sweep is due: at most one a ttl.
func (f *firstAcquires) abandon(k key, now time.Time, ttl time.Duration) (from string, sweep bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m := f.m[k]; m != nil {
		m.abandoned, m.at = true, now
		if m.taken != 0 {
			from = m.from
		}
	}
	if now.Sub(f.lastSweep) >= ttl {
		f.lastSweep = now
		return from, true
	}
	return from, false
}

// created records, on a marked key, the token of the grant that created its
// lease on this node.
func (f *firstAcquires) created(k key, token uint64) {
	f.mu.Lock()
	if m := f.m[k]; m != nil {
		m.created = token
	}
	f.mu.Unlock()
}

// took records, on a marked key, the token of a grant to this node that took
// the lease from another node, and that node. It reports whether the mark is
// abandoned. A nil set records nothing: the hook also runs on services built
// without one.
func (f *firstAcquires) took(k key, token uint64, from string) (abandoned bool) {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.m[k]
	if m == nil {
		return false
	}
	m.taken, m.from = token, from
	return m.abandoned
}

// take removes an abandoned mark and returns it.
func (f *firstAcquires) take(k key) (firstAcquire, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.m[k]
	if m == nil || !m.abandoned {
		return firstAcquire{}, false
	}
	delete(f.m, k)
	return *m, true
}

// sweep drops the abandoned marks keepAbandoned does not keep. here reports
// whether the key's lease is this node's and online; it is called without the
// lock held.
func (f *firstAcquires) sweep(now time.Time, ttl time.Duration, here func(key) bool) {
	f.mu.Lock()
	old := map[key]*firstAcquire{}
	for k, m := range f.m {
		if m.abandoned && now.Sub(m.at) >= ttl {
			old[k] = m
		}
	}
	f.mu.Unlock()
	for k, m := range old {
		if keepAbandoned(now.Sub(m.at), ttl, here(k)) {
			continue
		}
		f.mu.Lock()
		if f.m[k] == m {
			delete(f.m, k)
		}
		f.mu.Unlock()
	}
}

func (f *firstAcquires) len() int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.m)
}

// createdHere decides whether a lease found here online with nothing fetched
// is the one this node's abandoned first acquire created: the mark abandoned
// and the lease still at the token of the grant recorded on it (0, never a
// lease's token, when none was), which every move of a lease to another node
// and back replaces. Only then had the client no session anywhere.
func createdHere(abandoned bool, created, current uint64) bool {
	return abandoned && created == current
}

// takenFrom decides which node the session must be fetched from for a lease
// that is this node's at current, from an abandoned first-acquire mark: the
// node its grant took the lease from, while the lease is still at that grant's
// token. On the raced branch the table may not have applied that grant yet, so a
// mark that records no grant at all names seen, the owner the CONNECT's lookup
// saw: its acquire was proposed after the abandoned one. "" for none.
func takenFrom(abandoned bool, created, taken, current uint64, from, seen, self string) string {
	switch {
	case !abandoned:
		return ""
	case taken == current:
		return from
	case created == 0 && taken == 0 && seen != self:
		return seen
	}
	return ""
}

// firstTaken notes the owner a grant to this node took the lease from, for a
// first acquire whose CONNECT gave up: a pull here names it.
func (s *Service) firstTaken(k key, token uint64, from string) {
	if s.firsts.took(k, token, from) {
		s.noteTaking(k, from)
	}
}

// abandonFirst keeps the mark of a first acquire whose CONNECT gave up.
func (s *Service) abandonFirst(k key) {
	s.acquiresAbandoned.Add(1)
	now := s.steady()
	from, sweep := s.firsts.abandon(k, now, s.o.TTL)
	s.noteTaking(k, from)
	if sweep {
		s.firsts.sweep(now, s.o.TTL, func(k key) bool {
			l, ok := s.table.Lookup(k.ns, k.id)
			return ok && l.Owner == s.o.Node && l.Online
		})
	}
}
