package lease

import (
	"sync"
	"time"
)

// A node that refused a client for an owner it could not account for still
// holds the adoption that names that owner. When the client retries on another
// node, that node takes the lease from this one, which never held the session:
// its answer names the owner it was waiting on, so the new owner judges that
// owner as it judges its own previous owner, and takes the wait over.

// waitNote is the previous owner of an adoption that ended because its lease
// moved, and when. A takeover's note is the owner this node's own takeover of
// the client is waiting on, written before the takeover has placed the session
// here: until then a pass finds no session and no adoption for the client.
type waitNote struct {
	owner    string
	at       time.Time
	takeover bool
}

type waitNotes struct {
	mu        sync.Mutex
	m         map[key]waitNote
	lastSweep time.Time
}

func newWaitNotes() *waitNotes { return &waitNotes{m: map[key]waitNote{}} }

// waitNoteKept decides whether a note is still answered: a note is kept for
// bound, the longest this node would itself have waited on the owner.
func waitNoteKept(age, bound time.Duration) bool { return age < bound }

// note records owner for the key, replacing an earlier note, and drops the
// expired ones at most once a bound.
func (w *waitNotes) note(k key, owner string, now time.Time, bound time.Duration, takeover bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.m[k] = waitNote{owner: owner, at: now, takeover: takeover}
	if now.Sub(w.lastSweep) < bound {
		return
	}
	w.lastSweep = now
	for k, n := range w.m {
		if !waitNoteKept(now.Sub(n.at), bound) {
			delete(w.m, k)
		}
	}
}

// owner returns the key's noted owner while the note is kept, and whether a
// takeover wrote it. It does not remove it: a requester whose answer was lost
// asks again.
func (w *waitNotes) owner(k key, now time.Time, bound time.Duration) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, ok := w.m[k]
	if !ok || !waitNoteKept(now.Sub(n.at), bound) {
		return "", false
	}
	return n.owner, n.takeover
}

func (w *waitNotes) len() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.m)
}

// prevFor returns the previous owner of the key's unfinished adoption.
func (a *adoptionSet) prevFor(k key) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ad := a.m[k]; ad != nil {
		return ad.job.prev, true
	}
	return "", false
}

// repoint makes owner the previous owner of an adoption its caller has
// claimed: the node it was fetching from named owner as the one it was
// waiting on.
func (a *adoptionSet) repoint(ad *adoption, owner string) {
	a.mu.Lock()
	ad.job.prev = owner
	a.mu.Unlock()
}

// waitingOn is the owner a pass answering with no state names: the previous
// owner of this node's unfinished adoption for the key or, failing one, the
// noted owner of an adoption that ended because the lease moved or of a
// takeover of this node's own.
func (s *Service) waitingOn(k key) string {
	if s.o.OmitPendingOwner {
		return ""
	}
	owner, recorded := s.adopting.prevFor(k)
	noted, takeover := false, false
	if !recorded {
		owner, takeover = s.waitNotes.owner(k, s.steady(), s.o.SealTimeout)
		noted = true
	}
	if owner == "" || owner == s.o.Node {
		return ""
	}
	s.answeredPending.Add(1)
	if noted {
		s.answeredPendingNoted.Add(1)
	}
	if takeover {
		s.answeredPendingTaking.Add(1)
	}
	return owner
}

// noteWaitedOn keeps an adoption's previous owner after the adoption ended
// because its lease moved, for the pass that answers the new owner.
func (s *Service) noteWaitedOn(k key, owner string) {
	if owner == "" || owner == s.o.Node {
		return
	}
	s.waitNotes.note(k, owner, s.steady(), s.o.SealTimeout, false)
}

// noteTaking notes the owner a takeover of this node's own is waiting on for
// the key.
func (s *Service) noteTaking(k key, owner string) {
	if owner == "" || owner == s.o.Node {
		return
	}
	s.waitNotes.note(k, owner, s.steady(), s.o.SealTimeout, true)
}
