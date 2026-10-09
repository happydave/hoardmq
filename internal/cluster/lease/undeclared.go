package lease

import "time"

// A takeover or an adoption that finds no state anywhere serves a fresh
// session only when its previous owner answered or the leader has declared it
// dead. An owner this node has no live link to was never asked, and until its
// declaration it may still hold the session, which a partition that heals
// before the declaration gives back.

// ownerUnaccounted reports whether a previous owner leaves a fetch that found
// nothing short of showing the session is held nowhere: it was not asked,
// having no live link from this node, and the log says it is alive.
func ownerUnaccounted(prev, self string, asked, live bool) bool {
	return prev != "" && prev != self && !asked && live
}

// holderUnaccounted reports whether a replica holder the fetch could not ask,
// because it was down, leaves a fetch that found nothing short of showing
// the session is held nowhere: the log says it is alive, so it may come back
// with its copy. A holder declared dead is not waited for.
func holderUnaccounted(holder, self string, live bool) bool {
	return holder != "" && holder != self && live
}

// heldNowhere decides whether a fetch that brought no state back lets the
// client be served a fresh session, which also drops this node's replica copy
// of it: every node asked answered "not found", and no previous owner is
// unaccounted.
func heldNowhere(answered, unaccounted bool) bool {
	return answered && !unaccounted
}

// ownerWait is what a fetch that brought no state back waits on in its
// previous owner: an owner it could not ask, one it asked that did not
// answer, or neither. While the log says that owner is alive it may hold the
// session, sealed for as long as nothing shows the session held elsewhere.
func ownerWait(prev, self string, asked, answered, live bool) undeclaredWait {
	switch {
	case prev == "" || prev == self || !live:
		return waitNone
	case !asked:
		return waitOwner
	case !answered:
		return waitOwnerSilent
	}
	return waitNone
}

// adoptionGivesUp decides whether the adoption worker ends an adoption whose
// fetch settled nothing; the client's next CONNECT is then served fresh. A
// wait on a node alive by the log, a previous owner or a replica holder that
// was down, never ends here: it ends when the node answers, a copy is found,
// the node is declared dead, or the lease moves. Any other ends once more than
// retries fetches have waited on nothing alive (spent).
func adoptionGivesUp(spent, retries int, wait undeclaredWait) bool {
	if wait != waitNone {
		return false
	}
	return spent > retries
}

// noStateVerdict is how a fetch of adopted state that brought no state back
// ends, and what it waits on: a previous owner alive by the log that gave no
// answer (owner), then an owner named by the node asked that is unaccounted
// (pending), then a replica source that was down and is alive by the log
// (holder), then a source that did not answer; with none of them it is found
// nowhere, which lets the client be served fresh. A node alive by the log
// that may hold the session outranks a source that did not answer, whose
// retries would otherwise end the wait while that node still holds the copy.
func noStateVerdict(owner undeclaredWait, pending, holder, answered bool) (fetchResult, undeclaredWait) {
	switch {
	case owner != waitNone:
		return fetchUnanswered, owner
	case pending:
		return fetchUnanswered, waitOwner
	case holder:
		return fetchUnanswered, waitHolder
	case !answered:
		return fetchUnanswered, waitNone
	}
	return fetchEmpty, waitNone
}

// giveUpServe is what a client is served after its adoption is given up.
type giveUpServe int

const (
	serveFresh giveUpServe = iota
	serveKept
)

// giveUpServes decides what an adoption that is given up serves. A copy a
// former holder kept, found by the give-up's own fetch while a source did not
// answer, is served: it loses at most what was written after it, where a fresh
// session loses everything. It is pure: serving
// either drops this node's replica copy of the session.
func giveUpServes(wait undeclaredWait, kept bool) giveUpServe {
	if wait == waitNone && kept {
		return serveKept
	}
	return serveFresh
}

// waitPastBound reports whether an adoption waiting on a node alive by the
// log has waited longer than bound. A node is declared once the leader has
// gone a ttl without hearing it, so one still alive that long after is one
// the leader hears: a client refused for the wait is sent there.
func waitPastBound(wait undeclaredWait, age, bound time.Duration) bool {
	return wait != waitNone && age > bound
}

// undeclaredHolder is the first of the down holders a fetch named that the
// log says is alive, or "".
func (s *Service) undeclaredHolder(down []string) string {
	for _, h := range down {
		if holderUnaccounted(h, s.o.Node, h != "" && s.table.Live(h)) {
			return h
		}
	}
	return ""
}
