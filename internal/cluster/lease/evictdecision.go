package lease

// evictChoice is what the deferred reclaim after a lost lease should do.
type evictChoice int

const (
	// evictKeep: the lease is back here, so there is nothing stale to reclaim.
	evictKeep evictChoice = iota
	// evictDefer: the lease is elsewhere but a client is connected here, so
	// reclaiming would close a connection the client has already made.
	evictDefer
	// evictSeal: the lease is another node's and no client is here. The
	// session is sealed for that node's grant, not deleted: its records and
	// every replica copy stay until the node confirms it holds the session.
	evictSeal
	// evictNow: the lease no longer exists (released or expired), so no node
	// will confirm; the session is evicted and its delete journaled.
	evictNow
)

// evictDeferBound is how many times the decision may be deferred before the
// session is reclaimed regardless. A client connected here whose lease never
// returns is an inconsistent state, and the reclaim exists to end it; the
// bound is what stops "wait for the client to leave" becoming "never reclaim".
const evictDeferBound = 3

// shouldEvict decides what the deferred reclaim of a session whose lease left
// this node does. It is pure: its inputs are passed in, it performs no
// eviction, closes nothing and reads no state, so its test can pass exactly
// the arguments that would be destructive without any of them being acted on
// (AGENTS.md, "a destructive operation decides in a pure predicate").
//
// The guard it replaces tested only ownsLease. The comment on that guard said
// it was sparing "a client that came back here", and a client that has come
// back is one with a live connection — which the old test could not see, since
// a returning client's takeover is still in flight and the lease has not
// arrived yet. Measured at 681-19,991 connections closed per run this way
// (WI 1495, sim/results/evict-2026-09-16/).
func shouldEvict(ownsLease, leaseGone, clientHere bool, deferred int) evictChoice {
	switch {
	case ownsLease:
		return evictKeep
	case clientHere && deferred < evictDeferBound:
		return evictDefer
	case leaseGone:
		return evictNow
	}
	return evictSeal
}

// recoveryChoice decides what a node's recovery does with a session it holds.
// It is pure, like shouldEvict: evicting journals a delete. A session whose
// lease is another node's is sealed for that node's grant; removeOnHandOver is
// the injected defect that restores the eviction.
func recoveryChoice(leaseFound bool, owner, self string, removeOnHandOver bool) evictChoice {
	switch {
	case !leaseFound || owner == self:
		return evictKeep
	case removeOnHandOver:
		return evictNow
	}
	return evictSeal
}
