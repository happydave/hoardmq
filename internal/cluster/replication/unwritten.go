package replication

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// A copy from an earlier grant than the owner's, on a shard this node still
// holds and is not the owner's target for, or one whose hand-over the shard's
// holders refused, is the previous owner's, and the owner may not have fetched
// it yet: it is kept until the owner confirms it has written the session at
// its grant, or until one of the owner's targets is seen holding the session
// at that grant. No time ends the keep: an owner that dies before it is
// declared may have written the session nowhere else.

// A copy on a node that stopped being the session's target at a map change,
// while its owner is alive, is kept until the owner confirms the session
// durable at its targets now, or until one of the owner's targets is seen
// holding the session at the owner's grant: the owner's whole write to the
// target it gained may not have landed, and an owner that dies before it is
// declared would leave this copy the only one. No time ends the keep. A
// confirmation can be lost, and a copy kept behind the owner's later writes is
// what a takeover may read first. A copy written under the owner's grant on a
// node the owner does not replicate to is such a copy whatever map it is judged
// against: only a target receives the owner's writes.

// unfedVerdict is what dropKeptCopy decides for such a copy.
type unfedVerdict int

const (
	keepUnwritten unfedVerdict = iota
	dropConfirmed
	dropHeldAtTarget
)

// dropKeptCopy decides what happens to a copy this node is not the session's
// target for, with the owner alive. It is pure: acting on it deletes the copy.
// confirmed: the owner confirmed the session at the lease's token or above.
// heldAtTarget: one of the owner's targets was seen holding the session at
// that token or a later one.
func dropKeptCopy(confirmed, heldAtTarget bool) unfedVerdict {
	switch {
	case confirmed:
		return dropConfirmed
	case heldAtTarget:
		return dropHeldAtTarget
	}
	return keepUnwritten
}

// rejudgeAhead is how soon a kept copy is judged again while the
// confirmation noted for it names a later grant than this node's lease table.
const rejudgeAhead = 50 * time.Millisecond

// rejudgeSoon decides whether a kept copy is judged again at rejudgeAhead: the
// confirmation noted for it, at noted and age old, names a later grant than
// lease, the token this node's table reads, so the table has not yet applied
// the grant whose owner confirmed. Past one check interval the check takes
// over. It is pure, and decides no drop.
func rejudgeSoon(noted uint64, age time.Duration, lease uint64, every time.Duration) bool {
	return noted > lease && age < every
}

// writtenNote is the highest grant an owner confirmed a session held at.
type writtenNote struct {
	owner string
	token uint64
	at    time.Time
}

// keptNote is when a copy was first kept waiting for owner, and whether it
// was kept because this node stopped being the session's target. heldAt is
// the highest epoch a check saw one of owner's targets hold the session at;
// check names the check in flight, zero when none is.
type keptNote struct {
	owner  string
	since  time.Time
	moved  bool
	heldAt uint64
	check  uint64
}

// unwrittenCopies are the copies kept waiting for their owner, the
// confirmations received, and the copies to judge again. A confirmation is
// held for at most one check interval.
type unwrittenCopies struct {
	mu        sync.Mutex
	kept      map[store.Scope]keptNote
	written   map[store.Scope]writtenNote
	recheck   map[store.Scope]bool
	lastSweep time.Time
	checks    uint64
}

func newUnwrittenCopies() *unwrittenCopies {
	return &unwrittenCopies{kept: map[store.Scope]keptNote{}, written: map[store.Scope]writtenNote{}, recheck: map[store.Scope]bool{}}
}

// judge reads, for a copy whose lease is owner's at token, whether owner
// confirmed it, whether a target was seen holding it at token, and how long
// it has been kept; a copy not kept yet, or kept for another owner, is kept
// from now. fresh reports that it was.
func (u *unwrittenCopies) judge(s store.Scope, owner string, token uint64, now time.Time, moved bool) (keptFor time.Duration, confirmed, held, fresh bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	w, ok := u.written[s]
	confirmed = ok && w.owner == owner && w.token >= token
	k, ok := u.kept[s]
	if !ok || k.owner != owner {
		k, fresh = keptNote{owner: owner, since: now, moved: moved}, true
	}
	k.moved = moved
	u.kept[s] = k
	return now.Sub(k.since), confirmed, k.heldAt >= token && token > 0, fresh
}

// startCheck names a check of s's targets for owner, unless s is not kept for
// owner or a check is already in flight.
func (u *unwrittenCopies) startCheck(s store.Scope, owner string) (id uint64, ok bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	k, kept := u.kept[s]
	if !kept || k.owner != owner || k.check != 0 {
		return 0, false
	}
	u.checks++
	k.check = u.checks
	u.kept[s] = k
	return k.check, true
}

// endCheck records what check id saw, the epoch a target held the session
// at (zero for none), into the note it was started for; a note forgotten or
// renewed meanwhile takes nothing. It reports whether the note took it.
func (u *unwrittenCopies) endCheck(s store.Scope, id, heldAt uint64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	k, kept := u.kept[s]
	if !kept || k.check != id {
		return false
	}
	k.check = 0
	k.heldAt = max(k.heldAt, heldAt)
	u.kept[s] = k
	return true
}

// notedAt is the confirmation held for s: its token and when it was noted.
func (u *unwrittenCopies) notedAt(s store.Scope) (token uint64, at time.Time, ok bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	w, ok := u.written[s]
	return w.token, w.at, ok
}

// keptMoved reports whether s is kept because this node stopped being its
// target.
func (u *unwrittenCopies) keptMoved(s store.Scope) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.kept[s].moved
}

// forget ends what is kept for a copy that is gone or no longer this rule's.
func (u *unwrittenCopies) forget(s store.Scope) {
	u.mu.Lock()
	delete(u.kept, s)
	u.mu.Unlock()
}

// note records owner's confirmation of the session at token unless one for a
// later grant is held, and reports whether a copy is kept waiting for it.
// Expired entries go at most once a bound.
func (u *unwrittenCopies) note(s store.Scope, owner string, token uint64, now time.Time, bound time.Duration) (waiting bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if w, ok := u.written[s]; !ok || w.token <= token {
		u.written[s] = writtenNote{owner: owner, token: token, at: now}
	}
	if now.Sub(u.lastSweep) >= bound {
		u.lastSweep = now
		for k, w := range u.written {
			if now.Sub(w.at) >= bound {
				delete(u.written, k)
			}
		}
	}
	_, waiting = u.kept[s]
	return waiting
}

// queue marks a copy to be judged again.
func (u *unwrittenCopies) queue(s store.Scope) {
	u.mu.Lock()
	u.recheck[s] = true
	u.mu.Unlock()
}

// takeQueued returns the copies to judge again and clears them.
func (u *unwrittenCopies) takeQueued() []store.Scope {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]store.Scope, 0, len(u.recheck))
	for s := range u.recheck {
		out = append(out, s)
	}
	clear(u.recheck)
	return out
}

func (u *unwrittenCopies) keptCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.kept)
}

// unwrittenCheck is how long a copy is kept waiting for its owner before its
// owner's targets are asked whether one holds the session, and how often they
// are asked again: twice the lease ttl.
func (r *Replicator) unwrittenCheck() time.Duration {
	if r.o.UnwrittenCheck > 0 {
		return r.o.UnwrittenCheck
	}
	return 2 * r.o.TTL
}

// NoteWritten is told that owner holds the session at token and that its
// records are durable at its targets: the new owner's confirmation. A copy
// kept waiting for it is judged again on the reconcile worker.
func (r *Replicator) NoteWritten(ns, id, owner string, token uint64) {
	s := store.Scope{Namespace: ns, ClientID: id}
	waiting := r.unwritten.note(s, owner, token, time.Now(), r.unwrittenCheck())
	r.log.Debug("confirmation noted", "client", id, "owner", owner, "token", token, "waiting", waiting)
	if waiting {
		r.queueRecheck(s)
	}
}

// Unfed lists the nodes this node, as the session's owner, tells when it
// confirms the session: every other member alive by the log that it does not
// replicate the session to under its current map, and every member marked
// leaving. Any of them may keep a copy, a seal or a drain's wait for a session
// that changed hands again before the owner that knew of it confirmed.
func (r *Replicator) Unfed(ns, id string) []string {
	m := r.o.Map.Current()
	if m.Version == 0 {
		return nil
	}
	alive, leaving := r.placementFacts()
	return unfedOf(alive, leaving, r.o.Node, m.Targets(r.o.Map.ShardOf(ns, id), r.o.Node))
}

// unfedOf is alive less self and the targets that are not leaving, sorted. It
// is pure.
func unfedOf(alive, leaving []string, self string, targets []string) []string {
	var out []string
	for _, n := range alive {
		if n != self && (!contains(targets, n) || contains(leaving, n)) && !contains(out, n) {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// UnfedHolders lists the holders of the session's shard, under this node's
// current map, that this node would not replicate the session to as its
// owner: the nodes that may keep a copy waiting for this node to write it.
func (r *Replicator) UnfedHolders(ns, id string) []string {
	m := r.o.Map.Current()
	if m.Version == 0 {
		return nil
	}
	shard := r.o.Map.ShardOf(ns, id)
	targets := m.Targets(shard, r.o.Node)
	var out []string
	for _, h := range m.Holders(shard) {
		if h != r.o.Node && !contains(targets, h) {
			out = append(out, h)
		}
	}
	return out
}

func (r *Replicator) queueRecheck(s store.Scope) {
	r.unwritten.queue(s)
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// judgeUnfed acts on a copy, at copyEpoch, of a session whose lease is owner's
// at token, where owner is alive and does not replicate to this node; targets
// are owner's targets for the session under the map this node acts on. It
// runs on the reconcile worker.
func (r *Replicator) judgeUnfed(ss store.Scope, copyEpoch uint64, owner string, token uint64, targets []string) {
	if copyEpoch == token {
		r.judgeMoved(ss, owner, token, targets)
		return
	}
	every := r.unwrittenCheck()
	keptFor, confirmed, held, fresh := r.unwritten.judge(ss, owner, token, time.Now(), false)
	reason := ""
	switch dropKeptCopy(confirmed, held) {
	case keepUnwritten:
		r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-unwritten"}, 1)
		r.rejudgeIfAhead(ss, token, every)
		if fresh {
			r.log.Debug("replica copy kept until its owner writes the session", "client", ss.ClientID, "owner", owner, "token", token, "copyEpoch", copyEpoch)
			time.AfterFunc(every-keptFor, func() { r.queueRecheck(ss) })
		} else if keptFor >= every {
			r.checkTargets(ss, owner, token, targets, every)
		}
		return
	case dropConfirmed:
		reason = "not-a-target-confirmed"
	case dropHeldAtTarget:
		reason = "not-a-target-held"
	}
	r.unwritten.forget(ss)
	r.follower.dropCopy(ss.Namespace, ss.ClientID, reason)
}

// rejudgeIfAhead queues a kept copy to be judged again shortly when the
// confirmation noted for it is ahead of token, the lease as this node reads it.
func (r *Replicator) rejudgeIfAhead(ss store.Scope, token uint64, every time.Duration) {
	noted, at, ok := r.unwritten.notedAt(ss)
	if ok && rejudgeSoon(noted, time.Since(at), token, every) {
		time.AfterFunc(rejudgeAhead, func() { r.queueRecheck(ss) })
	}
}

// unwrittenChecksAtOnce bounds the checks a node has in flight. Each asks a
// target for a whole session, and a target whose fetch pool is full answers
// on the worker that applies this node's records.
const unwrittenChecksAtOnce = 8

// checkTargets asks owner's targets, off the reconcile worker, whether one
// holds the session at token or a later grant. Seen, the copy is judged
// again; not seen, the check is made again one interval later.
func (r *Replicator) checkTargets(ss store.Scope, owner string, token uint64, targets []string, every time.Duration) {
	id, ok := r.unwritten.startCheck(ss, owner)
	if !ok {
		return
	}
	go func() {
		if !r.takeCheckSlot(r.ctx) {
			return
		}
		defer r.releaseCheckSlot()
		r.count("hoardmq_replicas_unwritten_checks_total", map[string]string{"outcome": "started"}, 1)
		heldAt := r.askTargets(r.ctx, ss, token, targets)
		if !r.unwritten.endCheck(ss, id, heldAt) {
			return
		}
		if heldAt > 0 {
			r.count("hoardmq_replicas_unwritten_checks_total", map[string]string{"outcome": "held"}, 1)
			r.log.Debug("replica copy's owner holds the session at a target", "client", ss.ClientID, "owner", owner, "token", token, "heldAt", heldAt)
			r.queueRecheck(ss)
			return
		}
		r.count("hoardmq_replicas_unwritten_checks_total", map[string]string{"outcome": "not-held"}, 1)
		r.log.Debug("replica copy kept: no target of its owner holds the session", "client", ss.ClientID, "owner", owner, "token", token, "targets", targets)
		time.AfterFunc(every, func() { r.queueRecheck(ss) })
	}()
}

// takeCheckSlot waits for one of the node's check slots, and reports false
// when ctx ends or the replicator stops first.
func (r *Replicator) takeCheckSlot(ctx context.Context) bool {
	select {
	case r.checkSlots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	case <-r.stop:
		return false
	}
}

func (r *Replicator) releaseCheckSlot() { <-r.checkSlots }

// askTargets asks targets in turn whether one holds the session at token or a
// later grant, and returns the first such epoch, or zero. This node, when it
// is one of them, answers from its own replica copy; a peer that is up is
// asked with a fetch under one ack timeout.
func (r *Replicator) askTargets(ctx context.Context, ss store.Scope, token uint64, targets []string) uint64 {
	for _, t := range targets {
		if t == r.o.Node {
			if epoch, held := r.follower.copyEpoch(ss.Namespace, ss.ClientID); held && epoch >= token {
				return epoch
			}
			continue
		}
		if r.o.Transport == nil || !r.up(t) {
			continue
		}
		actx, cancel := context.WithTimeout(ctx, r.o.AckTimeout)
		ans, answered := r.request(actx, t, frame{Kind: kindFetch, Namespace: ss.Namespace, ClientID: ss.ClientID})
		cancel()
		if answered && ans.Found && ans.State != nil && ans.State.Epoch >= token {
			return ans.State.Epoch
		}
	}
	return 0
}

// HeldAtTargets asks owner's targets for the session, under the map this
// node last reconciled, whether one holds it at token or a later grant, and
// returns the epoch it saw, or zero. It holds one of the node's check slots
// while it asks, and returns zero when ctx ends or the replicator stops
// before one is free.
func (r *Replicator) HeldAtTargets(ctx context.Context, ns, id, owner string, token uint64) uint64 {
	if token == 0 || owner == "" {
		return 0
	}
	r.recMu.Lock()
	cur := r.reconciled
	r.recMu.Unlock()
	if cur.Version == 0 {
		return 0
	}
	targets := cur.Targets(r.o.Map.ShardOf(ns, id), owner)
	if len(targets) == 0 || !r.takeCheckSlot(ctx) {
		return 0
	}
	defer r.releaseCheckSlot()
	return r.askTargets(ctx, store.Scope{Namespace: ns, ClientID: id}, token, targets)
}

// judgeMoved acts on a copy of a session whose lease is owner's at token,
// where owner is alive and this node stopped being one of its targets;
// targets are owner's targets for the session under the map this node acts
// on. It runs on the reconcile worker.
func (r *Replicator) judgeMoved(ss store.Scope, owner string, token uint64, targets []string) {
	every := r.unwrittenCheck()
	keptFor, confirmed, held, fresh := r.unwritten.judge(ss, owner, token, time.Now(), true)
	reason := ""
	switch dropKeptCopy(confirmed, held) {
	case keepUnwritten:
		r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "moved-target"}, 1)
		r.rejudgeIfAhead(ss, token, every)
		if fresh {
			r.log.Debug("replica copy kept until its owner confirms the session at its new targets", "client", ss.ClientID, "owner", owner, "token", token)
			time.AfterFunc(every-keptFor, func() { r.queueRecheck(ss) })
		} else if keptFor >= every {
			r.checkTargets(ss, owner, token, targets, every)
		}
		return
	case dropHeldAtTarget:
		reason = "not-a-target-held"
	default:
		reason = "not-a-target-confirmed"
	}
	r.unwritten.forget(ss)
	r.follower.dropCopy(ss.Namespace, ss.ClientID, reason)
}

// KeptMoved is how many copies this node keeps because it stopped being
// their target and their owner has not yet confirmed them held elsewhere. A
// copy another path deleted meanwhile is not counted, nor one the lease's
// owner now replicates to this node, though its note lasts until its recheck.
func (r *Replicator) KeptMoved() int {
	n := 0
	for _, e := range r.movedNotes() {
		if r.stillMoved(e.s) {
			n++
		}
	}
	return n
}

// KeptMovedSample names up to n of the copies KeptMoved counts, each as
// client@owner:since, for a log line.
func (r *Replicator) KeptMovedSample(n int) []string {
	var out []string
	for _, e := range r.movedNotes() {
		if len(out) >= n {
			break
		}
		if r.stillMoved(e.s) {
			out = append(out, e.s.ClientID+"@"+e.k.owner+":"+e.k.since.Format("15:04:05.000"))
		}
	}
	return out
}

type movedNote struct {
	s store.Scope
	k keptNote
}

func (r *Replicator) movedNotes() []movedNote {
	r.unwritten.mu.Lock()
	defer r.unwritten.mu.Unlock()
	var out []movedNote
	for s, k := range r.unwritten.kept {
		if k.moved {
			out = append(out, movedNote{s, k})
		}
	}
	return out
}

// stillMoved is countsAsMoved over this node's copy, its lease, and the map
// this node last reconciled.
func (r *Replicator) stillMoved(s store.Scope) bool {
	_, held := r.follower.copyEpoch(s.Namespace, s.ClientID)
	l, known := r.o.Leases.Lookup(s.Namespace, s.ClientID)
	r.recMu.Lock()
	cur := r.reconciled
	r.recMu.Unlock()
	var targets []string
	if known && l.Owner != "" {
		targets = cur.Targets(r.o.Map.ShardOf(s.Namespace, s.ClientID), l.Owner)
	}
	return countsAsMoved(held, known, l.Owner, r.o.Node, targets)
}

// countsAsMoved decides whether a copy with a moved note counts as kept: it is
// held, and its lease's owner is neither this node nor one that replicates to
// it. It is pure.
func countsAsMoved(held, known bool, owner, self string, ownerTargets []string) bool {
	if !held {
		return false
	}
	return !known || (owner != self && !contains(ownerTargets, self))
}

// recheckUnwritten judges again the copies queued by a confirmation or a
// bound, against the map this node last acted on. A copy that stopped being
// this rule's is left to the next rebalance.
func (r *Replicator) recheckUnwritten() {
	queued := r.unwritten.takeQueued()
	if len(queued) == 0 {
		return
	}
	r.recMu.Lock()
	cur := r.reconciled
	r.recMu.Unlock()
	if cur.Version == 0 || !r.mayDrop(cur, cur) {
		return
	}
	me := r.o.Node
	for _, s := range queued {
		copyEpoch, held := r.follower.copyEpoch(s.Namespace, s.ClientID)
		l, known := r.o.Leases.Lookup(s.Namespace, s.ClientID)
		shard := r.o.Map.ShardOf(s.Namespace, s.ClientID)
		if r.unwritten.keptMoved(s) {
			if !held || !known || l.Owner == "" || l.Owner == me || contains(cur.Targets(shard, l.Owner), me) || !r.ownerAlive(s.Namespace, s.ClientID) {
				r.unwritten.forget(s)
				continue
			}
			r.judgeMoved(s, l.Owner, l.Epoch, cur.Targets(shard, l.Owner))
			continue
		}
		if !held || !known || l.Owner == "" || l.Owner == me || contains(cur.Targets(shard, l.Owner), me) || !r.ownerAlive(s.Namespace, s.ClientID) {
			r.unwritten.forget(s)
			continue
		}
		r.judgeUnfed(s, copyEpoch, l.Owner, l.Epoch, cur.Targets(shard, l.Owner))
	}
	if err := r.follower.sync(); err != nil {
		r.log.Error("replica sync after a recheck failed", "err", err)
	}
}

// WrittenFor is the highest confirmation this node holds for the session:
// the owner that sent it and its token.
func (r *Replicator) WrittenFor(ns, id string) (owner string, token uint64, ok bool) {
	r.unwritten.mu.Lock()
	defer r.unwritten.mu.Unlock()
	w, ok := r.unwritten.written[store.Scope{Namespace: ns, ClientID: id}]
	return w.owner, w.token, ok
}
