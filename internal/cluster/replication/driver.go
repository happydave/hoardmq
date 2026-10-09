package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// driveLoop, on the consensus leader, keeps the shard map computed from the
// live members: a new map is proposed whenever the basis changes. It checks
// on an interval, and at once when this node applies a declaration: until the
// map drops a declared node, publishes whose records target it are refused.
func (r *Replicator) driveLoop() {
	defer close(r.done)
	next := time.Duration(0)
	cause := causeTick
	for {
		select {
		case <-r.stop:
			return
		case <-time.After(next):
		case <-r.basisWake:
			cause = causeDeclaration
		}
		next = r.o.Interval
		r.refreshPresence()
		ok := r.drive(cause)
		cause = causeTick
		if !ok {
			next = max(r.o.Interval/4, 50*time.Millisecond)
			cause = causeRetry
		}
	}
}

// BasisChanged makes the driver check the basis now rather than at its next
// interval. It never blocks; calls made while a check is pending add nothing.
func (r *Replicator) BasisChanged() {
	select {
	case r.basisWake <- struct{}{}:
	default:
	}
}

// drive proposes a map when needed; it returns false when the attempt
// should be retried sooner than the interval.
func (r *Replicator) drive(cause string) bool {
	r.maps.checked(cause)
	alive, leaving := r.placementFacts()
	r.gauge("hoardmq_shardmap_leaving", float64(len(leaving)))
	if r.o.Core.Status().Role != "leader" {
		return true
	}
	basis := placementBasis(alive, leaving, r.o.Replicas)
	if len(basis) == 0 {
		return false
	}
	cur := r.o.Map.Current()
	if cur.Version > 0 && cur.SameBasis(basis, r.o.Replicas) {
		return true
	}
	decided := time.Now()
	m := shardmap.Compute(cur.Version+1, basis, r.o.Replicas, r.o.Map.Count())
	ctx, cancel := context.WithTimeout(r.ctx, r.o.AckTimeout)
	defer cancel()
	res, err := r.o.Core.Propose(ctx, shardmap.Kind, shardmap.Encode(m))
	if err != nil {
		r.log.Debug("shard map proposal failed", "err", err)
		return false
	}
	if sr, ok := res.(shardmap.Result); ok && sr.Applied {
		r.maps.decided(m.Version, m.Basis, cause, decided, time.Now())
		r.audit("shardmap.changed", r.o.Node, "applied", map[string]string{"version": fmt.Sprint(m.Version), "basis": fmt.Sprint(m.Basis), "replicas": fmt.Sprint(r.o.Replicas), "leaving": fmt.Sprint(leaving)})
	}
	return true
}

// placementFacts are the members the shard map is computed over: this node
// and every member the lease table says is alive, and those of them a drain's
// hand-off marked leaving.
func (r *Replicator) placementFacts() (alive, leaving []string) {
	seen := map[string]bool{r.o.Node: true}
	for _, m := range r.o.Core.Members() {
		if m.Name != r.o.Node && r.o.Leases.Live(m.Name) {
			seen[m.Name] = true
		}
	}
	alive = sortedKeys(seen)
	for _, n := range alive {
		if r.o.Leases.Leaving(n) {
			leaving = append(leaving, n)
		}
	}
	return alive, leaving
}

// placementBasis is the shard map's basis: the alive members less those
// leaving, unless that leaves fewer than the copies the alive members would
// give, when it is every alive member. A drain never lowers the copy count.
// It is pure; the members it leaves out stop being anyone's replica target.
func placementBasis(alive, leaving []string, replicas int) []string {
	var stay []string
	for _, n := range alive {
		if !contains(leaving, n) {
			stay = append(stay, n)
		}
	}
	if len(stay) >= min(max(replicas, 1), len(alive)) {
		return sortedCopy(stay)
	}
	return sortedCopy(alive)
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// Placement reports, for this node while it drains, whether the current
// facts place it (willPlace: it is in the basis the leader would compute),
// whether the shard map has left it out and this node has reconciled that
// map, and whether the lease table marks it leaving. willPlace and marked
// come from one reading of the marks: a mark applied between two readings
// would read as marked and kept by the copy count.
func (r *Replicator) Placement() (willPlace, left, marked bool) {
	alive, leaving := r.placementFacts()
	return placementOf(r.o.Node, alive, leaving, r.o.Replicas, r.o.Map.Current(), r.Reconciled())
}

// placementOf is Placement over given facts: alive and leaving as
// placementFacts reads them, the copy count, the current map and the version
// this node last reconciled. It is pure.
func placementOf(node string, alive, leaving []string, replicas int, cur shardmap.Map, reconciled uint64) (willPlace, left, marked bool) {
	left = cur.Version > 0 && !contains(cur.Basis, node) && reconciled >= cur.Version
	return contains(placementBasis(alive, leaving, replicas), node), left, contains(leaving, node)
}

// mapChanged runs on the apply goroutine: it records the latest map and
// wakes the reconcile worker. Nothing is acted on here.
func (r *Replicator) mapChanged(_, cur shardmap.Map) {
	r.maps.applied(cur.Version, cur.Basis, time.Now())
	r.gauge("hoardmq_shardmap_version", float64(cur.Version))
	r.gauge("hoardmq_shardmap_basis_size", float64(len(cur.Basis)))
	r.recMu.Lock()
	if cur.Version >= r.latest.Version {
		r.latest = cur
		r.pending = true
	}
	r.recMu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// reconcileLoop is the one worker that acts on map changes: each pass
// reconciles from the map it last finished with to the latest, so two
// changes never run concurrently and a map superseded while a pass ran is
// skipped rather than acted on.
func (r *Replicator) reconcileLoop() {
	defer r.wg.Done()
	for {
		// While a retained move is pending, a shard is kept or a release is
		// owed, the worker looks again (retainedPoll): nothing else wakes it.
		var retry <-chan time.Time
		if d, ok := r.retainedPoll(); ok {
			retry = time.After(d)
		}
		select {
		case <-r.stop:
			return
		case <-r.wake:
		case <-retry:
		}
		// Copies a confirmation or a bound asked to judge again, against the
		// map this node last acted on.
		r.recheckUnwritten()
		r.retainedStep()
		for {
			r.recMu.Lock()
			if !r.pending {
				r.recMu.Unlock()
				break
			}
			r.pending = false
			old, cur := r.reconciled, r.latest
			r.recMu.Unlock()
			if cur.Version == 0 {
				continue
			}
			r.rebalance(old, cur)
			// Leadership may have moved under retained messages this node
			// already held, in either direction.
			r.recomputePresence(true)
			r.refreshPresence()
			r.recMu.Lock()
			r.reconciled = cur
			r.recMu.Unlock()
			r.maps.reconciled(cur.Version, time.Now())
			r.writeReconciled(cur.Version)
			r.retainedStep()
			select {
			case <-r.stop:
				return
			default:
			}
		}
	}
}

// Reconciled is the version of the map this node last acted on, for tests.
func (r *Replicator) Reconciled() uint64 {
	r.recMu.Lock()
	defer r.recMu.Unlock()
	return r.reconciled.Version
}

// reconciledFile holds the last reconciled map version across restarts.
const reconciledFile = "reconciled.json"

// readReconciled reads the version the previous incarnation last acted on.
// No directory or no file is a node that never reconciled: version 0, known.
// A file that exists and cannot be read is unknown: the floor a drop needs is
// missing, and a drop against a map below it would discard the last copy of
// a session whose owner is gone — so nothing is dropped until this
// incarnation has reconciled and written a version of its own.
func (r *Replicator) readReconciled() (version uint64, known bool) {
	if r.o.ReplicaDir == "" {
		return 0, true
	}
	data, err := os.ReadFile(filepath.Join(r.o.ReplicaDir, reconciledFile))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, true
		}
		r.log.Warn("reconciled map version unreadable; no replica copy is dropped until one is written", "err", err)
		return 0, false
	}
	var v struct {
		Version uint64 `json:"version"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		r.log.Warn("reconciled map version unreadable; no replica copy is dropped until one is written", "err", err)
		return 0, false
	}
	return v.Version, true
}

func (r *Replicator) writeReconciled(version uint64) {
	if r.o.ReplicaDir == "" {
		return
	}
	data, _ := json.Marshal(struct {
		Version uint64 `json:"version"`
	}{version})
	path := filepath.Join(r.o.ReplicaDir, reconciledFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		r.log.Warn("reconciled map version not written", "err", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		r.log.Warn("reconciled map version not written", "err", err)
		return
	}
	r.recMu.Lock()
	r.persisted, r.persistedKnown = version, true
	r.recMu.Unlock()
}

// rebalance moves state after a map change: owned sessions are sent whole
// to targets they gained; replicas of shards this node no longer holds are
// dropped; retained messages follow their shard's leader (promoted from a
// replica, or handed over by an old holder), and new followers receive
// them.
func (r *Replicator) rebalance(old, cur shardmap.Map) {
	me := r.o.Node
	st := r.o.Sessions.Snapshot()
	type movedFrom struct {
		ns, id string
		lost   []string
	}
	var movedOut []movedFrom
	if r.fanOutGate != nil {
		r.fanOutGate()
	}
	if r.o.Transport != nil {
		bulk := newBulk("rebalance")
		for _, ss := range st.Sessions {
			shard := r.o.Map.ShardOf(ss.Namespace, ss.ClientID)
			var before []string
			if old.Version > 0 {
				before = old.Targets(shard, me)
			}
			now := cur.Targets(shard, me)
			for _, t := range now {
				if !contains(before, t) {
					bulk.note(r.outboundFor(t))
					r.sendSnapshot(t, ss)
				}
			}
			var lost []string
			for _, t := range before {
				if !contains(now, t) {
					lost = append(lost, t)
				}
			}
			if len(lost) > 0 {
				movedOut = append(movedOut, movedFrom{ss.Namespace, ss.ClientID, lost})
			}
		}
		bulk.done()
	}
	r.fanned.Store(cur.Version)
	if r.o.ConfirmMoved != nil {
		// Queued after the pushes, so the durability wait each confirmation
		// makes covers the whole write a gained target was sent.
		for _, m := range movedOut {
			r.o.ConfirmMoved(m.ns, m.id, m.lost)
		}
	}
	// Replicas this node no longer holds. A copy is only ever dropped on a
	// map that moved forward, past what this node acted on in this and any
	// previous incarnation: dropping against an older map would discard the
	// surviving copy of a session whose owner is already gone. The same floor
	// covers retained copies below.
	rep := r.follower.snapshot(holdSnapshotRebalance)
	mayDrop := r.mayDrop(old, cur)
	served := make(map[store.Scope]bool, len(st.Sessions))
	for _, ss := range st.Sessions {
		served[store.Scope{Namespace: ss.Namespace, ClientID: ss.ClientID}] = true
	}
	// A shard whose holders the map did not move cannot have changed who is
	// replicated to, so its copies are not looked at again: on a rebalance
	// that moves a few shards this keeps the lease lookups below to the
	// sessions those shards carry, rather than one per copy the node holds.
	moved := movedShards(old, cur)
	if mayDrop {
		for _, ss := range rep.Sessions {
			shard := r.o.Map.ShardOf(ss.Namespace, ss.ClientID)
			held := cur.Holds(shard, me)
			if held && (r.o.KeepUnfedCopies || !moved[shard]) {
				// Defects, or a shard that did not move: a holder keeps what
				// it has, as it did before a copy was tied to being a target.
				continue
			}
			owner, ownerKnown, token := "", false, uint64(0)
			if l, ok := r.o.Leases.Lookup(ss.Namespace, ss.ClientID); ok {
				owner, ownerKnown, token = l.Owner, true, l.Epoch
			}
			if ownerKnown && owner == me && !r.o.HandOverOwnedCopies {
				if dropOwnedCopy(served[store.Scope{Namespace: ss.Namespace, ClientID: ss.ClientID}]) {
					r.follower.dropCopy(ss.Namespace, ss.ClientID, "owned-rebalance")
				} else {
					r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "owned-unserved"}, 1)
				}
				continue
			}
			if ownerKnown && owner != me && !r.o.DeleteWithoutHandoverAck && old.Version > 0 && (!held || ss.Epoch == token) &&
				stoppedBeingTarget(old, cur, shard, owner, me) && r.ownerAlive(ss.Namespace, ss.ClientID) {
				// This node was the session's target and is not now: the owner
				// confirms once its new targets hold the session, and until
				// then this copy may be the only other one. A copy from another
				// grant on a shard this node still holds waits for the owner's
				// write instead (judgeUnfed).
				r.judgeMoved(store.Scope{Namespace: ss.Namespace, ClientID: ss.ClientID}, owner, token, cur.Targets(shard, owner))
				continue
			}
			if held {
				// Still a holder of the shard. An owner replicates to at most
				// replicas-1 of a shard's holders, so holding it is not
				// enough: a copy this node is not a target for receives
				// nothing, and a takeover that read it would hand the client
				// a queue from before the map moved (WI 1569).
				scope := store.Scope{Namespace: ss.Namespace, ClientID: ss.ClientID}
				if !ownerKnown || owner == me || contains(cur.Targets(shard, owner), me) {
					r.unwritten.forget(scope)
					continue
				}
				if !r.ownerAlive(ss.Namespace, ss.ClientID) {
					// The owner is gone, so no target is being fed either and
					// this copy may be the last one.
					r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-not-alive"}, 1)
					continue
				}
				r.judgeUnfed(scope, ss.Epoch, owner, token, cur.Targets(shard, owner))
				continue
			}
			// This node stops holding the copy: it hands it to the shard's
			// new holders itself and lets go only once they acknowledge —
			// the owner's own push may be lost, and the owner may die before
			// it writes again.
			acked := r.handOver(ss, cur.Holders(shard))
			switch shouldDropCopy(true, r.ownerAlive(ss.Namespace, ss.ClientID), acked) {
			case keepOwnerNotAlive:
				// If the owner is gone, this may be the last copy of its
				// queue and subscriptions, and the node taking the lease
				// over has nowhere else to read them from.
				r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "owner-not-alive"}, 1)
				continue
			case keepHandoverUnacknowledged:
				r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "handover-unacknowledged"}, 1)
				if ownerKnown && owner != me {
					// A holder of the session refuses it, and every holder refuses
					// an earlier grant's copy. Kept with no check, it falls behind
					// the owner's later writes; on its grant's rule it goes on the
					// owner's word or a target seen holding the session.
					r.judgeUnfed(store.Scope{Namespace: ss.Namespace, ClientID: ss.ClientID}, ss.Epoch, owner, token, cur.Targets(shard, owner))
				}
				continue
			}
			r.log.Debug("replica dropped after hand-over", "client", ss.ClientID, "copyEpoch", ss.Epoch, "entries", len(ss.Entries), "holders", cur.Holders(shard), "map", cur.Version)
			if _, err := r.follower.apply(store.DeleteRecord(ss.Namespace, ss.ClientID)); err != nil {
				r.log.Error("replica drop failed", "err", err)
			}
		}
	} else {
		kept := 0
		for _, ss := range rep.Sessions {
			if !cur.Holds(r.o.Map.ShardOf(ss.Namespace, ss.ClientID), me) {
				kept++
			}
		}
		if kept > 0 {
			r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "map-behind"}, float64(kept))
		}
		r.log.Warn("shard map behind what this node last acted on; replicas kept", "from", old.Version, "to", cur.Version, "persisted", r.persisted, "floorKnown", r.persistedKnown, "sessions", kept)
	}
	for shard := 0; shard < r.o.Map.Count(); shard++ {
		r.rebalanceRetained(old, cur, shard, st.Retained, rep.Retained, mayDrop)
	}
	r.beginRetainedMoves(old, cur)
	if err := r.follower.sync(); err != nil {
		r.log.Error("replica sync after rebalance failed", "err", err)
	}
	r.log.Info("shard map applied", "version", cur.Version, "basis", cur.Basis, "replicas", cur.Replicas)
}

// stoppedBeingTarget reports whether node was one of owner's replica targets
// for the shard under old and is not under cur.
func stoppedBeingTarget(old, cur shardmap.Map, shard int, owner, node string) bool {
	return contains(old.Targets(shard, owner), node) && !contains(cur.Targets(shard, owner), node)
}

// movedShards marks the shards whose holders differ between two maps. A
// shard that did not move keeps the same targets for every owner, so nothing
// about which copies belong on this node can have changed for it.
func movedShards(old, cur shardmap.Map) map[int]bool {
	out := make(map[int]bool)
	for shard := 0; shard < len(cur.Shards); shard++ {
		before := []string(nil)
		if old.Version > 0 && shard < len(old.Shards) {
			before = old.Holders(shard)
		}
		now := cur.Holders(shard)
		if len(before) != len(now) {
			out[shard] = true
			continue
		}
		for i := range now {
			if before[i] != now[i] {
				out[shard] = true
				break
			}
		}
	}
	return out
}

// dropOwnedCopy decides what happens to a replica copy of a session whose
// lease this node owns. It is never handed over: it is the previous owner's
// state, not this node's. It is dropped once this node serves the session,
// which is then the truth. Until then it is kept: a reassignment names this
// node owner the moment it applies, the adoption imports the session later,
// and between the two this copy may be the only one there is.
func dropOwnedCopy(servedHere bool) bool { return servedHere }

// dropVerdict is what shouldDropCopy decides for a copy this node no longer
// holds under the new map.
type dropVerdict int

const (
	dropCopy dropVerdict = iota
	keepMapBehind
	keepOwnerNotAlive
	keepHandoverUnacknowledged
)

// shouldDropCopy is the decision to delete a session replica, taken as a
// function of the three facts it needs: the map has moved past what this
// node acted on, the session's owner is alive to write new copies, and the
// new holders acknowledged the copy this node handed them.
func shouldDropCopy(mapPastFloor, ownerAlive, handoverAcknowledged bool) dropVerdict {
	switch {
	case !mapPastFloor:
		return keepMapBehind
	case !ownerAlive:
		return keepOwnerNotAlive
	case !handoverAcknowledged:
		return keepHandoverUnacknowledged
	}
	return dropCopy
}

// handOver pushes this node's copy of a session to each of the shard's
// holders that is not this node, and reports whether every one of them
// acknowledged it within the acknowledgement timeout. A holder that is down,
// refuses it, or does not answer in time is not holding it, and the copy
// stays here. With DeleteWithoutHandoverAck the push is made and the answer
// not waited for: the defect this exists to remove.
func (r *Replicator) handOver(ss *store.SessionState, holders []string) bool {
	if r.o.Transport == nil {
		return true
	}
	// The owner holds the session itself and takes no copy of it; the
	// other holders do.
	owner := ""
	if l, ok := r.o.Leases.Lookup(ss.Namespace, ss.ClientID); ok {
		owner = l.Owner
	}
	var targets []string
	for _, h := range holders {
		if h != r.o.Node && h != owner {
			targets = append(targets, h)
		}
	}
	if len(targets) == 0 {
		return true
	}
	for _, t := range targets {
		r.log.Debug("replica handed over", "client", ss.ClientID, "to", t, "copyEpoch", ss.Epoch, "entries", len(ss.Entries), "owner", owner)
		if r.o.HandOverOwnedCopies {
			r.sendSnapshot(t, ss)
		} else {
			r.sendHandOver(t, ss)
		}
	}
	if r.o.DeleteWithoutHandoverAck {
		return true
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.o.AckTimeout)
	defer cancel()
	keys := []store.Scope{{Namespace: ss.Namespace, ClientID: ss.ClientID}}
	for _, t := range targets {
		if err := r.outboundFor(t).wait(ctx, r.up, keys); err != nil {
			r.log.Warn("session hand-over not acknowledged; copy kept", "client", ss.ClientID, "to", t, "err", err)
			return false
		}
	}
	return true
}

// mayDrop reports whether copies this node no longer holds under cur may be
// dropped: the map moved forward past what this node acted on in this and any
// previous incarnation, and that floor is known.
func (r *Replicator) mayDrop(old, cur shardmap.Map) bool {
	r.recMu.Lock()
	persisted, known := r.persisted, r.persistedKnown
	r.recMu.Unlock()
	return known && cur.Version >= old.Version && cur.Version >= persisted
}

// ownerAlive reports whether the session's lease owner is alive by the log:
// not declared dead, and named by the current shard map's basis. An unknown
// lease or an unknown owner counts as not alive: the question is only ever
// asked before discarding a copy, so uncertainty keeps it.
//
// The basis is half of the answer because the two verdicts reach the log
// separately. A dead owner leaves the map first or is declared first, in
// either order, and the declaration moves its leases to a live adopter in the
// same command. The copies at risk are the ones examined between the two,
// when the map that started this rebalance has already dropped the owner and
// nothing has declared it.
func (r *Replicator) ownerAlive(ns, id string) bool {
	l, ok := r.o.Leases.Lookup(ns, id)
	if !ok || l.Owner == "" {
		return false
	}
	return ownerAliveByLog(r.o.Leases.Live(l.Owner), contains(r.o.Map.Current().Basis, l.Owner))
}

// ownerAliveByLog is the two facts ownerAlive needs.
func ownerAliveByLog(live, inBasis bool) bool { return live && inBasis }

// retainedHandOver pushes retained messages to the new leader of their shard
// and reports whether it acknowledged them. A leader that already held the
// shard has them and is not sent them; one that is down cannot take them.
func (r *Replicator) retainedHandOver(leader string, msgs []*message.Message, version uint64, leaderHeld bool) bool {
	if r.o.Transport == nil || leaderHeld {
		return true
	}
	return r.handOverRetained(leader, msgs, version)
}

// handOverRetained sends retained copies to a shard's leader, marked as a
// hand-over, and reports whether it acknowledged them: installed, or held
// already, which the leader keeps over the hand-over's copy.
func (r *Replicator) handOverRetained(leader string, msgs []*message.Message, version uint64) bool {
	if r.o.Transport == nil {
		return true
	}
	if !r.up(leader) {
		return false
	}
	marked := !r.o.PreRetainedMoves
	keys := make([]store.Scope, 0, len(msgs))
	for _, m := range msgs {
		r.outboundFor(leader).push(frame{Kind: kindRetainedWrite, Namespace: m.Namespace, Topic: m.Topic, Epoch: version, Msg: m, Handover: marked})
		keys = append(keys, store.Scope{Namespace: m.Namespace, Topic: m.Topic})
	}
	if r.o.DeleteWithoutHandoverAck {
		return true
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.o.AckTimeout)
	defer cancel()
	if err := r.outboundFor(leader).wait(ctx, r.up, keys); err != nil {
		r.log.Warn("retained hand-over not acknowledged; copies kept", "to", leader, "count", len(msgs), "err", err)
		return false
	}
	return true
}

func inShard(r *Replicator, msgs []*message.Message, shard int) []*message.Message {
	var out []*message.Message
	for _, m := range msgs {
		if r.o.Map.ShardOf(m.Namespace, m.Topic) == shard {
			out = append(out, m)
		}
	}
	return out
}

func (r *Replicator) rebalanceRetained(old, cur shardmap.Map, shard int, local, replica []*message.Message, mayDrop bool) {
	me := r.o.Node
	newLeader := cur.Shards[shard].Leader
	oldLeader := ""
	wasHolder := false
	if old.Version > 0 {
		oldLeader = old.Shards[shard].Leader
		wasHolder = old.Holds(shard, me)
	}
	mine := inShard(r, local, shard)
	held := inShard(r, replica, shard)
	// A node that stops holding the shard keeps its copies until the shard's
	// leader under cur releases them: until the new holders hold the shard,
	// they may be the only copies but the leader's.
	keep := !r.o.PreRetainedMoves && !r.o.DropMovedRetained && mayDrop && wasHolder && !cur.Holds(shard, me) && newLeader != ""
	keptOwn := false
	handedOver := true
	switch {
	case newLeader == me && oldLeader != me:
		// Promote: whatever this node replicated becomes the local store;
		// the tap replicates it to the new followers.
		for _, m := range held {
			_ = r.o.Sessions.InjectRetained(m)
		}
		if len(held) > 0 {
			r.count("hoardmq_retained_promotions_total", nil, float64(len(held)))
			r.log.Debug("replica retained copies promoted", "shard", shard, "map", cur.Version, "topics", len(held))
		}
	case newLeader != me && oldLeader == me:
		// Hand over to the new leader, then let go of the local copies —
		// once the new leader acknowledged them; otherwise they stay here,
		// counted, and the hand-over is made again. (Kept copies come back
		// on a later promotion; lost ones do not come back at all.) A node
		// that no longer holds the shard keeps them instead, until the new
		// leader releases them. A new leader that followed the shard gets
		// them too: its push may never have landed.
		leaderHeld := old.Holds(shard, newLeader) && (r.o.PreRetainedMoves || r.o.NoHandOverAtLeaderChange)
		acked := len(mine) == 0 || r.retainedHandOver(newLeader, mine, cur.Version, leaderHeld)
		switch {
		case !acked:
			r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "handover-unacknowledged"}, float64(len(mine)))
			if !r.o.PreRetainedMoves {
				// The new leader's move waits for this hand-over: it is made
				// again until acknowledged (retryHandOver).
				r.keepRetained(shard, newLeader, cur.Version, true, true)
				return
			}
		case keep && len(mine) > 0:
			keptOwn = true
		case r.o.PreRetainedMoves:
			r.dropOwnRetained(shard)
		default:
			r.endOwnAfterHandOver(cur, shard, newLeader)
		}
		if acked && !leaderHeld && !r.o.PreRetainedMoves && r.o.Transport != nil {
			// The new leader waits for this hand-over before it releases the
			// shard's old holders.
			r.pushRelease(newLeader, cur.Version, shard)
		}
	case newLeader != me && wasHolder && oldLeader != me && (!old.Holds(shard, newLeader) || newLeader != oldLeader && !r.o.PreRetainedMoves && !r.o.NoHandOverAtLeaderChange):
		// A follower hands over to a new leader for a dead or absent one: a
		// new leader that held nothing has only what is handed to it, and
		// one that followed may lack what a lost push carried. Its copies are
		// dropped below only if the new leader acknowledged them.
		if len(held) > 0 && !r.retainedHandOver(newLeader, held, cur.Version, false) {
			r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "handover-unacknowledged"}, float64(len(held)))
			handedOver = false
			if !r.o.PreRetainedMoves && !r.o.NoHandOverRetry && mayDrop {
				// These may be the only copies: the hand-over is made again
				// until acknowledged (retryHandOver), and no release drops
				// them meanwhile (owesHandOver).
				r.keepRetained(shard, newLeader, cur.Version, false, true)
				return
			}
		}
	}
	if newLeader == me && r.o.Transport != nil && r.o.PreRetainedMoves {
		// New followers get the shard's retained messages whole; a build with
		// retained moves pushes them in beginRetainedMoves, after a release.
		for _, f := range cur.Shards[shard].Followers {
			if old.Version > 0 && old.Holds(shard, f) && oldLeader == me {
				continue
			}
			for _, m := range mine {
				rec := store.RetainRecord(m)
				r.outboundFor(f).push(frame{Kind: kindAppend, Type: rec.Type, Namespace: m.Namespace, Topic: m.Topic, Epoch: cur.Version, Payload: rec.Payload})
			}
		}
	}
	if keep && (keptOwn || len(held) > 0) {
		r.keepRetained(shard, newLeader, cur.Version, keptOwn, false)
		return
	}
	if !cur.Holds(shard, me) || newLeader == me {
		// Replica retained copies are for followers only. Dropped under the
		// same floor as session copies: on a map behind what this node acted
		// on, a copy this node holds may be the last one; and only once a
		// hand-over this node owed was acknowledged.
		if !mayDrop {
			if len(held) > 0 {
				r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "map-behind"}, float64(len(held)))
			}
			return
		}
		if !handedOver {
			return
		}
		if newLeader != me {
			if _, noted := r.keptNote(shard); noted && !r.o.PreRetainedMoves {
				// Kept from an earlier move, or across a restart: its release
				// or its hand-on ends it (judgeKeptRetained).
				return
			}
		}
		for _, m := range held {
			if _, err := r.follower.apply(store.RetainRecord(&message.Message{Namespace: m.Namespace, Topic: m.Topic})); err != nil {
				r.log.Error("replica retained drop failed", "err", err)
			}
		}
		if len(held) > 0 {
			r.log.Debug("replica retained copies dropped at a rebalance", "shard", shard, "map", cur.Version, "topics", len(held))
		}
	}
}
