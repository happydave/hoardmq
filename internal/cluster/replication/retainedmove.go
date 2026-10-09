package replication

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// A retained shard moves when a map change changes its holders. Its leader
// under the new map owns the move: it sends each new follower a release of the
// shard and then the shard whole, and once they hold it, sends a release to
// every node that held the shard and no longer does. Until then it acknowledges
// no retained write on the shard (Barrier), and every old holder keeps its
// copies: a leader that dies before its new followers hold the shard would
// otherwise take the shard's messages with it. A kept copy whose releaser is no
// longer alive by the log is handed to the shard's leader, which installs only
// topics it holds nothing for. No time ends a keep.

// kindRetainedRelease tells the receiver that the sender's followers hold the
// shard named in Shards, under the map version in Epoch: copies of the shard
// it keeps are no longer needed. A new follower is sent one ahead of the
// shard's whole push, which clears what it kept from an earlier move. A peer
// that does not know the kind acknowledges it unapplied, which is safe: such a
// peer dropped its copies at its rebalance and has nothing to release.
const kindRetainedRelease byte = 10

// keptRetainedFile holds the kept shards' notes across restarts.
const keptRetainedFile = "retained-kept.json"

// retainedNote is what an old holder knows about a shard it keeps: the
// shard's leader under the map it stopped holding the shard at, which is the
// node whose release ends the keep; that map's version; and whether it keeps
// its own copies, as the shard's former leader, as well as replica copies.
type retainedNote struct {
	Releaser string `json:"releaser"`
	Version  uint64 `json:"version"`
	Own      bool   `json:"own,omitempty"`
	// HandOver marks a former leader whose hand-over of its own copies the
	// shard's leader has not acknowledged: it is made again until it is.
	HandOver bool      `json:"handOver,omitempty"`
	Since    time.Time `json:"since"`
}

// pendingMove is a move of a shard this node leads: the sequence of the last
// frame pushed to each new follower, the sequence of the release sent to each
// old holder (zero before one is sent), and the former leader whose hand-over
// this node waits for when it did not hold the shard before.
type pendingMove struct {
	version      uint64
	newFollowers map[string]uint64
	oldHolders   map[string]uint64
	waitHandover string
	handoverIn   bool
	began        time.Time
}

// passedRelease is a release owed to a node a move completed without, because
// it was not alive by the log: it is sent once the node is again.
type passedRelease struct {
	version uint64
	seq     uint64
}

// retainedMoves is a node's share of every retained move: what it keeps, the
// moves it leads, and the releases it owes nodes it passed over.
type retainedMoves struct {
	mu      sync.Mutex
	kept    map[int]retainedNote
	pending map[int]*pendingMove
	passed  map[int]map[string]passedRelease
	// received is the highest map version each peer released a shard at, so
	// a new leader learns its former leader's hand-over is in even when the
	// release arrives before this node has recorded the move.
	received map[int]map[string]uint64
}

func newRetainedMoves() *retainedMoves {
	return &retainedMoves{kept: map[int]retainedNote{}, pending: map[int]*pendingMove{}, passed: map[int]map[string]passedRelease{}, received: map[int]map[string]uint64{}}
}

// releaseDrops decides whether a release of a shard drops this node's copies
// of it. A leader's own store is the shard's truth and never goes; a node that
// holds the shard under a map newer than the release's was given it back
// after the release was sent, and keeps its copies as a follower's; a copy
// kept for a releaser that is gone is handed to the shard's leader first
// (orphaned), since the sender's followers hold only what the gone releaser
// had pushed them; and a copy this node still owes the sender (owed,
// owesHandOver) may be one the sender does not hold. It is pure.
func releaseDrops(leadsNow, holdsNow, orphaned, owed bool, releaseVersion, mapVersion uint64) bool {
	if leadsNow || orphaned || owed {
		return false
	}
	return !holdsNow || releaseVersion >= mapVersion
}

// owesHandOver decides whether this node owes a release's sender a hand-over
// of the shard, so that the release must not drop its copies yet: its note
// records a hand-over not yet acknowledged, to whichever node leads the shard
// now, so no node has been shown to hold its copies; or it has not acted
// on the release's map, and either has acted on no map at all or, under the
// map it last acted on, held the shard while the sender did not lead it. Only
// a leader's store is shown to hold every acknowledged write: a follower's
// may lack a push that never landed, and a node that held nothing has only
// what was handed to it. It is pure.
func owesHandOver(noteOwesSender bool, reconciledVersion, releaseVersion uint64, heldThen, senderLedThen bool) bool {
	if noteOwesSender {
		return true
	}
	if reconciledVersion >= releaseVersion {
		return false
	}
	if reconciledVersion == 0 {
		return true
	}
	return heldThen && !senderLedThen
}

// releasedEarlier decides a kept shard whose note owes no hand-over: act is
// whether its releaser's release, at or above the note's version, was already
// received while the node still owed a hand-over and kept its copies; drop is
// whether acting deletes them, which it does only on a node that neither leads
// nor holds the shard now. It is pure.
func releasedEarlier(received, noteVersion uint64, leadsNow, holdsNow bool) (act, drop bool) {
	act = noteVersion > 0 && received >= noteVersion
	return act, act && !leadsNow && !holdsNow
}

// movingShard decides whether a shard this node leads is moving: a move of
// it is pending, or the map is newer than the one this node last reconciled
// and changed the shard's holders. It is pure. The second clause covers the
// time between a map's apply and the rebalance that records the move, so a
// write cannot pass on a new map before its move is known.
func movingShard(pending bool, curVersion, reconciledVersion uint64, changed bool) bool {
	return pending || (curVersion > reconciledVersion && changed)
}

// keptVerdict is what happens to a kept shard's copies at a rebalance or a
// retry.
type keptVerdict int

const (
	keepRetained keptVerdict = iota
	promoteRetained
	handOnRetained
)

// judgeKept decides a kept shard's copies: promoted when this node leads the
// shard now; handed on once the releaser is not alive by the log and the
// shard has a leader that is (leaderAlive: another node, alive, leads it under
// this node's map, so not a map still naming the gone releaser); kept
// otherwise, until the releaser's release (or, for a node that follows the
// shard again, its release and whole push) ends the keep. It is pure, and
// decides no deletion by itself: a hand-on deletes only once acknowledged,
// and not at all from a node that follows the shard again.
func judgeKept(releaserAlive, leadsNow, leaderAlive bool) keptVerdict {
	switch {
	case leadsNow:
		return promoteRetained
	case !releaserAlive && leaderAlive:
		return handOnRetained
	}
	return keepRetained
}

// installHandOver decides whether a handed-over retained message is installed:
// only when the leader holds no retained message for its topic. A copy kept
// from before a move may be older than what the leader holds, and a version
// to tell them apart does not exist. It is pure.
func installHandOver(held bool) bool { return !held }

// handOverVerdict is what a node does with a retained hand-over.
type handOverVerdict int

const (
	handOverInstall handOverVerdict = iota
	handOverHeld
	handOverRefuse
)

// judgeHandOver decides a retained hand-over: only the shard's leader under
// the map applied now answers that it holds the topic. Another node that holds
// the topic, or keeps its own copies of the shard for a release, refuses it,
// as those copies may still go; the sender keeps its own and makes it again.
// It is pure.
func judgeHandOver(leadsNow, holdsTopic, keepsOwn bool) handOverVerdict {
	switch {
	case !leadsNow && (holdsTopic || keepsOwn):
		return handOverRefuse
	case !installHandOver(holdsTopic):
		return handOverHeld
	}
	return handOverInstall
}

// moveOwnToReplica decides, for a former leader whose hand-over the shard's
// new leader acknowledged, whether its own copies become replica copies
// rather than going: it still holds the shard, and the new leader's release
// at that map, which the shard's whole push follows on the same stream, has
// not reached it. It is pure.
func moveOwnToReplica(holdsNow, releaseReceived bool) bool {
	return holdsNow && !releaseReceived
}

// moveDone decides whether a move is complete: its new followers hold the
// shard, the former leader's hand-over is in when one was awaited, and no
// old holder alive by the log is waiting for its release. It is pure.
func moveDone(newFollowersHeld, handoverIn bool, oldHoldersOutstanding int) bool {
	return newFollowersHeld && handoverIn && oldHoldersOutstanding == 0
}

// holdersChanged reports whether a shard's leader or followers differ between
// two maps; any shard does against a map with no version.
func holdersChanged(old, cur shardmap.Map, shard int) bool {
	if old.Version == 0 || shard >= len(old.Shards) || shard >= len(cur.Shards) {
		return true
	}
	a, b := old.Shards[shard], cur.Shards[shard]
	if a.Leader != b.Leader || len(a.Followers) != len(b.Followers) {
		return true
	}
	for i := range a.Followers {
		if a.Followers[i] != b.Followers[i] {
			return true
		}
	}
	return false
}

// retainedMoving reports whether a retained key's shard is moving at this node,
// its leader under m.
func (r *Replicator) retainedMoving(m shardmap.Map, shard int) bool {
	if r.o.PreRetainedMoves || shard < 0 || shard >= len(m.Shards) || m.Shards[shard].Leader != r.o.Node {
		return false
	}
	r.rmoves.mu.Lock()
	_, pending := r.rmoves.pending[shard]
	r.rmoves.mu.Unlock()
	r.recMu.Lock()
	prev := r.reconciled
	r.recMu.Unlock()
	return movingShard(pending, m.Version, prev.Version, holdersChanged(prev, m, shard))
}

// KeptRetained is how many retained shards this node keeps copies of, waiting
// for their release.
func (r *Replicator) KeptRetained() int {
	r.rmoves.mu.Lock()
	defer r.rmoves.mu.Unlock()
	return len(r.rmoves.kept)
}

// RetainedMovesPending is how many moves this node leads that are not
// complete.
func (r *Replicator) RetainedMovesPending() int {
	r.rmoves.mu.Lock()
	defer r.rmoves.mu.Unlock()
	return len(r.rmoves.pending)
}

// RetainedMovesSample describes up to n pending moves, for a log line: the
// shard, the map version, the new followers with whether each holds the push,
// the old holders with their release's state, and an awaited hand-over.
func (r *Replicator) RetainedMovesSample(n int) []string {
	r.rmoves.mu.Lock()
	type mv struct {
		shard int
		p     pendingMove
		nf    map[string]uint64
		oh    map[string]uint64
	}
	var moves []mv
	for s, p := range r.rmoves.pending {
		c := mv{shard: s, p: *p, nf: map[string]uint64{}, oh: map[string]uint64{}}
		for k, v := range p.newFollowers {
			c.nf[k] = v
		}
		for k, v := range p.oldHolders {
			c.oh[k] = v
		}
		moves = append(moves, c)
	}
	r.rmoves.mu.Unlock()
	sort.Slice(moves, func(i, j int) bool { return moves[i].shard < moves[j].shard })
	var out []string
	for _, m := range moves {
		if len(out) >= n {
			break
		}
		line := "shard " + strconv.Itoa(m.shard) + " v" + strconv.FormatUint(m.p.version, 10)
		for f, seq := range m.nf {
			done, failed := r.outboundFor(f).delivered(seq)
			line += " new:" + f + "@" + strconv.FormatUint(seq, 10) + map[bool]string{true: "+held", false: ""}[done] + map[bool]string{true: "+failed", false: ""}[failed]
		}
		for h, seq := range m.oh {
			line += " old:" + h + "@" + strconv.FormatUint(seq, 10) + map[bool]string{true: "+live", false: "-dead"}[r.o.Leases.Live(h)]
		}
		if m.p.waitHandover != "" {
			line += " handover:" + m.p.waitHandover + map[bool]string{true: "+in", false: ""}[m.p.handoverIn]
		}
		out = append(out, line)
	}
	return out
}

// retainedPollMoving is how often the reconcile worker looks at a pending
// move: the shard refuses retained writes until it completes, so it is
// completed within a few milliseconds of its last acknowledgement.
const retainedPollMoving = 20 * time.Millisecond

// retainedPoll is how soon the reconcile worker should look at the retained
// moves again, and false when nothing waits: a pending move is looked at
// often; a kept shard or an owed release each acknowledgement timeout.
func (r *Replicator) retainedPoll() (time.Duration, bool) {
	r.rmoves.mu.Lock()
	defer r.rmoves.mu.Unlock()
	switch {
	case len(r.rmoves.pending) > 0:
		return retainedPollMoving, true
	case len(r.rmoves.kept) > 0 || len(r.rmoves.passed) > 0:
		return r.o.AckTimeout, true
	}
	return 0, false
}

// readKeptRetained reads the notes a previous incarnation persisted. A file
// that cannot be read is no notes: a restart then drops copies of shards it
// does not hold, as it did before notes existed.
func (r *Replicator) readKeptRetained() map[int]retainedNote {
	out := map[int]retainedNote{}
	if r.o.ReplicaDir == "" {
		return out
	}
	data, err := os.ReadFile(filepath.Join(r.o.ReplicaDir, keptRetainedFile))
	if err != nil {
		if !os.IsNotExist(err) {
			r.log.Warn("kept retained notes unreadable; copies of shards this node does not hold are dropped as before", "err", err)
		}
		return out
	}
	var byShard map[string]retainedNote
	if err := json.Unmarshal(data, &byShard); err != nil {
		r.log.Warn("kept retained notes unreadable; copies of shards this node does not hold are dropped as before", "err", err)
		return out
	}
	for k, n := range byShard {
		if s, err := strconv.Atoi(k); err == nil {
			out[s] = n
		}
	}
	return out
}

// writeKeptRetained persists the notes; called with rmoves.mu held.
func (r *Replicator) writeKeptRetainedLocked() {
	if r.o.ReplicaDir == "" {
		return
	}
	byShard := make(map[string]retainedNote, len(r.rmoves.kept))
	for s, n := range r.rmoves.kept {
		byShard[strconv.Itoa(s)] = n
	}
	data, _ := json.Marshal(byShard)
	path := filepath.Join(r.o.ReplicaDir, keptRetainedFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		r.log.Warn("kept retained notes not written", "err", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		r.log.Warn("kept retained notes not written", "err", err)
	}
}

// keepRetained notes a shard this node stopped holding and keeps, or, with
// handOver, a shard whose hand-over it still owes the shard's leader.
func (r *Replicator) keepRetained(shard int, releaser string, version uint64, own, handOver bool) {
	r.rmoves.mu.Lock()
	n, had := r.rmoves.kept[shard]
	if had {
		// An older note names an earlier releaser; the newest move's leader
		// is the one that releases now. Own copies stay noted, and so does a
		// hand-over still owed: no node has been shown to hold those copies.
		own = own || n.Own
		handOver = handOver || (n.HandOver && !r.o.HandOverNoteOverwritten)
	}
	r.rmoves.kept[shard] = retainedNote{Releaser: releaser, Version: version, Own: own, HandOver: handOver, Since: r.o.Clock()}
	r.writeKeptRetainedLocked()
	r.rmoves.mu.Unlock()
	if !had {
		r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "retained-moved"}, 1)
	}
	r.log.Info("retained copies kept until the shard's leader releases them", "shard", shard, "releaser", releaser, "map", version, "own", own, "handOver", handOver)
}

// keptNote reads a shard's note.
func (r *Replicator) keptNote(shard int) (retainedNote, bool) {
	r.rmoves.mu.Lock()
	defer r.rmoves.mu.Unlock()
	n, ok := r.rmoves.kept[shard]
	return n, ok
}

// receivedFrom is the highest map version peer has released shard at.
func (r *Replicator) receivedFrom(shard int, peer string) uint64 {
	r.rmoves.mu.Lock()
	defer r.rmoves.mu.Unlock()
	return r.rmoves.received[shard][peer]
}

// forgetKept ends a shard's note.
func (r *Replicator) forgetKept(shard int) {
	r.rmoves.mu.Lock()
	if _, ok := r.rmoves.kept[shard]; ok {
		delete(r.rmoves.kept, shard)
		r.writeKeptRetainedLocked()
	}
	r.rmoves.mu.Unlock()
}

// ownRetainedIn is this node's own retained messages in a shard.
func (r *Replicator) ownRetainedIn(shard int) []*message.Message {
	return inShard(r, r.o.Local.AllRetained(), shard)
}

// dropKeptShard deletes every copy this node keeps of a shard: its replica
// copies, and its own when the note says it keeps them. It reports how many
// topics it deleted.
func (r *Replicator) dropKeptShard(shard int, own bool, reason string) int {
	n := 0
	for _, m := range r.follower.retainedIn(shard) {
		if _, err := r.follower.apply(store.RetainRecord(&message.Message{Namespace: m.Namespace, Topic: m.Topic})); err != nil {
			r.log.Error("kept retained copy not dropped", "topic", m.Topic, "err", err)
			continue
		}
		n++
	}
	if own {
		n += r.dropOwnRetained(shard)
	}
	if n > 0 {
		r.count("hoardmq_replicas_dropped_total", map[string]string{"reason": reason}, float64(n))
	}
	return n
}

// ownRetainedMayGo decides whether this node's own retained copies of a shard
// may be deleted: never while it leads the shard under the map applied now.
// Its own store is then the shard's, and a delete there is a clear its tap
// sends every follower. It is pure.
func ownRetainedMayGo(leadsNow bool) bool { return !leadsNow }

// dropOwnRetained deletes this node's own retained copies of a shard, unless
// it leads the shard under the map applied now, and reports how many went.
func (r *Replicator) dropOwnRetained(shard int) int {
	now := r.o.Map.Current()
	if !ownRetainedMayGo(shard < len(now.Shards) && now.Shards[shard].Leader == r.o.Node) {
		r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "retained-own-leads"}, 1)
		r.log.Info("own retained copies not dropped: this node leads the shard now", "shard", shard, "map", now.Version)
		return 0
	}
	n := 0
	for _, m := range r.ownRetainedIn(shard) {
		_ = r.o.Sessions.InjectRetained(&message.Message{Namespace: m.Namespace, Topic: m.Topic})
		n++
	}
	if n > 0 {
		r.log.Debug("own retained copies dropped", "shard", shard, "map", now.Version, "topics", n)
	}
	return n
}

// endOwnAfterHandOver ends a former leader's own copies of a shard once the
// shard's leader under cur has acknowledged them: moved into the replica store
// first while this node still follows the shard and has not had the leader's
// release (moveOwnToReplica), deleted otherwise. A topic the replica store
// holds already keeps its copy.
func (r *Replicator) endOwnAfterHandOver(cur shardmap.Map, shard int, leader string) {
	holds := cur.Holds(shard, r.o.Node)
	received := r.receivedFrom(shard, leader) >= cur.Version
	if r.o.DeleteOwnOnHandOver || !moveOwnToReplica(holds, received) {
		r.dropOwnRetained(shard)
		return
	}
	have := map[string]bool{}
	for _, m := range r.follower.retainedIn(shard) {
		have[m.Namespace+"\x00"+m.Topic] = true
	}
	n := 0
	for _, m := range r.ownRetainedIn(shard) {
		if have[m.Namespace+"\x00"+m.Topic] {
			continue
		}
		if _, err := r.follower.apply(store.RetainRecord(m)); err != nil {
			r.log.Error("own retained copy not kept as a follower's; the own copies stay", "topic", m.Topic, "err", err)
			return
		}
		n++
	}
	if err := r.follower.sync(); err != nil {
		r.log.Error("own retained copies not synced as a follower's; the own copies stay", "shard", shard, "err", err)
		return
	}
	r.log.Debug("own retained copies kept as a follower's until the leader's push", "shard", shard, "leader", leader, "map", cur.Version, "topics", n)
	r.dropOwnRetained(shard)
}

// sameNote reports whether two notes of a shard are the same keep: a note
// made again, or for another releaser, map or hand-over, is another. It is
// pure.
func sameNote(a, b retainedNote) bool {
	return a.Releaser == b.Releaser && a.Version == b.Version && a.Own == b.Own && a.HandOver == b.HandOver && a.Since.Equal(b.Since)
}

// stillJudged reports whether a decision taken on a shard's note and map cur
// still holds: the note is the one judged, and the map applied now is cur.
// Judging reads a snapshot of the notes and blocks on hand-overs, and a
// release or a newer map can change both meanwhile.
func (r *Replicator) stillJudged(shard int, note retainedNote, cur shardmap.Map) bool {
	if r.o.Map.Current().Version != cur.Version {
		return false
	}
	n, ok := r.keptNote(shard)
	return ok && sameNote(n, note)
}

// claimJudged ends a shard's note for an action decided on it and cur, if
// both still hold, and reports whether the action may go ahead.
func (r *Replicator) claimJudged(shard int, note retainedNote, cur shardmap.Map) bool {
	if r.o.StaleKeptJudgement {
		r.forgetKept(shard)
		return true
	}
	if r.o.Map.Current().Version != cur.Version {
		return false
	}
	return r.forgetKeptIf(shard, note)
}

// forgetKeptIf ends a shard's note only if it is still that note, and
// reports whether it did.
func (r *Replicator) forgetKeptIf(shard int, note retainedNote) bool {
	r.rmoves.mu.Lock()
	defer r.rmoves.mu.Unlock()
	n, ok := r.rmoves.kept[shard]
	if !ok || !sameNote(n, note) {
		return false
	}
	delete(r.rmoves.kept, shard)
	r.writeKeptRetainedLocked()
	return true
}

// applyRelease is a release received from peer. On this node's pending move
// it marks the former leader's hand-over in; on a node the release drops, it
// deletes every copy of the shard and the note.
func (r *Replicator) applyRelease(peer string, fr frame) (changed bool) {
	for _, shard := range fr.Shards {
		if r.applyReleaseOf(peer, shard, fr.Epoch) {
			changed = true
		}
	}
	return changed
}

// applyReleaseOf is a release of one shard.
func (r *Replicator) applyReleaseOf(peer string, shard int, version uint64) (changed bool) {
	cur := r.o.Map.Current()
	if shard < 0 || shard >= len(cur.Shards) {
		return false
	}
	r.count("hoardmq_retained_releases_total", map[string]string{"direction": "in"}, 1)
	r.rmoves.mu.Lock()
	if r.rmoves.received[shard] == nil {
		r.rmoves.received[shard] = map[string]uint64{}
	}
	r.rmoves.received[shard][peer] = max(r.rmoves.received[shard][peer], version)
	if mv := r.rmoves.pending[shard]; mv != nil && mv.waitHandover == peer && version >= mv.version {
		mv.handoverIn = true
	}
	r.rmoves.mu.Unlock()
	leads := cur.Shards[shard].Leader == r.o.Node
	if leads {
		return false
	}
	note, noted := r.keptNote(shard)
	orphaned := noted && note.Releaser != peer && !r.o.Leases.Live(note.Releaser)
	r.recMu.Lock()
	acted := r.reconciled
	r.recMu.Unlock()
	owed := false
	if !r.o.IgnoreOwedHandOver {
		heldThen, senderLedThen := false, false
		if acted.Version > 0 && shard < len(acted.Shards) {
			heldThen, senderLedThen = acted.Holds(shard, r.o.Node), acted.Shards[shard].Leader == peer
			if r.o.NoHandOverAtLeaderChange {
				senderLedThen = acted.Holds(shard, peer)
			}
		}
		owed = owesHandOver(noted && note.HandOver, acted.Version, version, heldThen, senderLedThen)
	}
	if !releaseDrops(leads, cur.Holds(shard, r.o.Node), orphaned, owed, version, cur.Version) {
		if owed && (len(r.follower.retainedIn(shard)) > 0 || (noted && note.Own)) {
			r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "retained-handover-owed"}, 1)
			r.log.Debug("retained copies kept through a release; a hand-over to its sender is owed", "shard", shard, "by", peer, "map", version, "acted", acted.Version)
		}
		return false
	}
	n := r.dropKeptShard(shard, noted && note.Own, "retained-released")
	if noted {
		r.forgetKept(shard)
		r.log.Info("kept retained copies released", "shard", shard, "by", peer, "map", version, "topics", n)
	} else if n > 0 {
		r.log.Debug("retained copies dropped by a release", "shard", shard, "by", peer, "map", version, "acted", acted.Version, "topics", n)
	}
	return n > 0
}

// newFollowerPush is one release-then-whole push owed to a new follower.
type newFollowerPush struct {
	peer  string
	shard int
}

// pushReleasesAndShards sends each new follower a release of its shard and
// then the shard whole, read now, under the push lock: a retained record tapped
// meanwhile is either in what was read or queued behind the push, never ahead
// of an older copy that would replace it. It returns the last sequence pushed
// to each.
func (r *Replicator) pushReleasesAndShards(version uint64, owed []newFollowerPush) map[newFollowerPush]uint64 {
	out := make(map[newFollowerPush]uint64, len(owed))
	if len(owed) == 0 || r.o.Transport == nil {
		return out
	}
	if !r.o.UnorderedRetainedPush {
		r.pushMu.Lock()
		defer r.pushMu.Unlock()
	}
	byShard := map[int][]*message.Message{}
	for _, m := range r.o.Local.AllRetained() {
		s := r.o.Map.ShardOf(m.Namespace, m.Topic)
		byShard[s] = append(byShard[s], m)
	}
	if r.pushGate != nil {
		r.pushGate()
	}
	// One release a peer names every shard it is owed, so a map change costs
	// each new follower one frame more than its shards' messages.
	byPeer := map[string][]int{}
	var peers []string
	for _, o := range owed {
		if byPeer[o.peer] == nil {
			peers = append(peers, o.peer)
		}
		byPeer[o.peer] = append(byPeer[o.peer], o.shard)
	}
	for _, peer := range peers {
		ob := r.outboundFor(peer)
		shards := byPeer[peer]
		sort.Ints(shards)
		ob.push(frame{Kind: kindRetainedRelease, Shards: shards, Epoch: version})
		topics := 0
		for _, shard := range shards {
			for _, m := range byShard[shard] {
				rec := store.RetainRecord(m)
				ob.push(frame{Kind: kindAppend, Type: rec.Type, Namespace: m.Namespace, Topic: m.Topic, Epoch: version, Payload: rec.Payload})
				topics++
			}
		}
		seq := ob.lastSent()
		r.log.Debug("retained release and whole push to a new follower", "peer", peer, "shards", shards, "map", version, "topics", topics, "seq", seq)
		for _, shard := range shards {
			out[newFollowerPush{peer: peer, shard: shard}] = seq
		}
	}
	return out
}

// pushRelease sends a release of shards to a peer and returns its sequence.
func (r *Replicator) pushRelease(peer string, version uint64, shards ...int) uint64 {
	ob := r.outboundFor(peer)
	ob.push(frame{Kind: kindRetainedRelease, Shards: shards, Epoch: version})
	r.count("hoardmq_retained_releases_total", map[string]string{"direction": "out"}, 1)
	return ob.lastSent()
}

// beginRetainedMoves records a move for every shard this node leads under cur
// whose holders differ from old, and pushes each new follower its release and
// the shard. Against no map (this node's first reconcile, a restart among
// them) every shard it leads moves, and every other member alive by the log is
// an old holder: what this node owed before it restarted is not remembered.
func (r *Replicator) beginRetainedMoves(old, cur shardmap.Map) {
	if r.o.Transport == nil || r.o.PreRetainedMoves {
		return
	}
	me := r.o.Node
	var members []string
	if old.Version == 0 {
		for _, m := range r.o.Core.Members() {
			members = append(members, m.Name)
		}
	}
	var owed []newFollowerPush
	r.rmoves.mu.Lock()
	for shard := range cur.Shards {
		if cur.Shards[shard].Leader != me {
			continue
		}
		if !holdersChanged(old, cur, shard) {
			continue
		}
		mv := r.rmoves.pending[shard]
		if mv == nil {
			mv = &pendingMove{newFollowers: map[string]uint64{}, oldHolders: map[string]uint64{}, handoverIn: true, began: r.o.Clock()}
			r.rmoves.pending[shard] = mv
		}
		mv.version = cur.Version
		oldLeader := ""
		if old.Version > 0 {
			oldLeader = old.Shards[shard].Leader
		}
		for _, f := range cur.Shards[shard].Followers {
			if f == me || (old.Version > 0 && old.Holds(shard, f) && oldLeader == me) {
				continue
			}
			owed = append(owed, newFollowerPush{peer: f, shard: shard})
		}
		if old.Version > 0 {
			for _, h := range old.Holders(shard) {
				if h != me && !cur.Holds(shard, h) {
					if _, ok := mv.oldHolders[h]; !ok {
						mv.oldHolders[h] = 0
					}
				}
			}
			if !old.Holds(shard, me) && oldLeader != "" && oldLeader != me {
				mv.waitHandover = oldLeader
				mv.handoverIn = r.rmoves.received[shard][oldLeader] >= cur.Version
			}
		} else {
			for _, n := range members {
				if n != me && !cur.Holds(shard, n) {
					if _, ok := mv.oldHolders[n]; !ok {
						mv.oldHolders[n] = 0
					}
				}
			}
		}
		for h := range mv.oldHolders {
			if cur.Holds(shard, h) {
				delete(mv.oldHolders, h)
			}
		}
	}
	r.rmoves.mu.Unlock()
	seqs := r.pushReleasesAndShards(cur.Version, owed)
	r.rmoves.mu.Lock()
	for o, seq := range seqs {
		if mv := r.rmoves.pending[o.shard]; mv != nil {
			mv.newFollowers[o.peer] = seq
		}
	}
	r.gauge("hoardmq_retained_moves_pending", float64(len(r.rmoves.pending)))
	r.rmoves.mu.Unlock()
}

// advanceRetainedMoves moves each pending move as far as it can go without
// waiting: a new follower whose push failed is pushed again; once the new
// followers hold the shard and any awaited hand-over is in, each old holder
// alive by the log is sent its release; a move with nothing outstanding is
// complete. Releases owed to passed-over nodes are sent once they are alive.
func (r *Replicator) advanceRetainedMoves(cur shardmap.Map) {
	if r.o.Transport == nil || r.o.PreRetainedMoves || cur.Version == 0 {
		return
	}
	me := r.o.Node
	type send struct {
		peer    string
		shard   int
		version uint64
		passed  bool
	}
	var repush []newFollowerPush
	var sends []send
	r.rmoves.mu.Lock()
	shards := make([]int, 0, len(r.rmoves.pending))
	for s := range r.rmoves.pending {
		shards = append(shards, s)
	}
	sort.Ints(shards)
	for _, shard := range shards {
		mv := r.rmoves.pending[shard]
		if shard >= len(cur.Shards) || cur.Shards[shard].Leader != me {
			// Another node leads the shard now and owns its move.
			delete(r.rmoves.pending, shard)
			continue
		}
		held := true
		for f, seq := range mv.newFollowers {
			if !cur.Holds(shard, f) {
				delete(mv.newFollowers, f)
				continue
			}
			done, failed := r.outboundFor(f).delivered(seq)
			if failed || seq == 0 {
				held = false
				if r.up(f) {
					repush = append(repush, newFollowerPush{peer: f, shard: shard})
				}
				continue
			}
			if !done {
				held = false
				if r.o.Clock().Sub(mv.began) >= r.o.AckTimeout {
					r.outboundFor(f).probeIfQuiet()
				}
			}
		}
		if !mv.handoverIn && (mv.waitHandover == "" || !r.o.Leases.Live(mv.waitHandover)) {
			mv.handoverIn = true
		}
		if !held || !mv.handoverIn {
			continue
		}
		outstanding := 0
		for h, seq := range mv.oldHolders {
			if cur.Holds(shard, h) {
				delete(mv.oldHolders, h)
				continue
			}
			if !r.o.Leases.Live(h) {
				// Not waited for: it is released when it is alive again.
				if r.rmoves.passed[shard] == nil {
					r.rmoves.passed[shard] = map[string]passedRelease{}
				}
				r.rmoves.passed[shard][h] = passedRelease{version: mv.version}
				delete(mv.oldHolders, h)
				continue
			}
			outstanding++
			if seq != 0 {
				done, failed := r.outboundFor(h).delivered(seq)
				if done {
					delete(mv.oldHolders, h)
					outstanding--
					continue
				}
				if !failed {
					if r.o.Clock().Sub(mv.began) >= r.o.AckTimeout {
						r.outboundFor(h).probeIfQuiet()
					}
					continue
				}
			}
			if r.up(h) {
				sends = append(sends, send{peer: h, shard: shard, version: mv.version})
			}
		}
		if moveDone(held, mv.handoverIn, outstanding) {
			delete(r.rmoves.pending, shard)
			r.count("hoardmq_retained_moves_total", map[string]string{"outcome": "complete"}, 1)
			r.log.Debug("retained move complete", "shard", shard, "map", mv.version, "seconds", r.o.Clock().Sub(mv.began).Seconds())
		}
	}
	for shard, holders := range r.rmoves.passed {
		for h, pr := range holders {
			if cur.Holds(shard, h) {
				delete(holders, h)
				continue
			}
			if !r.o.Leases.Live(h) {
				continue
			}
			if pr.seq != 0 {
				done, failed := r.outboundFor(h).delivered(pr.seq)
				if done {
					delete(holders, h)
					continue
				}
				if !failed {
					continue
				}
			}
			if r.up(h) {
				sends = append(sends, send{peer: h, shard: shard, version: pr.version, passed: true})
			}
		}
		if len(holders) == 0 {
			delete(r.rmoves.passed, shard)
		}
	}
	r.rmoves.mu.Unlock()

	seqs := r.pushReleasesAndShards(cur.Version, repush)
	// Releases to one peer at one version go in one frame.
	type batchKey struct {
		peer    string
		version uint64
	}
	batches := map[batchKey][]int{}
	var order []batchKey
	for _, s := range sends {
		k := batchKey{s.peer, s.version}
		if batches[k] == nil {
			order = append(order, k)
		}
		batches[k] = append(batches[k], s.shard)
	}
	batchSeq := map[batchKey]uint64{}
	for _, k := range order {
		batchSeq[k] = r.pushRelease(k.peer, k.version, batches[k]...)
	}
	sent := make([]uint64, len(sends))
	for i, s := range sends {
		sent[i] = batchSeq[batchKey{s.peer, s.version}]
	}
	r.rmoves.mu.Lock()
	for o, seq := range seqs {
		if mv := r.rmoves.pending[o.shard]; mv != nil {
			mv.newFollowers[o.peer] = seq
		}
	}
	for i, s := range sends {
		if s.passed {
			if hs := r.rmoves.passed[s.shard]; hs != nil {
				if pr, ok := hs[s.peer]; ok {
					pr.seq = sent[i]
					hs[s.peer] = pr
				}
			}
			continue
		}
		if mv := r.rmoves.pending[s.shard]; mv != nil {
			if _, ok := mv.oldHolders[s.peer]; ok {
				mv.oldHolders[s.peer] = sent[i]
			}
		}
	}
	r.gauge("hoardmq_retained_moves_pending", float64(len(r.rmoves.pending)))
	r.rmoves.mu.Unlock()
}

// judgeKeptRetained decides every kept shard against cur: a shard this node
// leads now has had its copies promoted by the rebalance and its note goes; a
// shard whose releaser is not alive by the log is handed to the shard's
// leader, and its copies dropped once that leader acknowledges them.
func (r *Replicator) judgeKeptRetained(cur shardmap.Map) {
	if cur.Version == 0 {
		return
	}
	r.rmoves.mu.Lock()
	notes := make(map[int]retainedNote, len(r.rmoves.kept))
	for s, n := range r.rmoves.kept {
		notes[s] = n
	}
	r.rmoves.mu.Unlock()
	shards := make([]int, 0, len(notes))
	for s := range notes {
		shards = append(shards, s)
	}
	sort.Ints(shards)
	for _, shard := range shards {
		note := notes[shard]
		if shard >= len(cur.Shards) {
			continue
		}
		if r.judgeGate != nil {
			r.judgeGate(shard)
		}
		// Every action below acts on this snapshot and on cur, so each checks
		// both still hold (stillJudged, claimJudged): a hand-over earlier in
		// this loop may have blocked while they changed.
		if !r.o.StaleKeptJudgement && !r.stillJudged(shard, note, cur) {
			continue
		}
		leader := cur.Shards[shard].Leader
		if note.HandOver {
			r.retryHandOver(cur, shard, note)
			continue
		}
		// The releaser's release may have come while this node still owed it a
		// hand-over; the hand-over is in, so the release is acted on now.
		if act, drop := releasedEarlier(r.receivedFrom(shard, note.Releaser), note.Version, leader == r.o.Node, cur.Holds(shard, r.o.Node)); act && !r.o.NoEarlyReleaseDrop {
			if r.claimJudged(shard, note, cur) && drop {
				n := r.dropKeptShard(shard, note.Own, "retained-released")
				r.log.Info("kept retained copies released", "shard", shard, "by", note.Releaser, "map", note.Version, "topics", n)
			}
			continue
		}
		leaderAlive := leader != "" && leader != r.o.Node && leader != note.Releaser && r.o.Leases.Live(leader)
		switch judgeKept(r.o.Leases.Live(note.Releaser), leader == r.o.Node, leaderAlive) {
		case promoteRetained:
			r.claimJudged(shard, note, cur)
		case handOnRetained:
			if r.o.KeepRetainedNotHandedOn {
				continue
			}
			msgs := r.follower.retainedIn(shard)
			if note.Own {
				msgs = append(msgs, r.ownRetainedIn(shard)...)
			}
			if len(msgs) == 0 {
				r.claimJudged(shard, note, cur)
				continue
			}
			if !r.handOverRetained(leader, msgs, cur.Version) {
				r.count("hoardmq_replicas_kept_total", map[string]string{"reason": "handover-unacknowledged"}, float64(len(msgs)))
				continue
			}
			if !r.claimJudged(shard, note, cur) {
				continue
			}
			if !cur.Holds(shard, r.o.Node) {
				// A node that follows the shard again keeps them: what it
				// holds is the leader's now, the handed-on copies included,
				// which the leader's tap sends back.
				r.dropKeptShard(shard, note.Own, "retained-handed-on")
			}
			r.log.Info("kept retained copies handed to the shard's leader; their releaser is gone", "shard", shard, "releaser", note.Releaser, "to", leader, "topics", len(msgs))
		}
	}
}

// retryHandOver makes again a hand-over the shard's leader under cur has not
// acknowledged: a former leader's of its own copies, which the leader's move
// waits for, or a follower's of its replica copies for a dead or absent
// leader. Once acknowledged, a former leader sends that leader its release;
// its copies then go when this node follows the shard, and are kept until
// the leader releases them when it does not hold it. A follower's copies stay
// a follower's when it holds the shard, and are kept for the release when it
// does not.
func (r *Replicator) retryHandOver(cur shardmap.Map, shard int, note retainedNote) {
	leader := cur.Shards[shard].Leader
	switch {
	case leader == r.o.Node:
		r.claimJudged(shard, note, cur)
		return
	case leader == "" || !r.o.Leases.Live(leader):
		return
	}
	if !note.Own {
		// A follower's copies, owed to a new leader: they stay a follower's
		// once the leader has them, and are kept for its release otherwise.
		held := r.follower.retainedIn(shard)
		if len(held) > 0 && !r.handOverRetained(leader, held, cur.Version) {
			return
		}
		if !r.claimJudged(shard, note, cur) {
			return
		}
		if !cur.Holds(shard, r.o.Node) && len(held) > 0 {
			r.keepRetained(shard, leader, cur.Version, false, false)
		}
		return
	}
	// A note owing a hand-over may also hold replica copies, kept from an
	// earlier move: every copy of the shard goes to the leader.
	mine := r.ownRetainedIn(shard)
	held := r.follower.retainedIn(shard)
	if all := append(append([]*message.Message(nil), mine...), held...); len(all) > 0 && !r.handOverRetained(leader, all, cur.Version) {
		return
	}
	if !r.claimJudged(shard, note, cur) {
		return
	}
	r.pushRelease(leader, cur.Version, shard)
	if cur.Holds(shard, r.o.Node) || len(mine)+len(held) == 0 {
		r.endOwnAfterHandOver(cur, shard, leader)
		return
	}
	r.keepRetained(shard, leader, cur.Version, true, false)
}

// retainedStep runs the moves this node leads and the shards it keeps against
// the map it last reconciled.
func (r *Replicator) retainedStep() {
	if r.o.PreRetainedMoves {
		return
	}
	r.recMu.Lock()
	cur := r.reconciled
	r.recMu.Unlock()
	r.advanceRetainedMoves(cur)
	r.judgeKeptRetained(cur)
}
