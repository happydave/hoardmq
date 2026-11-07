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

// kindRetainedTombstonesDropped tells a follower which retained tombstones
// of a shard its leader dropped: each named by topic and version, dropped
// there only while it is still that tombstone. A follower drops a tombstone
// on no other word. An older receiver acknowledges the kind unapplied and
// keeps its tombstones, which costs memory only.
const kindRetainedTombstonesDropped byte = 11

// maxTombstoneDropBatch bounds the entries one kind-11 frame names.
const maxTombstoneDropBatch = 1024

// droppedFile holds each shard's dropped-through version, in the replica
// store's directory: the highest version of a tombstone this node dropped or
// was told dropped. It serves a signal only, so a file that cannot be read is
// no versions.
const droppedFile = "retained-dropped.json"

// tombDrop is one dropped tombstone a kind-11 frame names.
type tombDrop struct {
	Namespace string        `json:"ns"`
	Topic     string        `json:"topic"`
	RV        store.Version `json:"rv"`
}

// tombState is what the leader tracks of one of its tombstones: when it first
// saw it, on its own monotonic clock, and the stream position each follower
// has to acknowledge to hold it.
type tombState struct {
	rv     store.Version
	seen   time.Time
	sentAt map[string]uint64
}

// tapped is the stream position the tap pushed a clear's record at, to each
// follower: a tombstone judged before its tap ran takes these once it has.
type tapped struct {
	rv   store.Version
	seqs map[string]uint64
	at   time.Time
}

// sentDrops is a kind-11 frame sent to a peer, kept until acknowledged.
type sentDrops struct {
	seq   uint64
	shard int
	drops []tombDrop
}

type tombstones struct {
	mu sync.Mutex
	// tracked is the leader's tombstones, by shard and namespace/topic.
	tracked map[int]map[[2]string]*tombState
	// tapped is each clear's tap, by namespace/topic, until it is judged.
	tapped map[[2]string]tapped
	// sent is each peer's kind-11 frames not yet acknowledged.
	sent map[string][]sentDrops
	// owed is each peer's kind-11 entries not known applied, by shard: those
	// it acknowledged unapplied and those whose frame a broken stream lost.
	// They are sent again with the next kind 11, a resync, or once the peer's
	// owed hand-over of the shard is in.
	owed map[string]map[int][]tombDrop
	// handIn is when each peer last handed this node a copy of a shard; once
	// that hand-over has gone quiet for the acknowledgement timeout, what the
	// peer is owed of the shard is sent again (judgeTombstones).
	handIn map[string]map[int]time.Time
	// dropped is each shard's dropped-through version.
	dropped map[int]store.Version
	// gauged is the namespaces the tombstone gauge last named, so one that
	// holds none any more is set to zero.
	gauged map[string]bool
	// fileMu orders the dropped-through file's writes, so a later write never
	// lands under an earlier one's older versions.
	fileMu sync.Mutex
}

func newTombstones() *tombstones {
	return &tombstones{tracked: map[int]map[[2]string]*tombState{}, tapped: map[[2]string]tapped{}, sent: map[string][]sentDrops{},
		owed: map[string]map[int][]tombDrop{}, handIn: map[string]map[int]time.Time{}, dropped: map[int]store.Version{}, gauged: map[string]bool{}}
}

// tombstoneGoes decides whether a leader drops a tombstone: its followers
// hold it, no move of its shard is pending and none of its old holders is
// passed over (so every old holder alive by the log has been released), and
// it has been held for at least hold. Time is one condition, never the only
// one. It is pure.
func tombstoneGoes(followersHold, movePending, passedOver bool, age, hold time.Duration) bool {
	return followersHold && !movePending && !passedOver && age >= hold
}

// kind11Applies decides whether a follower acts on a kind-11 frame: it comes
// from the shard's leader under the map this node applied, this node does not
// lead the shard, and this node owes the sender no hand-over of it
// (owesHandOver, as a release is judged). It is pure.
func kind11Applies(senderLeads, leadsNow, owesSender bool) bool {
	return senderLeads && !leadsNow && !owesSender
}

// countMerge counts a merge's outcome on one install path (store.MergeOutcome),
// with a debug line naming both versions where the copy was not installed.
func (r *Replicator) countMerge(path, topic string, installed bool, held, in store.Version) {
	outcome := store.MergeOutcome(installed, held, in)
	r.count("hoardmq_retained_merges_total", map[string]string{"path": path, "outcome": outcome}, 1)
	if !installed {
		r.log.Debug("retained copy not installed: the topic is held at a version as high", "path", path, "topic", topic, "rv", in.String(), "held_rv", held.String(), "outcome", outcome)
	}
}

// gaugeTombstones sets the tombstone gauge for each namespace, and to zero
// for each the gauge named before that now holds none.
func (r *Replicator) gaugeTombstones(byNS map[string]int) {
	t := r.tombs
	t.mu.Lock()
	gone := make([]string, 0, len(t.gauged))
	for ns := range t.gauged {
		if byNS[ns] == 0 {
			gone = append(gone, ns)
			delete(t.gauged, ns)
		}
	}
	for ns := range byNS {
		t.gauged[ns] = true
	}
	t.mu.Unlock()
	for _, ns := range gone {
		r.gaugeWith("hoardmq_retained_tombstones", map[string]string{"namespace": ns}, 0)
	}
	for ns, n := range byNS {
		r.gaugeWith("hoardmq_retained_tombstones", map[string]string{"namespace": ns}, float64(n))
	}
}

// tombstoneHold is how long a tombstone is held at least.
func (r *Replicator) tombstoneHold() time.Duration {
	if r.o.RetainedTombstoneHold > 0 {
		return r.o.RetainedTombstoneHold
	}
	return time.Hour
}

// judgeTombstones is the leader's pass over its tombstones, on the reconcile
// worker, against cur, the map the retained moves follow: each that meets the
// three conditions is dropped here, by a compare-and-delete, and named to
// every follower of its shard in kind-11 frames. It also sends again what a
// peer is owed once its hand-over has gone quiet, and forgets what is owed or
// sent for a shard this node no longer leads or a peer that no longer follows
// it. It reports whether tombstones or owed entries remain, for the worker's
// poll.
func (r *Replicator) judgeTombstones(cur shardmap.Map) bool {
	if r.o.Transport == nil || r.o.PreRetainedMoves || cur.Version == 0 {
		return false
	}
	now := time.Now()
	hold := r.tombstoneHold()
	t := r.tombs
	byShard := map[int][]store.RetainedEntry{}
	byNS := map[string]int{}
	for _, e := range r.o.Local.RetainedEntries() {
		if !e.Tombstone || e.RV.Zero() {
			continue
		}
		byNS[e.Msg.Namespace]++
		s := r.o.Map.ShardOf(e.Msg.Namespace, e.Msg.Topic)
		if s < len(cur.Shards) && cur.Shards[s].Leader == r.o.Node {
			byShard[s] = append(byShard[s], e)
		}
	}
	r.gaugeTombstones(byNS)
	r.rmoves.mu.Lock()
	moving := map[int]bool{}
	for s := range r.rmoves.pending {
		moving[s] = true
	}
	passed := map[int]bool{}
	for s, hs := range r.rmoves.passed {
		passed[s] = len(hs) > 0
	}
	r.rmoves.mu.Unlock()
	drops := map[int][]tombDrop{}
	t.mu.Lock()
	tracked := 0
	for s := range t.tracked {
		if _, ok := byShard[s]; !ok {
			delete(t.tracked, s)
		}
	}
	live := map[[2]string]bool{}
	for s, es := range byShard {
		cur0 := t.tracked[s]
		if cur0 == nil {
			cur0 = map[[2]string]*tombState{}
			t.tracked[s] = cur0
		}
		for _, e := range es {
			k := [2]string{e.Msg.Namespace, e.Msg.Topic}
			live[k] = true
			ts := cur0[k]
			if ts == nil || ts.rv != e.RV {
				ts = &tombState{rv: e.RV, seen: now, sentAt: map[string]uint64{}}
				cur0[k] = ts
			}
			if tp, ok := t.tapped[k]; ok && tp.rv == e.RV {
				// The tap's own position: a sample taken before the tap ran
				// is below the tombstone's frame.
				for f, seq := range tp.seqs {
					ts.sentAt[f] = max(ts.sentAt[f], seq)
				}
			}
			held := true
			for _, f := range cur.Shards[s].Followers {
				if !r.followerHolds(f, ts, e.Msg.Namespace, e.Msg.Topic) {
					held = false
				}
			}
			if tombstoneGoes(held, moving[s], passed[s], now.Sub(ts.seen), hold) {
				drops[s] = append(drops[s], tombDrop{Namespace: e.Msg.Namespace, Topic: e.Msg.Topic, RV: e.RV})
			}
		}
		for k := range cur0 {
			if !live[k] {
				delete(cur0, k)
			}
		}
		tracked += len(cur0)
	}
	for k, tp := range t.tapped {
		// A tap after this pass's read is kept for the next one.
		if !live[k] && tp.at.Before(now) {
			delete(t.tapped, k)
		}
	}
	follows := func(peer string, shard int) bool {
		return shard >= 0 && shard < len(cur.Shards) && cur.Shards[shard].Leader == r.o.Node && contains(cur.Shards[shard].Followers, peer)
	}
	for peer, byS := range t.owed {
		for s := range byS {
			if !follows(peer, s) {
				delete(byS, s)
			}
		}
		if len(byS) == 0 {
			delete(t.owed, peer)
		}
	}
	for peer, ss := range t.sent {
		kept := ss[:0]
		for _, sd := range ss {
			if follows(peer, sd.shard) {
				kept = append(kept, sd)
			}
		}
		if len(kept) == 0 {
			delete(t.sent, peer)
		} else {
			t.sent[peer] = kept
		}
	}
	quiet := map[string][]int{}
	for peer, byS := range t.handIn {
		for s, at := range byS {
			if !follows(peer, s) {
				delete(byS, s)
				continue
			}
			if now.Sub(at) >= r.o.AckTimeout {
				quiet[peer] = append(quiet[peer], s)
				delete(byS, s)
			}
		}
		if len(byS) == 0 {
			delete(t.handIn, peer)
		}
	}
	pending := tracked > 0 || len(t.owed) > 0 || len(t.handIn) > 0
	t.mu.Unlock()
	for s, ds := range drops {
		r.dropTombstones(cur.Shards[s].Followers, s, ds)
	}
	for peer, shards := range quiet {
		r.resendOwedDrops(peer, shards...)
	}
	return pending
}

// followerHolds reports whether follower f holds the tombstone ts tracks: it
// acknowledged its stream through ts's position for it, no send at or past
// that failed, and it has not refused the topic's record since. After a
// break the position is taken again once the stream's resync is queued,
// since the resync carries the tombstone.
func (r *Replicator) followerHolds(f string, ts *tombState, ns, topic string) bool {
	p := r.outboundFor(f)
	at, ok := ts.sentAt[f]
	if !ok {
		// Everything already pushed to this follower, its whole push or the
		// tombstone's tap among it, must be held.
		at = p.lastSent()
		ts.sentAt[f] = at
	}
	done, failed := p.delivered(at)
	if failed {
		if !p.holed() {
			ts.sentAt[f] = p.lastSent()
		}
		return false
	}
	return done && !p.refusedTopic(ns, topic)
}

// noteTapped records the stream position the tap pushed a clear's record at,
// for each target.
func (r *Replicator) noteTapped(ns, topic string, rv store.Version, seqs map[string]uint64) {
	t := r.tombs
	t.mu.Lock()
	t.tapped[[2]string{ns, topic}] = tapped{rv: rv, seqs: seqs, at: time.Now()}
	t.mu.Unlock()
}

// noteHandOverIn records that peer handed this node a copy of shard: what the
// peer is owed of it is sent again once its hand-over has gone quiet.
func (r *Replicator) noteHandOverIn(peer string, shard int) {
	t := r.tombs
	t.mu.Lock()
	if len(t.owed[peer][shard]) == 0 {
		t.mu.Unlock()
		return
	}
	if t.handIn[peer] == nil {
		t.handIn[peer] = map[int]time.Time{}
	}
	t.handIn[peer][shard] = time.Now()
	t.mu.Unlock()
	if !r.tombsTracked.Swap(true) {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}
}

// handOverReleased is a release of shard from peer at this node, its leader:
// the release follows a former leader's completed hand-over, so what the peer
// is owed of the shard goes again now.
func (r *Replicator) handOverReleased(peer string, shard int) {
	t := r.tombs
	t.mu.Lock()
	delete(t.handIn[peer], shard)
	owed := len(t.owed[peer][shard]) > 0
	t.mu.Unlock()
	if owed {
		r.resendOwedDrops(peer, shard)
	}
}

// tombstoneSendsBroken moves a peer's kind-11 frames at or below through,
// which its broken stream lost, to what it is owed: its cumulative
// acknowledgement after the resync would otherwise settle them as applied.
func (r *Replicator) tombstoneSendsBroken(peer string, through uint64) {
	t := r.tombs
	t.mu.Lock()
	defer t.mu.Unlock()
	kept := t.sent[peer][:0]
	for _, sd := range t.sent[peer] {
		if sd.seq > through {
			kept = append(kept, sd)
			continue
		}
		if t.owed[peer] == nil {
			t.owed[peer] = map[int][]tombDrop{}
		}
		t.owed[peer][sd.shard] = append(t.owed[peer][sd.shard], sd.drops...)
	}
	t.sent[peer] = kept
}

// dropTombstones drops a shard's tombstones here, each only while it is still
// that tombstone, and names the dropped ones to every follower of the shard.
func (r *Replicator) dropTombstones(followers []string, shard int, ds []tombDrop) {
	var done []tombDrop
	for _, d := range ds {
		if r.o.Sessions.DropRetainedTombstone(d.Namespace, d.Topic, d.RV) {
			done = append(done, d)
		}
	}
	if len(done) == 0 {
		return
	}
	r.count("hoardmq_retained_tombstones_dropped_total", nil, float64(len(done)))
	r.noteDropped(shard, done)
	for _, f := range followers {
		r.sendTombstoneDrops(f, shard, done)
	}
}

// sendTombstoneDrops queues kind-11 frames naming ds, with any entries the
// peer acknowledged unapplied before, at most maxTombstoneDropBatch a frame,
// under the push lock, so a resync read before the drop cannot land after it.
func (r *Replicator) sendTombstoneDrops(peer string, shard int, ds []tombDrop) {
	t := r.tombs
	t.mu.Lock()
	all := append(append([]tombDrop(nil), t.owed[peer][shard]...), ds...)
	delete(t.owed[peer], shard)
	t.mu.Unlock()
	if len(all) == 0 {
		return
	}
	version := r.o.Map.Current().Version
	r.pushMu.Lock()
	defer r.pushMu.Unlock()
	for len(all) > 0 {
		n := min(len(all), maxTombstoneDropBatch)
		batch := all[:n]
		all = all[n:]
		seq := r.outboundFor(peer).push(frame{Kind: kindRetainedTombstonesDropped, Shards: []int{shard}, Epoch: version, Drops: batch})
		if seq == 0 {
			continue
		}
		t.mu.Lock()
		t.sent[peer] = append(t.sent[peer], sentDrops{seq: seq, shard: shard, drops: batch})
		t.mu.Unlock()
	}
}

// tombstoneDropsAcked handles a peer's acknowledgement through seq: its
// kind-11 frames at or below it are done, except one it acknowledged
// unapplied, whose entries are owed again.
func (r *Replicator) tombstoneDropsAcked(peer string, seq uint64, unapplied bool) {
	t := r.tombs
	t.mu.Lock()
	defer t.mu.Unlock()
	kept := t.sent[peer][:0]
	for _, s := range t.sent[peer] {
		switch {
		case s.seq > seq:
			kept = append(kept, s)
		case unapplied && s.seq == seq:
			if t.owed[peer] == nil {
				t.owed[peer] = map[int][]tombDrop{}
			}
			t.owed[peer][s.shard] = append(t.owed[peer][s.shard], s.drops...)
		}
	}
	t.sent[peer] = kept
}

// resendOwedDrops sends a peer every kind-11 entry it is owed, of the shards
// named or of every shard: at a resync of it, and once its owed hand-over of a
// shard is in.
func (r *Replicator) resendOwedDrops(peer string, shards ...int) {
	t := r.tombs
	t.mu.Lock()
	var todo []int
	for s := range t.owed[peer] {
		if len(shards) == 0 || containsInt(shards, s) {
			todo = append(todo, s)
		}
	}
	t.mu.Unlock()
	sort.Ints(todo)
	for _, s := range todo {
		r.sendTombstoneDrops(peer, s, nil)
	}
}

func containsInt(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// applyTombstoneDrops is a follower's apply of a kind-11 frame. It reports
// whether it applied the frame; unapplied, the sender sends its entries again.
func (w *worker) applyTombstoneDrops(fr frame) (applied bool) {
	r := w.f.r
	if len(fr.Shards) != 1 {
		return false
	}
	shard := fr.Shards[0]
	m := r.o.Map.Current()
	if shard >= len(m.Shards) {
		return false
	}
	owes, _ := r.owesHandOverTo(w.peer, shard, fr.Epoch)
	if !kind11Applies(m.Shards[shard].Leader == w.peer, m.Shards[shard].Leader == r.o.Node, owes) {
		r.count("hoardmq_retained_tombstone_drops_total", map[string]string{"outcome": "unapplied"}, 1)
		return false
	}
	// An entry is acted on only for the shard the frame names, whose leader
	// the fence checked.
	ds := make([]tombDrop, 0, len(fr.Drops))
	for _, d := range fr.Drops {
		if r.o.Map.ShardOf(d.Namespace, d.Topic) == shard {
			ds = append(ds, d)
		}
	}
	n := w.f.dropTombstones(ds)
	r.noteDropped(shard, ds)
	r.count("hoardmq_retained_tombstone_drops_total", map[string]string{"outcome": "applied"}, 1)
	if n > 0 {
		r.count("hoardmq_retained_tombstones_dropped_total", nil, float64(n))
	}
	return true
}

// dropTombstones drops each named tombstone from the replica store while it
// is still that tombstone, journaling the drop under the same lock as every
// other replica record, so a later record for the topic follows it.
func (f *follower) dropTombstones(ds []tombDrop) int {
	f.mu.Lock()
	defer f.unlock(holdApplyOther, time.Now())
	n := 0
	for _, d := range ds {
		if _, held, tomb := f.state.RetainedHeld(d.Namespace, d.Topic); !held || !tomb {
			continue
		}
		if !f.state.DropTombstoneIf(d.Namespace, d.Topic, d.RV) {
			continue
		}
		rec := store.RetainRecord(&message.Message{Namespace: d.Namespace, Topic: d.Topic})
		if err := f.r.o.Replica.AppendRaw(rec.Type, rec.Payload); err != nil {
			f.r.log.Error("replica tombstone drop not journaled; a restart brings it back", "topic", d.Topic, "err", err)
		}
		n++
	}
	return n
}

// noteDropped raises a shard's dropped-through version to the highest of ds,
// written durably when the replica directory is known.
func (r *Replicator) noteDropped(shard int, ds []tombDrop) {
	t := r.tombs
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	t.mu.Lock()
	high := t.dropped[shard]
	for _, d := range ds {
		if high.Less(d.RV) {
			high = d.RV
		}
	}
	changed := high != t.dropped[shard]
	t.dropped[shard] = high
	var data []byte
	if changed && r.o.ReplicaDir != "" {
		byShard := make(map[string]store.Version, len(t.dropped))
		for s, v := range t.dropped {
			byShard[strconv.Itoa(s)] = v
		}
		data, _ = json.Marshal(byShard)
	}
	t.mu.Unlock()
	if data != nil {
		if err := writeDurably(filepath.Join(r.o.ReplicaDir, droppedFile), data); err != nil {
			r.log.Warn("dropped-through versions not written", "err", err)
		}
	}
}

// readDropped reads the dropped-through versions a previous incarnation
// wrote; a file that cannot be read is none.
func (r *Replicator) readDropped() {
	if r.o.ReplicaDir == "" {
		return
	}
	data, err := os.ReadFile(filepath.Join(r.o.ReplicaDir, droppedFile))
	if err != nil {
		return
	}
	var byShard map[string]store.Version
	if json.Unmarshal(data, &byShard) != nil {
		return
	}
	r.tombs.mu.Lock()
	for k, v := range byShard {
		if s, err := strconv.Atoi(k); err == nil {
			r.tombs.dropped[s] = v
		}
	}
	r.tombs.mu.Unlock()
}

// belowDropped counts an install into an empty topic of the shard at or below
// its dropped-through version: a copy older than a tombstone already dropped
// may be one that tombstone was kept to outrank. It is a signal: a message
// set long ago and never cleared counts too. wasEmpty is whether nothing,
// message or tombstone, was held for the topic before the install.
func (r *Replicator) belowDropped(shard int, path, peer string, m *message.Message, v store.Version, wasEmpty bool) {
	if !wasEmpty {
		return
	}
	r.tombs.mu.Lock()
	x, ok := r.tombs.dropped[shard]
	r.tombs.mu.Unlock()
	if !ok || x.Less(v) {
		return
	}
	r.count("hoardmq_retained_merges_total", map[string]string{"path": path, "outcome": "below-dropped"}, 1)
	r.log.Warn("retained copy installed at or below the dropped-through version", "topic", m.Topic, "rv", v.String(), "dropped_through", x.String(), "from", peer, "path", path)
}

// DropEligibleTombstones drops every tombstone this node leads that meets the
// three conditions, at once, and reports how many of namespace ns's went: the
// retained-topic limit asks before it refuses a clear in ns. The pass is the
// reconcile worker's, so it drops other namespaces' eligible tombstones too.
func (r *Replicator) DropEligibleTombstones(ns string) int {
	before := 0
	for _, e := range r.o.Local.RetainedEntries() {
		if e.Tombstone && e.Msg.Namespace == ns {
			before++
		}
	}
	r.recMu.Lock()
	cur := r.reconciled
	r.recMu.Unlock()
	r.judgeTombstones(cur)
	after := 0
	for _, e := range r.o.Local.RetainedEntries() {
		if e.Tombstone && e.Msg.Namespace == ns {
			after++
		}
	}
	return before - after
}
