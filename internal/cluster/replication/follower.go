package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// follower is the receiving side: replicas of other nodes' sessions and
// retained messages, persisted in the replica store and materialised in
// memory. Frames from each peer are applied in order by one worker per
// peer; acknowledgements are sent after the replica store is synced.
type follower struct {
	r *Replicator

	mu    sync.Mutex
	state *store.State

	// afterAppend, set only by tests, runs in apply between the record's append
	// and its apply.
	afterAppend func()

	// lockHolds times what holds mu.
	lockHolds lockHoldCounters

	wmu     sync.Mutex
	workers map[string]*worker
	// closed stops enqueue creating a worker once Stop is under way; see
	// enqueue for why a late frame must not start one.
	closed bool
}

func newFollower(r *Replicator) (*follower, error) {
	st, err := r.o.Replica.Recover()
	if err != nil {
		return nil, fmt.Errorf("replica store: %w", err)
	}
	f := &follower{r: r, state: st, workers: map[string]*worker{}}
	r.gauge("hoardmq_replicas_held", float64(len(st.Sessions)))
	return f, nil
}

// snapshot copies the replica state; kind names the holder for the lock's
// trace.
func (f *follower) snapshot(kind int) *store.State {
	f.mu.Lock()
	defer f.unlock(kind, time.Now())
	data, _ := json.Marshal(f.state)
	out := store.NewState()
	_ = json.Unmarshal(data, out)
	return out
}

// checkpointState is the snapshot a checkpoint of the replica store writes. A
// record is appended and applied under f.mu, so the state read under it holds
// every record appended so far and no other: it is exact at the store's last
// appended index read here (WI 1818).
func (f *follower) checkpointState(appended func() uint64) *store.State {
	f.mu.Lock()
	defer f.unlock(holdCheckpoint, time.Now())
	data, _ := json.Marshal(f.state)
	out := store.NewState()
	_ = json.Unmarshal(data, out)
	out.At = appended()
	return out
}

// find returns a copy of one replica session, if held.
func (f *follower) find(ns, id string) (*store.SessionState, bool) {
	c := &f.r.fetches
	asked := time.Now()
	f.mu.Lock()
	held := time.Now()
	defer func() {
		f.unlock(holdFind, held)
		c.finds.Add(1)
		c.findLockNs.Add(int64(held.Sub(asked)))
		c.findHoldNs.Add(since(held))
	}()
	ss := f.state.Find(ns, id)
	if ss == nil {
		return nil, false
	}
	data, _ := json.Marshal(ss)
	var out store.SessionState
	_ = json.Unmarshal(data, &out)
	return &out, true
}

// apply persists and materialises one record, both under f.mu, so a
// checkpoint's snapshot never reads a state behind the store (checkpointState).
func (f *follower) apply(rec store.Record) (store.Unapplied, error) {
	return f.applyTimed(rec, nil, holdApplyOther)
}

// applyTimed is apply, adding its parts to a sampled batch's timing when t is
// not nil; kind names the holder for the lock's trace.
func (f *follower) applyTimed(rec store.Record, t *batchTiming, kind int) (store.Unapplied, error) {
	var asked, appended time.Time
	if t != nil {
		asked = time.Now()
	}
	f.mu.Lock()
	held := time.Now()
	defer f.unlock(kind, held)
	if t != nil {
		t.s.LockWaitNs += held.Sub(asked).Nanoseconds()
	}
	if err := f.r.o.Replica.AppendRaw(rec.Type, rec.Payload); err != nil {
		return "", err
	}
	if t != nil {
		appended = time.Now()
		t.s.AppendNs += appended.Sub(held).Nanoseconds()
	}
	if f.afterAppend != nil {
		f.afterAppend()
	}
	un, err := f.state.ApplyReport(rec.Type, rec.Payload)
	if t != nil {
		t.s.ApplyNs += time.Since(appended).Nanoseconds()
	}
	if err != nil {
		return "", err
	}
	f.r.gauge("hoardmq_replicas_held", float64(len(f.state.Sessions)))
	return un, nil
}

// holds reports whether a replica copy of the session is held, without
// copying it.
func (f *follower) holds(ns, id string) bool {
	f.mu.Lock()
	defer f.unlock(holdFind, time.Now())
	return f.state.Find(ns, id) != nil
}

// retainedIn lists the replica retained copies of a shard, each a copy.
func (f *follower) retainedIn(shard int) []*message.Message {
	f.mu.Lock()
	defer f.unlock(holdFind, time.Now())
	var out []*message.Message
	for _, m := range f.state.Retained {
		if f.r.o.Map.ShardOf(m.Namespace, m.Topic) == shard {
			c := *m
			out = append(out, &c)
		}
	}
	return out
}

// copyEpoch is the epoch of the replica copy held for the session, if any.
// It is the read a drop decides on: find copies the whole session through
// JSON, which is more than a comparison needs.
func (f *follower) copyEpoch(ns, id string) (uint64, bool) {
	f.mu.Lock()
	defer f.unlock(holdFind, time.Now())
	ss := f.state.Find(ns, id)
	if ss == nil {
		return 0, false
	}
	return ss.Epoch, true
}

// dropCopy removes this node's replica copy of a session it must not hold:
// one of a session this node owns, which is older than the session it serves
// (WI 1293), or one of a session this node is not a target for, which
// receives nothing and would hand a takeover a queue from before the map
// moved (WI 1569). Either would replace a current copy with a stale one.
func (f *follower) dropCopy(ns, id string, reason string) bool {
	if !f.holds(ns, id) {
		return false
	}
	if _, err := f.apply(store.DeleteRecord(ns, id)); err != nil {
		f.r.log.Error("replica copy not dropped", "client", id, "err", err)
		return false
	}
	f.r.count("hoardmq_replicas_dropped_total", map[string]string{"reason": reason}, 1)
	f.r.log.Debug("replica copy dropped", "client", id, "reason", reason)
	return true
}

// unlock releases mu, recording a hold of kind that began at held.
func (f *follower) unlock(kind int, held time.Time) {
	f.mu.Unlock()
	f.lockHolds.held(kind, held)
}

// sync makes the applied records durable.
func (f *follower) sync() error { return f.r.o.Replica.Sync() }

// syncFailures is how many fsyncs of the replica store have failed.
func (f *follower) syncFailures() int64 { return f.r.o.Replica.SyncFailures() }

// syncedSince syncs the replica store and reports why what was written after
// the failure count read since may not be on the disk: "failed" when this sync
// failed, "covered" when another fsync of the store failed meanwhile, "" when
// it is durable.
func (f *follower) syncedSince(since int64) string {
	if err := f.sync(); err != nil {
		f.r.log.Error("replica sync failed", "err", err)
		return "failed"
	}
	if f.syncFailures() != since {
		return "covered"
	}
	return ""
}

// stale reports whether a session record from peer at epoch is behind this
// node's lease table: the table knows a higher epoch, or the same epoch
// with a different owner. An unknown or lower epoch in the table is the
// table lagging the owner and is accepted.
func (f *follower) stale(peer, ns, id string, epoch uint64) (bool, string) {
	if f.r.o.SkipEpochCheck {
		return false, ""
	}
	l, ok := f.r.o.Leases.Lookup(ns, id)
	// The highest grant token seen for the client, from any source. The
	// table's view lags: this node may have been told its own acquire
	// succeeded, or heard from the new owner, before its table applied the
	// grant, and the old owner's records arriving in that gap carry a lower
	// token however current the table makes them look.
	var seen uint64
	if f.r.o.Tokens != nil {
		seen = f.r.o.Tokens.Seen(ns, id)
	}
	if epoch != 0 && seen > epoch && (!ok || seen > l.Epoch) {
		f.r.count("hoardmq_lease_stale_token_total", map[string]string{"where": "replication"}, 1)
		return true, fmt.Sprintf("epoch %d is behind %d, the highest grant seen for the client", epoch, seen)
	}
	if !ok {
		f.noteToken(ns, id, epoch)
		return false, ""
	}
	if epoch == 0 {
		// A record written before its owner held a generation for the client
		// carries none, and "older than the table's" is not a judgement that
		// can be made about it. What can be judged is where it came from: the
		// node the table names as owner holds the newest state there is, so
		// refusing its record costs the client a publish and repairs nothing
		// (WI 1570).
		if l.Owner != peer {
			return true, fmt.Sprintf("no epoch, and %s is not the owner", peer)
		}
		return false, ""
	}
	if l.Epoch > epoch {
		return true, fmt.Sprintf("epoch %d is behind lease epoch %d", epoch, l.Epoch)
	}
	if l.Epoch == epoch && l.Owner != peer {
		return true, fmt.Sprintf("epoch %d is owned by %s, not %s", epoch, l.Owner, peer)
	}
	f.noteToken(ns, id, epoch)
	return false, ""
}

// noteToken records the epoch of a record that was accepted.
func (f *follower) noteToken(ns, id string, epoch uint64) {
	if f.r.o.Tokens != nil {
		f.r.o.Tokens.Note(ns, id, epoch)
	}
}

// handedOver reports whether a session snapshot from a node that is not the
// session's owner may be applied: the epoch is the table's current one — the
// copy is the owner's state as the holder replicated it — and this node
// holds nothing for the session yet. A holder that stops holding a copy on a
// map change hands it to the new holders this way (WI 1289). The owner's own
// push, if it arrives, replaces the copy whole; if it was lost, the gap in
// the owner's stream resyncs it.
func (f *follower) handedOver(peer, ns, id string, epoch uint64) bool {
	l, ok := f.r.o.Leases.Lookup(ns, id)
	if !ok || l.Epoch != epoch || l.Owner == peer || l.Owner == f.r.o.Node {
		// From the owner it is a takeover write, judged by stale; and the
		// owner itself takes no copy of a session it holds.
		return false
	}
	if _, held := f.find(ns, id); held {
		return false
	}
	if f.r.o.KeepUnfedCopies || !f.r.ownerAlive(ns, id) {
		// With the owner gone the copy being handed over may be the last one,
		// and no target is being fed either (WI 1289).
		return true
	}
	// Otherwise the copy is taken only where the owner's records will reach
	// it: a holder this session is not replicated to receives nothing after
	// this, and a takeover that read it would hand the client a queue from
	// before the map moved (WI 1569).
	shard := f.r.o.Map.ShardOf(ns, id)
	return contains(f.r.o.Map.Current().Targets(shard, l.Owner), f.r.o.Node)
}

func (f *follower) enqueue(peer string, fr frame) {
	f.wmu.Lock()
	if f.closed {
		// A frame that arrived after the replicator stopped. Starting a
		// worker for it adds to the wait group Stop is already waiting on,
		// which is both a race on the counter and a goroutine outliving the
		// replicator that owns it. There is nothing useful to do with the
		// frame: the peer's barrier over it fails, as it does for a peer
		// that went down.
		f.wmu.Unlock()
		return
	}
	w := f.workers[peer]
	if w == nil {
		w = newWorker(f, peer)
		f.workers[peer] = w
	}
	f.wmu.Unlock()
	w.push(fr)
}

func (f *follower) close() {
	f.wmu.Lock()
	f.closed = true
	workers := make([]*worker, 0, len(f.workers))
	for _, w := range f.workers {
		workers = append(workers, w)
	}
	f.wmu.Unlock()
	for _, w := range workers {
		w.close()
	}
}

// checkSequence reports a gap in the peer's stream before the frame that
// revealed it is acknowledged. Acknowledgements are cumulative, so the gap
// must be rejected first: the frames that never arrived changed nothing here
// and the owner's barriers over them must fail, and its stream be resynced,
// rather than pass on this frame's acknowledgement. A sequence below the
// expected one is the peer restarting its count and is accepted.
func (w *worker) checkSequence(fr frame) {
	if fr.Seq == 0 {
		return
	}
	// A stream starts at 1, so a first frame above it is a gap too: the
	// owner's first push to a new holder is the copy it is meant to hold. A
	// worker that starts on a stream already running (this node restarted)
	// reports a gap it cannot judge, and the resync that follows is what a
	// restarted follower needs anyway.
	expected := w.next
	if expected == 0 {
		expected = 1
	}
	if fr.Seq > expected && !w.f.r.o.IgnoreGaps {
		w.f.r.count("hoardmq_replication_gaps_total", map[string]string{"peer": w.peer}, 1)
		w.f.r.log.Warn("replication frames missing; reported to the owner", "peer", w.peer, "expected", w.next, "got", fr.Seq)
		w.refuse(fr, fmt.Sprintf("%s: expected %d, got %d", gapReason, expected, fr.Seq))
		w.holed = true
	}
	if fr.Seq < expected {
		w.gate.restarted()
	}
	w.next = fr.Seq + 1
}

// gapReason begins the rejection reason a follower gives for a sequence gap;
// the owner resyncs the stream on it rather than scoping a refusal.
const gapReason = "sequence gap"

// syncFailedReason begins the rejection a follower sends when writes of the
// stream may not be on its disk. Every owner reads the prefix as a gap: it
// breaks the stream, so nothing pushed so far passes a barrier, and resyncs it.
const syncFailedReason = gapReason + ": replica sync failed"

// worker applies one peer's frames in order and acknowledges them, syncing
// the replica store once per drained batch.
type worker struct {
	f    *follower
	peer string
	// next is the sequence the peer's stream should carry next; 0 before the
	// first sequenced frame. A frame above it means frames were lost with
	// the link up; one below it is the peer restarting its count.
	next uint64
	// holed is set when this worker reported a gap and cleared by the resync
	// mark that follows: between the two the peer's copies are being
	// replaced whole, so a record that changes nothing is expected.
	holed bool

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []frame
	closed bool

	// settles counts store syncs; touched only by the worker's goroutine.
	settles int64
	// gate withholds acknowledgements while a write of this stream may not
	// be on the disk.
	gate ackGate
	// trace is what this worker applied and refused, for the failover
	// measurement.
	trace inboundTrace
	// timing is the batch being applied when it is long enough to sample;
	// nil otherwise.
	timing *batchTiming
}

func newWorker(f *follower, peer string) *worker {
	w := &worker{f: f, peer: peer}
	w.cond = sync.NewCond(&w.mu)
	f.r.wg.Add(1)
	go w.loop()
	return w
}

func (w *worker) push(fr frame) {
	w.mu.Lock()
	if !w.closed {
		w.queue = append(w.queue, fr)
	}
	w.mu.Unlock()
	w.cond.Signal()
}

func (w *worker) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cond.Broadcast()
}

func (w *worker) loop() {
	defer w.f.r.wg.Done()
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && !w.closed {
			w.cond.Wait()
		}
		if w.closed {
			w.mu.Unlock()
			return
		}
		batch := w.queue
		w.queue = nil
		w.mu.Unlock()
		w.process(batch)
	}
}

// process applies a batch: records and snapshots are applied in order and
// acknowledged after one sync; requests are answered as met.
func (w *worker) process(batch []frame) {
	r := w.f.r
	began := time.Now()
	if len(batch) >= batchSampleMin {
		w.timing = newBatchTiming(w.peer, batch, began)
	}
	defer func() {
		end := time.Now()
		r.batches.note(w.peer, batch, end.Sub(began))
		if w.timing != nil {
			r.batches.keepBatch(w.timing.sample(end))
			w.timing = nil
		}
	}()
	for _, fr := range batch {
		if fr.Kind == kindFetch {
			settled := w.settles
			defer func() {
				storeMax(&r.fetches.maxBatch, int64(len(batch)))
				r.fetches.settlesBeside.Add(w.settles - settled)
			}()
			break
		}
	}
	var highest uint64
	dirty := false
	since := w.f.syncFailures()
	for _, fr := range batch {
		var frameBegan time.Time
		var syncedBefore int64
		if w.timing != nil {
			frameBegan, syncedBefore = time.Now(), w.timing.s.SyncNs
		}
		w.checkSequence(fr)
		if reason, above := w.aboveBuild(fr); above {
			w.refuse(fr, reason)
			highest = fr.Seq
			r.count("hoardmq_replication_records_total", map[string]string{"direction": "in"}, 1)
			continue
		}
		switch fr.Kind {
		case kindAppend:
			if rejected, reason := w.applyAppend(fr); rejected {
				w.refuse(fr, reason)
			} else {
				dirty = true
			}
			highest = fr.Seq
		case kindSnapshot:
			if rejected, reason := w.applySnapshot(fr); rejected {
				w.refuse(fr, reason)
			} else {
				dirty = true
			}
			highest = fr.Seq
		case kindRetainedWrite:
			// Sync what came before so the acknowledgement order holds.
			if dirty {
				w.settle(highest, true, since)
				dirty = false
				since = w.f.syncFailures()
			}
			w.applyRetainedWrite(fr)
			highest = fr.Seq
		case kindRetainedRelease:
			if r.o.PreRetainedMoves {
				// The older build's arm: acknowledged unapplied, as below.
				highest = fr.Seq
				break
			}
			if r.applyRelease(w.peer, fr) {
				dirty = true
			}
			highest = fr.Seq
		case kindResyncMark:
			// Nothing to apply: it exists to be acknowledged after a resync,
			// and it closes the window a reported gap opened.
			w.holed = false
			w.gate.saw(fr.FailedBelow)
			highest = fr.Seq
		case kindRetainedPresence:
			// Memory only, so no store sync is owed; advancing highest is
			// what puts it inside the acknowledgement the sender's barrier
			// waits for.
			r.applyPresence(w.peer, fr.Shards)
			highest = fr.Seq
		case kindFetch:
			w.f.answerFetch(w.peer, fr)
		case kindRetainedQuery:
			w.answerQuery(fr)
		default:
			// A kind from a newer build. It is acknowledged rather than
			// passed over: the stream's acknowledgement is cumulative, so
			// leaving this sequence unacknowledged wedges every barrier the
			// sender holds over it until the timeout — a rolling upgrade
			// would fail the new nodes' publishes against the old ones.
			//
			// The rule this establishes for any kind added later: it must be
			// safe for a peer that ignores it. Retained presence is, because
			// a peer that ignores it never announces, and a peer that never
			// announces is never skipped.
			r.log.Warn("replication frame of unknown kind acknowledged unapplied", "peer", w.peer, "kind", fr.Kind)
			r.count("hoardmq_replication_records_total", map[string]string{"direction": "in", "outcome": "unknown-kind"}, 1)
			highest = fr.Seq
		}
		r.count("hoardmq_replication_records_total", map[string]string{"direction": "in"}, 1)
		if t := w.timing; t != nil {
			now := time.Now()
			t.s.NsByKind[kindLabel(kindIndex(fr.Kind))] += now.Sub(frameBegan).Nanoseconds() - (t.s.SyncNs - syncedBefore)
			switch fr.Kind {
			case kindFetch, kindRetainedQuery, kindRetainedWrite:
				// Answered, or acknowledged on their own, not by the settle.
			default:
				t.frameDone(now)
			}
		}
	}
	if highest > 0 {
		w.settle(highest, dirty, since)
	}
	w.trace.applied(len(batch))
}

// refuse rejects one frame to its sender and records it in the trace.
func (w *worker) refuse(fr frame, reason string) {
	w.sendAck(rejection(fr, reason))
	s := RejectSample{Seq: fr.Seq, Kind: kindLabel(kindIndex(fr.Kind)), Client: fr.ClientID, Topic: fr.Topic,
		Epoch: fr.Epoch, Class: rejectClass(reason), Reason: reason}
	if fr.Kind == kindAppend {
		s.Type = store.RecordName(fr.Type)
	}
	if len(s.Reason) > 160 {
		s.Reason = s.Reason[:160]
	}
	r := w.f.r
	if fr.ClientID != "" && w.trace.wantsLease() {
		if l, ok := r.o.Leases.Lookup(fr.Namespace, fr.ClientID); ok {
			s.LeaseOwner, s.LeaseEpoch = l.Owner, l.Epoch
		}
		if r.o.Tokens != nil {
			s.Seen = r.o.Tokens.Seen(fr.Namespace, fr.ClientID)
		}
	}
	w.trace.refusedFrame(s)
}

// levelReason begins the rejection reason for a frame whose semantics this
// build does not understand.
const levelReason = "feature level"

// aboveBuild refuses a frame that changes what this node holds when its
// sender stamped a feature level this build does not understand: applying it
// with this build's semantics is how two copies of one session come to
// differ. The owner treats it as any refusal — the key is not held here.
func (w *worker) aboveBuild(fr frame) (string, bool) {
	switch fr.Kind {
	case kindAppend, kindSnapshot, kindRetainedWrite:
	default:
		return "", false
	}
	r := w.f.r
	build := r.buildLevel()
	if fr.Level <= build {
		return "", false
	}
	r.count("hoardmq_cluster_feature_level_refusals_total", map[string]string{"where": "frame"}, 1)
	r.log.Warn("replication frame refused: its feature level is above this build's", "peer", w.peer, "frame", fr.Level, "build", build, "client", fr.ClientID, "topic", fr.Topic)
	return fmt.Sprintf("%s %d is above this build's %d", levelReason, fr.Level, build), true
}

// settle ends the part of a batch up to seq: it syncs what the part wrote
// (dirty), with since the store's failure count when the part began, and
// acknowledges seq unless a write of this stream up to it may not be on the
// disk.
func (w *worker) settle(seq uint64, dirty bool, since int64) {
	r := w.f.r
	if dirty {
		w.settles++
		var syncing time.Time
		if w.timing != nil {
			syncing = time.Now()
		}
		cause := w.f.syncedSince(since)
		if w.timing != nil {
			w.timing.s.SyncNs += time.Since(syncing).Nanoseconds()
		}
		if cause != "" {
			w.gate.failed(seq, time.Now())
			r.count("hoardmq_replication_unsynced_batches_total", map[string]string{"peer": w.peer, "cause": cause}, 1)
			r.log.Error("replica writes may not be on the disk; acknowledging nothing until the sender resyncs", "peer", w.peer, "through", w.gate.lost, "cause", cause)
			w.rejectLost()
			return
		}
	}
	lost := w.gate.lost
	acknowledge, reject := w.gate.settled(time.Now(), r.o.AckTimeout)
	if reject {
		w.rejectLost()
	}
	if acknowledge {
		if lost > 0 {
			r.log.Info("replica stream resynced after a failed sync; acknowledging again", "peer", w.peer, "lost", lost, "seq", seq)
		}
		if w.timing != nil {
			w.timing.acknowledged(time.Now())
		}
		w.sendAck(ack{Seq: seq})
	}
}

// rejectLost tells the sender that this stream's writes through the gate's
// lost mark may not be on the disk, as a gap: its break fails every frame it
// has pushed, and its resync replaces them.
func (w *worker) rejectLost() {
	w.refuse(frame{Seq: w.gate.lost}, fmt.Sprintf("%s: frames through %d may not be on this node's disk", syncFailedReason, w.gate.lost))
}

// ackGate decides whether a worker may send a cumulative acknowledgement. A
// failed fsync can drop pages that a later successful one never writes again,
// so once a part of a batch was not durable, nothing is acknowledged until the
// sender shows that every frame through it has failed there, and the frames
// after it have synced. The sender shows it on a resync mark (FailedBelow).
type ackGate struct {
	// lost is the highest sequence whose writes may not be on the disk; zero
	// when every write is durable.
	lost uint64
	// marks is what the resync marks seen since the last settle carried.
	marks markSeen
	// rejectedAt is when the sender was last told.
	rejectedAt time.Time
}

// markSeen is what resync marks carried: the highest FailedBelow, whether any
// carried one, and whether any came from a sender that does not send it.
type markSeen struct {
	below uint64
	named bool
	older bool
}

// saw records a resync mark.
func (g *ackGate) saw(failedBelow uint64) {
	if failedBelow == 0 {
		g.marks.older = true
		return
	}
	g.marks.named = true
	if failedBelow > g.marks.below {
		g.marks.below = failedBelow
	}
}

// failed records that the writes of a part through seq may not be on the
// disk; the caller tells the sender. A mark seen before it proves nothing
// about it.
func (g *ackGate) failed(seq uint64, now time.Time) {
	if seq > g.lost {
		g.lost = seq
	}
	g.marks = markSeen{}
	g.rejectedAt = now
}

// restarted is the sender starting its count again: its barriers cover only
// the new stream's frames, so the old stream's lost ones hold nothing back.
func (g *ackGate) restarted() {
	g.lost = 0
	g.marks = markSeen{}
}

// settled is a settle whose part is durable. It acknowledges when nothing is
// lost, or when a mark since the last settle shows the sender has failed every
// frame through lost, or comes from an older sender, which cannot show it.
// Otherwise a mark showing the lost frames not failed is the sender asking
// without having read the rejection: tell it again, at most once an interval,
// so a resync under way is not broken by every stale probe.
func (g *ackGate) settled(now time.Time, interval time.Duration) (acknowledge, reject bool) {
	m := g.marks
	g.marks = markSeen{}
	if g.lost == 0 {
		return true, false
	}
	if m.below > g.lost || m.older {
		g.lost = 0
		return true, false
	}
	if m.named && now.Sub(g.rejectedAt) >= interval {
		g.rejectedAt = now
		return false, true
	}
	return false, false
}

// open reports whether an acknowledgement outside a settle may be sent.
func (g *ackGate) open() bool { return g.lost == 0 }

func (w *worker) sendAck(a ack) {
	body, _ := json.Marshal(a)
	ctx, cancel := context.WithTimeout(w.f.r.ctx, time.Second)
	defer cancel()
	if err := w.f.r.o.Transport.Send(ctx, w.peer, transport.ReplicationAck, body); err != nil {
		w.f.r.log.Debug("replication ack not sent", "peer", w.peer, "err", err)
	}
}

func (w *worker) applyAppend(fr frame) (rejected bool, reason string) {
	f := w.f
	if store.IsRetained(fr.Type) {
		if v := f.r.o.Map.Version(); fr.Epoch < v {
			return true, fmt.Sprintf("map version %d is behind %d", fr.Epoch, v)
		}
	} else if stale, _, why := w.staleTimed(fr, false); stale {
		return true, why
	}
	un, err := f.applyTimed(store.Record{Type: fr.Type, Key: store.Key{Namespace: fr.Namespace, ClientID: fr.ClientID, Topic: fr.Topic}, Payload: fr.Payload}, w.timing, holdApply)
	if err != nil {
		f.r.log.Error("replica apply failed", "peer", w.peer, "err", err)
		return true, err.Error()
	}
	if un != "" {
		reason := string(un)
		if w.holed {
			// The stream reported a gap and the resync has not marked its
			// end: the record arrived out of order or for a copy about to
			// be replaced, which the resync repairs.
			reason = "holed"
		}
		f.r.count("hoardmq_replication_unapplied_total", map[string]string{"reason": reason, "type": store.RecordName(fr.Type)}, 1)
		f.r.log.Debug("replicated record changed nothing", "peer", w.peer, "reason", reason, "type", store.RecordName(fr.Type), "client", fr.ClientID, "epoch", fr.Epoch, "seq", fr.Seq, "record", string(fr.Payload), "held", f.entriesHeld(fr.Namespace, fr.ClientID))
		if !w.holed && !f.r.o.AcknowledgeUnapplied {
			// On a whole stream the copy has diverged from the owner's: the
			// record is refused, so the barrier over it cannot pass on a copy
			// that does not hold it, and the owner repairs the key.
			return true, fmt.Sprintf("%s: %s %s", unappliedReason, un, store.RecordName(fr.Type))
		}
	}
	return false, ""
}

func (w *worker) applySnapshot(fr frame) (rejected bool, reason string) {
	f := w.f
	if fr.State == nil {
		return true, "snapshot without state"
	}
	refused, handed, why := w.staleTimed(fr, true)
	if refused {
		return true, why
	}
	f.r.log.Debug("replica snapshot applied", "peer", w.peer, "client", fr.ClientID, "epoch", fr.Epoch, "copyEpoch", fr.State.Epoch, "entries", len(fr.State.Entries), "handedOver", handed, "seq", fr.Seq, "held", f.entriesHeld(fr.Namespace, fr.ClientID))
	recs := append([]store.Record{store.DeleteRecord(fr.Namespace, fr.ClientID)}, store.RecordsFor(fr.State)...)
	if w.timing != nil {
		w.timing.s.SnapshotRecords += len(recs)
	}
	for _, rec := range recs {
		if _, err := f.applyTimed(rec, w.timing, holdApply); err != nil {
			f.r.log.Error("replica snapshot apply failed", "peer", w.peer, "err", err)
			return true, err.Error()
		}
	}
	return false, ""
}

// staleTimed is the lease check of a record, or of a snapshot with the
// hand-over that may still admit it (handed), timed into a sampled batch.
func (w *worker) staleTimed(fr frame, snapshot bool) (refused, handed bool, why string) {
	var began time.Time
	if w.timing != nil {
		began = time.Now()
	}
	refused, why = w.f.stale(w.peer, fr.Namespace, fr.ClientID, fr.Epoch)
	if refused && snapshot && w.f.handedOver(w.peer, fr.Namespace, fr.ClientID, fr.Epoch) {
		refused, handed = false, true
	}
	if w.timing != nil {
		w.timing.s.StaleNs += time.Since(began).Nanoseconds()
	}
	return refused, handed, why
}

// notJournaledReason refuses a retained write whose record this node's
// journal did not take: appended and synced.
const notJournaledReason = "retained record not journaled"

// notLeaderHandOverReason refuses a retained hand-over at a node that does
// not lead the shard and keeps copies of it; the sender makes it again.
const notLeaderHandOverReason = "retained hand-over to a node that does not lead the shard"

// applyRetainedWrite installs a retained message this node leads the shard
// of, waits for its own followers, and acknowledges.
func (w *worker) applyRetainedWrite(fr frame) {
	r := w.f.r
	if fr.Msg == nil {
		w.refuse(fr, "retained write without message")
		return
	}
	shard := r.o.Map.ShardOf(fr.Msg.Namespace, fr.Msg.Topic)
	if r.o.Map.Leader(shard, r.o.Node) != r.o.Node {
		// The map moved under the sender; install anyway rather than lose
		// the write, and let the next map change hand it on.
		r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted"}, 1)
	}
	handOver := fr.Handover && !r.o.PreRetainedMoves
	if handOver {
		holds := len(r.o.Local.RetainedFor(fr.Msg.Namespace, fr.Msg.Topic, r.o.Clock())) > 0
		leads := r.o.Map.Leader(shard, r.o.Node) == r.o.Node
		note, noted := r.keptNote(shard)
		if r.o.AnswerHandOverAnywhere {
			leads = true
		}
		switch judgeHandOver(leads, holds, noted && note.Own) {
		case handOverRefuse:
			r.count("hoardmq_retained_handovers_total", map[string]string{"outcome": "refused-not-leader"}, 1)
			r.log.Debug("retained hand-over refused: this node does not lead the shard and keeps copies of it", "peer", w.peer, "topic", fr.Msg.Topic, "map", fr.Epoch)
			w.refuse(fr, notLeaderHandOverReason)
			return
		case handOverHeld:
			// The leader holds a message for the topic, which the hand-over's
			// older copy must not replace; what the sender holds is held here.
			r.count("hoardmq_retained_handovers_total", map[string]string{"outcome": "held"}, 1)
			r.log.Debug("retained hand-over not installed: a message for the topic is held", "peer", w.peer, "topic", fr.Msg.Topic, "map", fr.Epoch)
			if w.gate.open() {
				w.sendAck(ack{Seq: fr.Seq})
			}
			return
		}
	}
	if !r.o.Sessions.InjectRetained(fr.Msg) {
		w.refuse(fr, notJournaledReason)
		return
	}
	if handOver {
		r.log.Debug("retained hand-over installed", "peer", w.peer, "topic", fr.Msg.Topic, "map", fr.Epoch)
	}
	// The injected record is keyed by topic, so naming it is what lets a
	// refusal of this write fail here instead of blocking every session's
	// barrier to the same peer.
	scope := []store.Scope{{Namespace: fr.Msg.Namespace, Topic: fr.Msg.Topic}}
	if err := r.barrier(r.ctx, scope, handOver); err != nil {
		w.refuse(fr, "followers did not acknowledge: "+err.Error())
		return
	}
	if w.gate.open() {
		w.sendAck(ack{Seq: fr.Seq})
	}
}

// answerFetch sends peer a copy of the session it asked for, or that there is
// none. It is called by the fetch pool and, when the pool is full or off, by
// the peer's worker.
func (f *follower) answerFetch(peer string, fr frame) {
	c := &f.r.fetches
	c.answered.Add(1)
	if !fr.queuedAt.IsZero() {
		c.queueNs.Add(since(fr.queuedAt))
	}
	finding := time.Now()
	ss, ok := f.find(fr.Namespace, fr.ClientID)
	c.answerFindNs.Add(since(finding))
	replying := time.Now()
	f.reply(peer, frame{Kind: kindFetchResult, Req: fr.Req, Namespace: fr.Namespace, ClientID: fr.ClientID, State: ss, Found: ok})
	c.reply.Add(since(replying))
}

// answerQuery returns this node's local retained messages matching the
// filter, restricted to the shards it leads.
func (w *worker) answerQuery(fr frame) {
	r := w.f.r
	msgs := r.localRetained(fr.Namespace, fr.Filter, r.o.Clock())
	r.count("hoardmq_retained_queries_total", map[string]string{"direction": "in"}, 1)
	w.reply(frame{Kind: kindRetainedResult, Req: fr.Req, Msgs: msgs})
}

func (w *worker) reply(fr frame) { w.f.reply(w.peer, fr) }

func (f *follower) reply(peer string, fr frame) {
	body, _ := json.Marshal(fr)
	ctx, cancel := context.WithTimeout(f.r.ctx, f.r.o.AckTimeout)
	defer cancel()
	if err := f.r.o.Transport.Send(ctx, peer, transport.ReplicationAppend, body); err != nil {
		f.r.log.Debug("replication reply not sent", "peer", peer, "err", err)
	}
}

// entriesHeld names a replica's entries for a session, for the debug lines
// that report what a copy held. It renders when the line is emitted, not when
// it is called: a snapshot is applied for every session of every rebalance,
// and the reader of these lines is a session being traced, not a cluster
// moving its shards.
func (f *follower) entriesHeld(ns, id string) heldEntries { return heldEntries{f: f, ns: ns, id: id} }

// heldEntries is the sequence, packet identifier and state of each entry the
// replica holds for one session; "-" when the session is not held.
type heldEntries struct {
	f      *follower
	ns, id string
}

func (h heldEntries) String() string {
	ss, ok := h.f.find(h.ns, h.id)
	if !ok {
		return "-"
	}
	var b strings.Builder
	for i, e := range ss.Entries {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d/%d/%d", e.Seq, e.PacketID, e.State)
	}
	return "[" + b.String() + "]"
}
