package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// outbound is this node's ordered stream of replication frames to one peer:
// frames take a sequence when pushed, a sender goroutine sends them in
// order, and the peer's acknowledgements advance the acked mark the barrier
// waits on. A send failure marks the stream broken: pending frames are
// dropped and the peer is resynced whole when it comes back.
type outbound struct {
	r    *Replicator
	name string

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []frame
	sent   uint64 // sequence of the last frame pushed
	ackSeq uint64 // sequence acknowledged by the peer
	failed uint64 // highest sequence whose send failed: not held by the peer
	// refused holds what this peer has refused and has not since been given a
	// replacement copy of, keyed by session or by retained topic. A barrier
	// naming any of these cannot honestly pass: the record is not held. Only a
	// replacement clears it — a completed resync, a session's whole write, a
	// later retained record of the topic — and nothing forgives a refusal on
	// elapsed time.
	refused map[store.Scope]refusal // bare key → the highest epoch refused, and why
	// wholeAt is, per bare key, the sequence of the last session-delete
	// pushed: the start of a whole write. A refusal of a frame pushed before
	// it is a refusal of the copy being replaced, and is not recorded.
	wholeAt map[store.Scope]uint64
	// clearedBy is, per bare key, the sequence of the whole write or retained
	// record whose push cleared the key's refusal, until the peer acknowledges
	// it. A barrier naming the key waits for it, whenever it started: until
	// then the peer's copy is the one that refused.
	clearedBy map[store.Scope]uint64
	// streamBlocked is the fallback for a refusal that cannot be scoped: a
	// frame carrying neither identity, or the refused set reaching its cap.
	// It blocks every barrier to this peer, which is what the whole stream
	// did before this work item, so the fallback's rate is the measure of how
	// much scoping the fix actually achieves.
	streamBlocked    bool
	fallbackNoKey    atomic.Int64
	fallbackAtCap    atomic.Int64
	fallbackMismatch atomic.Int64
	escapes          atomic.Int64 // a barrier that passed naming a refused key: must stay zero
	// pushed names what each recent sequence carries, oldest first, so a
	// rejection can say what it refused — the ack carries only the sequence.
	// Dropped from the front as the peer acknowledges, and capped so a peer
	// that stops acknowledging cannot grow it without bound.
	pushed        []seqKey
	pushedDropped atomic.Int64
	// repairedAt is, per bare key, when the owner last sent this peer the
	// session whole to repair a divergence, so one refusal per key per
	// acknowledgement timeout is repaired and the rest are counted.
	repairedAt map[store.Scope]time.Time
	// blockedSince is when this stream last became one no barrier may pass —
	// a hole, or a key refused — and zero while it may. It is what the
	// operator's gauge reports the age of (WI 1523).
	blockedSince time.Time
	// hole marks that frames were dropped from this stream. The peer's copy
	// is missing them, and later frames are applied over the gap without
	// either side noticing, so no barrier may pass until a whole resync has
	// been queued behind them.
	hole       bool
	holeGen    uint64 // incremented on every break, so a resync can tell whether one happened while it ran
	resyncing  bool
	lastResync time.Time // a resync snapshots every session held for this peer; they are not free
	// retryPending marks a resync arranged for when the cooldown ends; at
	// most one waits per peer. retries counts those arranged.
	retryPending bool
	retries      atomic.Int64
	probes       atomic.Int64 // markers pushed by a waiting barrier to ask for an acknowledgement
	lastProgress time.Time    // the last acknowledgement received or frame written
	lastProbe    time.Time
	closed       bool
	waiters      []chan struct{}

	// droppedFloor is the newest sequence the window's cap dropped, and
	// brokenAt the sequence the stream stood at when it last broke: what a
	// refusal that cannot be scoped is read against. trace is the stream's
	// record for the failover measurement.
	droppedFloor uint64
	brokenAt     uint64
	trace        outboundTrace
}

// seqKey ties a pushed sequence to what its frame carries.
type seqKey struct {
	seq   uint64
	scope store.Scope // with the epoch the frame was written under
}

// refusal is what a peer refused for a key: the highest epoch it refused a
// record at, and the class of the reason. It holds the key until the owner
// sends the session whole again (push clears it on the delete that begins a
// whole write) or the stream is resynced.
type refusal struct {
	epoch uint64
	class string
}

// maxPushedTracked bounds the attribution window. A peer that stops
// acknowledging would otherwise grow it for as long as the stream runs, and
// this is the storm path.
const maxPushedTracked = 4096

// maxRefusedTracked bounds the refused set. WI 1506 recorded 134 rejections
// in one run and this work item sampled 631 across five, so a cap three
// orders above that is reached only by a burst no scoping would survive;
// reaching it falls back to blocking the stream and is counted.
const maxRefusedTracked = 65536

func newOutbound(r *Replicator, name string) *outbound {
	p := &outbound{r: r, name: name}
	p.cond = sync.NewCond(&p.mu)
	r.wg.Add(1)
	go p.sendLoop()
	return p
}

// stoppedOutbound is what outboundFor answers with for a peer that has no
// stream once the replicator has stopped: closed from the start, so nothing is
// queued and no barrier passes, and with no send loop, so it adds nothing to
// the wait group Stop is waiting on.
func stoppedOutbound(r *Replicator, name string) *outbound {
	p := &outbound{r: r, name: name, closed: true}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// push queues a frame; it takes the next sequence.
func (p *outbound) push(f frame) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.sent++
	f.Seq = p.sent
	if f.Kind == kindResyncMark {
		f.FailedBelow = p.failed + 1
	}
	if p.r != nil {
		if l := p.r.level(); l > 1 {
			f.Level = l
		}
	}
	p.queue = append(p.queue, f)
	// A retained frame carries a topic and no client id; both are identities a
	// refusal can be scoped to, and a frame with neither cannot be.
	if scope := (store.Key{Namespace: f.Namespace, ClientID: f.ClientID, Topic: f.Topic, Epoch: f.Epoch}).Scope(); !scope.Zero() {
		if f.Kind == kindSnapshot || (f.Kind == kindAppend && store.IsSessionDelete(f.Type)) {
			// The owner is about to write the session whole (an import
			// deletes first): what the peer refused of the old copy is
			// answered by the copy that follows, and a refusal of that is
			// recorded again when its acknowledgement comes back. A refusal
			// of an older frame still in flight is not.
			if _, was := p.refused[scope.Bare()]; was {
				delete(p.refused, scope.Bare())
				if p.clearedBy == nil {
					p.clearedBy = make(map[store.Scope]uint64)
				}
				p.clearedBy[scope.Bare()] = p.sent
			}
			p.clearBlockedLocked()
			if p.wholeAt == nil {
				p.wholeAt = make(map[store.Scope]uint64)
			}
			p.wholeAt[scope.Bare()] = p.sent
		} else if f.Topic != "" {
			// A retained record replaces its topic's value, so a refusal of
			// an earlier one is answered by this frame's acknowledgement.
			// Unlike a session's whole write it does not carry the earlier
			// value, so a refusal of an earlier frame arriving later is still
			// recorded: no wholeAt.
			if _, was := p.refused[scope.Bare()]; was {
				delete(p.refused, scope.Bare())
				if p.clearedBy == nil {
					p.clearedBy = make(map[store.Scope]uint64)
				}
				p.clearedBy[scope.Bare()] = p.sent
				p.clearBlockedLocked()
			}
		}
		if len(p.pushed) >= maxPushedTracked {
			p.droppedFloor = p.pushed[0].seq
			p.pushed = p.pushed[1:]
			p.pushedDropped.Add(1)
		}
		p.pushed = append(p.pushed, seqKey{seq: p.sent, scope: scope})
	}
	p.trace.pushed(f.Kind, p.sent-p.ackSeq, len(p.pushed), p.sent, len(p.queue))
	p.mu.Unlock()
	p.cond.Signal()
}

// lastSent is the sequence of the last frame pushed.
func (p *outbound) lastSent() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent
}

// delivered reports whether the frame pushed at seq is held by the peer: its
// acknowledgement covers seq and no send at or above it failed. failed is
// whether one did; the frame must then be pushed again.
func (p *outbound) delivered(seq uint64) (done, failed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq == 0 {
		return false, false
	}
	if p.failed >= seq || p.closed {
		return false, true
	}
	return p.ackSeq >= seq, false
}

// probeIfQuiet asks a quiet stream for a fresh acknowledgement, as a waiting
// barrier does: a frame lost with nothing sent after it is otherwise never
// reported, and a lost acknowledgement never replaced.
func (p *outbound) probeIfQuiet() {
	p.mu.Lock()
	probe := false
	if !p.closed && len(p.queue) == 0 && p.r != nil {
		quiet := p.r.o.AckTimeout / 2
		now := time.Now()
		if now.Sub(p.lastProgress) >= quiet && now.Sub(p.lastProbe) >= quiet {
			probe, p.lastProbe = true, now
		}
	}
	p.mu.Unlock()
	if probe {
		p.probes.Add(1)
		p.push(frame{Kind: kindResyncMark})
	}
}

// acked records the peer's acknowledgement.
func (p *outbound) acked(seq uint64) {
	p.mu.Lock()
	from := p.ackSeq
	if seq > p.ackSeq {
		p.ackSeq = seq
	}
	// The peer holds everything up to here, so their attribution is spent.
	drop := 0
	for drop < len(p.pushed) && p.pushed[drop].seq <= seq {
		drop++
	}
	p.pushed = p.pushed[drop:]
	for k, at := range p.wholeAt {
		// Acknowledged past the whole write: no frame before it can still
		// be answered.
		if at <= seq {
			delete(p.wholeAt, k)
		}
	}
	for k, at := range p.clearedBy {
		if at <= seq {
			delete(p.clearedBy, k)
		}
	}
	p.lastProgress = time.Now()
	waiters := p.waiters
	p.waiters = nil
	p.mu.Unlock()
	p.trace.ackedOne(seq, from)
	for _, w := range waiters {
		close(w)
	}
}

// refusalArrived wakes the barriers waiting on this stream once a refusal has
// been recorded, so one naming the refused key fails now. It moves no mark:
// acked does that, on the peer's acknowledgement after its sync.
func (p *outbound) refusalArrived() {
	p.mu.Lock()
	p.lastProgress = time.Now()
	waiters := p.waiters
	p.waiters = nil
	p.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
}

// ErrReplicaUnavailable is returned by the barrier when a follower that
// should hold the copy is down: the acknowledgement would not be true.
var ErrReplicaUnavailable = errors.New("replication: a follower is unreachable; the copy is not held")

// failedAt records that the peer refused the frame with this sequence: the
// record is not held, so barriers naming what it carried fail until a resync
// replaces the peer's copy.
//
// It does not raise the stream-wide failed mark. That mark means one thing —
// a send that never landed — and sharing it with refusals is what made one
// session's refusal wedge every session's publishes to this peer.
// named is the key the follower said the frame carried, zero from a follower
// that names none.
// It returns true when the stream needs a resync: a refusal that cannot be
// scoped blocks every barrier to this peer, and only a resync clears that, so
// without asking for one the block would be permanent.
func (p *outbound) failedAt(seq uint64, reason string, named store.Scope) (needsResync bool) {
	p.mu.Lock()
	windowed, inWindow := p.windowedLocked(seq)
	scope, fallback := scopeRefusal(windowed, inWindow, named)
	if fallback != "" {
		// Nothing says which key the refused frame carried, or the follower
		// and the window disagree. Nothing can be scoped, so the stream is
		// blocked whole.
		fb := p.fallbackSampleLocked(seq, reason)
		if fallback == fallbackMismatch {
			fb.Outcome = rejMismatch
		}
		p.blockWhole()
		p.mu.Unlock()
		p.trace.rejected(fb.Outcome, &fb)
		if fallback == fallbackNoKey {
			p.fallbackNoKey.Add(1)
		} else {
			p.fallbackMismatch.Add(1)
		}
		p.countFallback(fallback)
		return true
	}
	if !inWindow {
		p.trace.namedOutsideWindow()
	}
	bare := scope.Bare()
	if seq < p.wholeAt[bare] {
		// The frame predates the last whole write of the key: the copy it
		// was refused for is being replaced, and the whole write's own
		// acknowledgement says whether the replacement was refused.
		p.mu.Unlock()
		p.trace.rejected(rejPredated, nil)
		return false
	}
	if cur, seen := p.refused[bare]; seen {
		if scope.Epoch > cur.epoch {
			p.refused[bare] = refusal{epoch: scope.Epoch, class: refusalClass(reason)}
		}
		p.mu.Unlock()
		p.trace.rejected(rejRepeat, nil)
		return false
	}
	if len(p.refused) >= maxRefusedTracked {
		fb := p.fallbackSampleLocked(seq, reason)
		fb.Outcome = rejAtCap
		p.blockWhole()
		p.mu.Unlock()
		p.trace.rejected(rejAtCap, &fb)
		p.fallbackAtCap.Add(1)
		p.countFallback("at-cap")
		return true
	}
	if p.refused == nil {
		p.refused = make(map[store.Scope]refusal)
	}
	p.refused[bare] = refusal{epoch: scope.Epoch, class: refusalClass(reason)}
	p.markBlockedLocked()
	p.mu.Unlock()
	p.trace.rejected(rejScoped, nil)
	return false
}

// fallbackSampleLocked describes a refusal whose sequence the window no
// longer attributes, and why it does not. Callers hold p.mu.
func (p *outbound) fallbackSampleLocked(seq uint64, reason string) FallbackSample {
	fb := FallbackSample{Seq: seq, Sent: p.sent, Acked: p.ackSeq, WindowLen: len(p.pushed),
		DroppedFloor: p.droppedFloor, BrokenAt: p.brokenAt, Reason: reason}
	if len(p.pushed) > 0 {
		fb.WindowOldest = p.pushed[0].seq
	}
	if len(fb.Reason) > 160 {
		fb.Reason = fb.Reason[:160]
	}
	switch {
	case seq <= p.ackSeq:
		fb.Outcome = rejAcked
	case seq <= p.brokenAt:
		fb.Outcome = rejBroken
	case seq <= p.droppedFloor:
		fb.Outcome = rejWindow
	default:
		fb.Outcome = rejNoIdentity
	}
	return fb
}

// scopeOf names what a refused sequence carried, decided as failedAt decides
// it.
func (p *outbound) scopeOf(seq uint64, named store.Scope) (store.Scope, bool) {
	p.mu.Lock()
	windowed, inWindow := p.windowedLocked(seq)
	p.mu.Unlock()
	scope, fallback := scopeRefusal(windowed, inWindow, named)
	return scope, fallback == ""
}

// windowedLocked is what the attribution window says a sequence carried.
// Callers hold p.mu.
func (p *outbound) windowedLocked(seq uint64) (store.Scope, bool) {
	for _, k := range p.pushed {
		if k.seq == seq {
			return k.scope, true
		}
	}
	return store.Scope{}, false
}

// Why a refusal could not be scoped, as the fallback counter labels it.
const (
	fallbackNoKey    = "no-key"
	fallbackMismatch = "mismatch"
)

// scopeRefusal decides which key a refusal is held against: the key the window
// attributed its sequence to, the key the follower named, or neither, with the
// fallback's label. The follower's name is what scopes a refusal of a frame
// the window has already dropped, as a map change can push more frames than
// the window holds before the first refusal comes back. A name that disagrees
// with the window blocks, as no name does.
func scopeRefusal(windowed store.Scope, inWindow bool, named store.Scope) (store.Scope, string) {
	switch {
	case inWindow && named.Zero():
		return windowed, ""
	case inWindow && named.Bare() == windowed.Bare():
		if named.Epoch > windowed.Epoch {
			windowed.Epoch = named.Epoch
		}
		return windowed, ""
	case inWindow:
		return store.Scope{}, fallbackMismatch
	case !named.Zero():
		return named, ""
	}
	return store.Scope{}, fallbackNoKey
}

// repairDue reports whether a repair of the key may be sent now — none was
// within the interval — and records it.
func (p *outbound) repairDue(bare store.Scope, now time.Time, interval time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if at, ok := p.repairedAt[bare]; ok && now.Sub(at) < interval {
		return false
	}
	if p.repairedAt == nil {
		p.repairedAt = make(map[store.Scope]time.Time)
	}
	// A key repaired longer ago than the interval decides nothing here, so
	// the map holds only what was repaired within one of them.
	for k, at := range p.repairedAt {
		if now.Sub(at) >= interval {
			delete(p.repairedAt, k)
		}
	}
	p.repairedAt[bare] = now
	return true
}

// refusalClass folds a follower's refusal reason into the few kinds a
// reader compares: the sender's epoch is behind the lease table
// (stale-epoch), the record could not be applied (apply), or something else.
func refusalClass(reason string) string {
	switch {
	case strings.HasPrefix(reason, unappliedReason):
		return "unapplied"
	case strings.Contains(reason, "epoch"):
		return "stale-epoch"
	case strings.Contains(reason, "apply"):
		return "apply"
	case reason == "":
		return "unknown"
	}
	return "other"
}

// refusedKeys copies the refused set — each key at the epoch it was refused
// at, with the class — for a reader outside the data path.
func (p *outbound) refusedKeys() map[store.Scope]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.refused) == 0 {
		return nil
	}
	out := make(map[store.Scope]string, len(p.refused))
	for k, v := range p.refused {
		k.Epoch = v.epoch
		out[k] = v.class
	}
	return out
}

// holed reports whether frames were dropped from this stream and no resync
// has repaired it since: the state in which no barrier to this peer may pass.
func (p *outbound) holed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hole
}

// blockWhole is the fallback for a refusal that cannot be scoped. It blocks
// the stream the way a dropped frame does — including while the peer is down,
// which the hole flag alone does not cover — and marks it holed so the resync
// the caller then asks for is the thing that clears it. Callers hold p.mu.
func (p *outbound) blockWhole() {
	if !p.streamBlocked {
		p.trace.blockedWhole(time.Now())
	}
	p.streamBlocked = true
	p.failed = p.sent
	if !p.hole {
		p.hole = true
		p.holeGen++
	}
	p.markBlockedLocked()
}

// markBlockedLocked records when this stream became one no barrier may pass;
// clearBlockedLocked forgets it once nothing blocks it. Callers hold p.mu.
func (p *outbound) markBlockedLocked() {
	if p.blockedSince.IsZero() {
		p.blockedSince = p.clock()
	}
}

func (p *outbound) clearBlockedLocked() {
	if !p.hole && !p.streamBlocked && len(p.refused) == 0 {
		p.blockedSince = time.Time{}
	}
}

func (p *outbound) clock() time.Time {
	if p.r != nil && p.r.o.Clock != nil {
		return p.r.o.Clock()
	}
	return time.Now()
}

// blocked reports whether no barrier to this peer may pass, why, and since
// when: "holed" while frames are missing and the resync has not landed,
// "refused" while a key the peer refused has not been sent whole since.
func (p *outbound) blocked() (reason string, since time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.hole || p.streamBlocked:
		return "holed", p.blockedSince
	case len(p.refused) > 0:
		return "refused", p.blockedSince
	}
	return "", time.Time{}
}

// countFallback reports a refusal that could not be scoped. The rate of these
// is how much of the scoping actually holds: every one of them blocks the
// whole stream, which is the behaviour this work item removed.
func (p *outbound) countFallback(kind string) {
	if p.r != nil {
		p.r.count("hoardmq_replication_refusal_fallback_total", map[string]string{"peer": p.name, "kind": kind}, 1)
	}
}

// refusedAmong reports the first of these keys the peer has refused a record
// for and not since been sent whole. Callers hold p.mu. The empty set is the
// common case and costs one length check, so a healthy stream pays nothing
// for the scoping.
func (p *outbound) refusedAmong(keys []store.Scope) (store.Scope, bool) {
	if len(p.refused) == 0 {
		return store.Scope{}, false
	}
	for _, k := range keys {
		if _, ok := p.refused[k.Bare()]; ok {
			return k, true
		}
	}
	return store.Scope{}, false
}

// clearedTargetLocked raises a barrier's target to the whole write that
// cleared the refusal of any key it names, so it passes on the peer's copy
// after that write rather than on the acknowledgement of the refused frame's
// batch, which changed nothing. Callers hold p.mu.
func (p *outbound) clearedTargetLocked(keys []store.Scope, target uint64) uint64 {
	if len(p.clearedBy) == 0 {
		return target
	}
	for _, k := range keys {
		if at := p.clearedBy[k.Bare()]; at > target {
			target = at
		}
	}
	return target
}

// refusal fails a barrier that does not wait on this peer's stream when the
// peer has refused a record for one of the keys.
func (p *outbound) refusal(keys []store.Scope) error {
	p.mu.Lock()
	s, blocked := p.refusedAmong(keys)
	p.mu.Unlock()
	if blocked {
		return p.notHeld(s)
	}
	return nil
}

func (p *outbound) notHeld(s store.Scope) error {
	return &barrierError{p.name, "not-held", fmt.Errorf("%w: %s (%s refused a record for %s/%s%s)", ErrReplicaUnavailable, p.name, p.name, s.Namespace, s.ClientID, s.Topic)}
}

// barrierError is a failed wait, with the reason a barrier failure is counted
// under. Its message and its wrapped error are what the wait returned before
// the reason was recorded.
type barrierError struct {
	peer, reason string
	err          error
}

func (e *barrierError) Error() string { return e.err.Error() }
func (e *barrierError) Unwrap() error { return e.err }

// barrierReason is the reason a failed wait is counted under.
func barrierReason(err error) string {
	var be *barrierError
	if errors.As(err, &be) {
		return be.reason
	}
	return "other"
}

// wait blocks until the peer acknowledged everything pushed before the call;
// a peer that is down fails it (the copy is not held), as does a failed send
// and the context. keys name what the caller is acknowledging for: a refusal
// of any of them fails the wait however long ago it happened, because nothing
// has restored the record since.
func (p *outbound) wait(ctx context.Context, up func(string) bool, keys []store.Scope) error {
	began := time.Now()
	p.mu.Lock()
	target := p.sent
	start := WaitSample{Target: target, Acked: p.ackSeq, Queued: len(p.queue)}
	p.mu.Unlock()
	err := p.waitFor(ctx, up, keys, target)
	if took := time.Since(began); took >= longWait {
		start.AtNs, start.TookNs = began.UnixNano(), took.Nanoseconds()
		if err != nil {
			start.Outcome = barrierReason(err)
		}
		p.trace.waited(start)
	}
	return err
}

// waitFor is wait's loop over the target it read.
func (p *outbound) waitFor(ctx context.Context, up func(string) bool, keys []store.Scope, target uint64) error {
	for {
		// A peer the transport still has is one whose acknowledgements are
		// arriving, so a hole in its stream matters. A peer that is gone
		// holds nothing either way, and the shard map drops it within the
		// ttl; failing every barrier for it until it returns would wedge the
		// node instead.
		peerUp := up(p.name)
		p.mu.Lock()
		if p.failed >= target && target > 0 {
			p.mu.Unlock()
			return &barrierError{p.name, "refused", fmt.Errorf("%w: %s (a frame was not delivered)", ErrReplicaUnavailable, p.name)}
		}
		if p.streamBlocked {
			p.mu.Unlock()
			return &barrierError{p.name, "unscoped", fmt.Errorf("%w: %s (a refusal could not be scoped)", ErrReplicaUnavailable, p.name)}
		}
		if s, blocked := p.refusedAmong(keys); blocked {
			p.mu.Unlock()
			return p.notHeld(s)
		}
		target = p.clearedTargetLocked(keys, target)
		if p.closed {
			// A stopped replicator holds nothing for anyone: a barrier that
			// passed here let a node mid-shutdown acknowledge forwards whose
			// copies died with it.
			p.mu.Unlock()
			return &barrierError{p.name, "stopped", fmt.Errorf("%w: %s (replication stopped)", ErrReplicaUnavailable, p.name)}
		}
		if p.hole && peerUp {
			// The peer acknowledges what it receives, including records for
			// entries it never got, which it applies as no-ops. Its
			// acknowledgement says nothing about the copy until the resync
			// has replaced it.
			p.mu.Unlock()
			return &barrierError{p.name, "resync", fmt.Errorf("%w: %s (frames were dropped; awaiting resync)", ErrReplicaUnavailable, p.name)}
		}
		if p.ackSeq >= target {
			// Reaching a pass with a named key still refused would mean the
			// checks above let a record through that the peer does not hold.
			// It cannot happen while they run first; the counter is what would
			// say so if a later edit reordered them.
			_, escaped := p.refusedAmong(keys)
			p.mu.Unlock()
			if escaped && p.r != nil {
				p.escapes.Add(1)
				p.r.count("hoardmq_replication_barrier_escapes_total", map[string]string{"peer": p.name}, 1)
			}
			return nil
		}
		// Everything was written and the peer is up, but neither an
		// acknowledgement nor a write has happened on the stream for half an
		// acknowledgement timeout: the peer's acknowledgement can be lost
		// while its frames were applied, and with nothing sent after them no
		// later acknowledgement carries it. A marker asks for a fresh
		// cumulative acknowledgement, at most one per half timeout per stream
		// (WI 1494). A frame is never lost silently on a live link: a reset
		// changes the peer's state, which breaks and resyncs the stream, so
		// the acknowledgement can only cover frames the peer applied.
		probe := false
		if peerUp && p.r != nil && len(p.queue) == 0 {
			quiet := p.r.o.AckTimeout / 2
			now := time.Now()
			if now.Sub(p.lastProgress) >= quiet && now.Sub(p.lastProbe) >= quiet {
				probe, p.lastProbe = true, now
			}
		}
		w := make(chan struct{})
		p.waiters = append(p.waiters, w)
		p.mu.Unlock()
		if probe {
			p.probes.Add(1)
			p.push(frame{Kind: kindResyncMark})
		}
		if !peerUp {
			return &barrierError{p.name, "down", fmt.Errorf("%w: %s", ErrReplicaUnavailable, p.name)}
		}
		select {
		case <-w:
		case <-ctx.Done():
			p.mu.Lock()
			ts := TimeoutSample{Target: target, Acked: p.ackSeq, Sent: p.sent}
			p.mu.Unlock()
			p.trace.timedOut(ts)
			return &barrierError{p.name, "timeout", ctx.Err()}
		case <-time.After(100 * time.Millisecond):
			// Re-check the peer's state: it may have gone down while we wait.
		}
	}
}

// broken drops the pending frames and marks everything sent so far as
// failed and settled: barriers covering those frames fail, the stream is
// marked holed so no later barrier passes either, and the peer is resynced
// whole.
func (p *outbound) broken() {
	p.mu.Lock()
	p.queue = nil
	p.pushed = nil // the frames went with the queue; their attribution is spent
	p.wholeAt = nil
	p.clearedBy = nil
	p.brokenAt = p.sent
	p.failed = p.sent
	p.ackSeq = p.sent
	p.hole = true
	p.holeGen++
	p.markBlockedLocked()
	waiters := p.waiters
	p.waiters = nil
	p.mu.Unlock()
	p.trace.broke()
	for _, w := range waiters {
		close(w)
	}
}

func (p *outbound) close() {
	p.mu.Lock()
	p.closed = true
	p.queue = nil
	waiters := p.waiters
	p.waiters = nil
	p.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
	p.cond.Broadcast()
}

func (p *outbound) sendLoop() {
	defer p.r.wg.Done()
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && !p.closed {
			p.cond.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		f := p.queue[0]
		p.queue = p.queue[1:]
		p.mu.Unlock()
		body, _ := json.Marshal(f)
		ctx, cancel := context.WithTimeout(p.r.ctx, p.r.o.AckTimeout)
		err := p.r.o.Transport.Send(ctx, p.name, transport.ReplicationAppend, body)
		cancel()
		p.mu.Lock()
		p.lastProgress = time.Now()
		p.mu.Unlock()
		if err != nil {
			p.r.count("hoardmq_replication_send_failures_total", nil, 1)
			p.r.log.Debug("replication send failed; peer will be resynced", "peer", p.name, "err", err)
			p.broken()
			if p.r.up(p.name) {
				// The transport still has the peer, so no state change will
				// come to trigger the repair; ask for it here.
				p.r.resync(p.name)
			}
		}
	}
}

// startResync claims the resync of a holed stream — only one runs at a
// time — and returns the generation it is repairing. When the cooldown
// refuses it, wait is how long remains, so the caller can try again then.
func (p *outbound) startResync(now time.Time, cooldown time.Duration) (gen uint64, ok bool, wait time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hole || p.resyncing || p.closed {
		return 0, false, 0
	}
	if !p.lastResync.IsZero() && now.Sub(p.lastResync) < cooldown {
		// A resync sends a snapshot of every session this node holds for
		// the peer, taking each session's lock to build it. Under a
		// reconnect storm, send failures arrive continuously, and running
		// one per failure starves the takeover path of those same locks —
		// the repair becomes the outage. The stream stays holed, so no
		// barrier passes over the gap meanwhile, and the caller arranges a
		// retry for when the cooldown ends (WI 1491).
		return 0, false, cooldown - now.Sub(p.lastResync)
	}
	p.resyncing = true
	return p.holeGen, true, 0
}

// whole closes the hole once the resync frames are queued: they carry a
// higher sequence than anything a waiting barrier is holding, so the
// barrier now waits for the replacement copy to be acknowledged. A stream
// that broke again while the resync ran is a different hole, and the
// frames just queued went the way of the first batch; it reports false so
// the caller repairs that one too.
func (p *outbound) whole(gen uint64, now time.Time) bool {
	p.mu.Lock()
	p.resyncing = false
	p.lastResync = now
	if p.holeGen != gen {
		p.mu.Unlock()
		p.trace.resynced(false)
		return false
	}
	wasBlocked := p.streamBlocked
	p.hole = false
	// The peer's copy has been replaced wholesale, so every refusal recorded
	// against it is answered. This is the only thing that clears them.
	p.refused = nil
	if p.r != nil {
		p.r.count("hoardmq_replication_resyncs_total", map[string]string{"peer": p.name}, 1)
	}
	p.streamBlocked = false
	p.clearBlockedLocked()
	waiters := p.waiters
	p.waiters = nil
	p.mu.Unlock()
	p.trace.resynced(true)
	if wasBlocked {
		p.trace.unblocked(time.Now())
	}
	for _, w := range waiters {
		close(w)
	}
	return true
}
