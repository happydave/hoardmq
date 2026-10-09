package replication

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// A per-stream trace of what each replication stream was sent, refused and
// failed, for the failover measurement. It is counters, capped samples and a
// ring of short time buckets, read at the end of a run: node logging moves the
// failover run's election (WI 1734), and this writes nothing while it runs.
// Nothing reads it to decide.

const (
	traceBucket     = 250 * time.Millisecond
	traceBuckets    = 480 // two minutes
	traceSampleHead = 128 // the first samples of a kind, kept
	traceSampleTail = 128 // and a ring of the latest
)

// Why a refusal came back and what the sender made of it.
const (
	rejScoped     = "scoped"          // recorded against the key it carried
	rejRepeat     = "repeat"          // the key was already refused
	rejPredated   = "predated"        // before the key's last whole write: not recorded
	rejGap        = "gap"             // a sequence gap: the stream is holed
	rejWindow     = "fallback-window" // its attribution was dropped by the window's cap
	rejBroken     = "fallback-broken" // its attribution went with a break
	rejAcked      = "fallback-acked"  // at or below the acknowledged mark
	rejNoIdentity = "fallback-no-identity"
	rejAtCap      = "fallback-at-cap"
	rejMismatch   = "fallback-mismatch" // the follower named another key than the window's
	// rejNamed counts, beside a refusal's outcome, one scoped by the key the
	// follower named because the window no longer held its sequence.
	rejNamed = "named-outside-window"
)

// OutboundTrace is what one stream to a peer was sent and answered.
type OutboundTrace struct {
	Pushed     map[string]int64 `json:"pushed"`     // frames by kind
	Rejections map[string]int64 `json:"rejections"` // by outcome
	// BarrierFailures are waits that failed on this stream, by reason, with
	// when the first and last of each happened.
	BarrierFailures map[string]int64  `json:"barrier_failures,omitempty"`
	FailFirstNs     map[string]int64  `json:"barrier_fail_first_ns,omitempty"`
	FailLastNs      map[string]int64  `json:"barrier_fail_last_ns,omitempty"`
	Blocks          int64             `json:"blocks"` // refusals that blocked the stream whole
	Breaks          int64             `json:"breaks"`
	ResyncsQueued   int64             `json:"resyncs_queued"`
	ResyncsRebroken int64             `json:"resyncs_rebroken"`
	MaxDepth        uint64            `json:"max_depth"`  // sent less acknowledged, at a push
	MaxWindow       int               `json:"max_window"` // attribution entries held
	WindowDropped   int64             `json:"window_dropped"`
	Bulk            []BulkPush        `json:"bulk,omitempty"`
	Fallbacks       []FallbackSample  `json:"fallbacks,omitempty"`
	FallbacksLatest []FallbackSample  `json:"fallbacks_latest,omitempty"`
	Timeouts        []TimeoutSample   `json:"timeouts,omitempty"`
	Series          []OutboundBucket  `json:"series,omitempty"`
	Blocked         []BlockedInterval `json:"blocked,omitempty"`
	// AckJumps are the acknowledgements that advanced the mark by
	// ackJumpMin frames or more, and Waits the barrier waits on this stream
	// that took longWait or longer.
	AckJumps []AckJump    `json:"ack_jumps,omitempty"`
	Waits    []WaitSample `json:"waits,omitempty"`
}

// ackJumpMin is the advance from which an acknowledgement is kept as a jump:
// one that covers a follower's big batch.
const ackJumpMin = 1000

// longWait is the barrier wait on one stream from which it is kept as a
// sample.
const longWait = 250 * time.Millisecond

// AckJump is one acknowledgement that covered many frames.
type AckJump struct {
	AtNs   int64  `json:"at_ns"`
	Seq    uint64 `json:"seq"`
	Frames uint64 `json:"frames"` // how far it moved the mark
}

// WaitSample is one long barrier wait on this stream: when it started, how
// long it took, what it waited for, where the stream stood when it started,
// and how it ended ("" when it passed).
type WaitSample struct {
	AtNs    int64  `json:"at_ns"`
	TookNs  int64  `json:"took_ns"`
	Target  uint64 `json:"target"`
	Acked   uint64 `json:"acked"`
	Queued  int    `json:"queued"` // frames not yet sent
	Outcome string `json:"outcome,omitempty"`
}

// BulkPush is one rebalance or resync: how many frames it queued for the
// peer and over what time.
type BulkPush struct {
	Cause   string `json:"cause"`
	AtNs    int64  `json:"at_ns"`
	Frames  int    `json:"frames"`
	TookNs  int64  `json:"took_ns"`
	SentAt  uint64 `json:"sent_at"`  // the stream's sequence when it began
	AckedAt uint64 `json:"acked_at"` // and its acknowledged mark
}

// FallbackSample is one refusal that could not be scoped.
type FallbackSample struct {
	AtNs    int64  `json:"at_ns"`
	Seq     uint64 `json:"seq"`
	Outcome string `json:"outcome"`
	Sent    uint64 `json:"sent"`
	Acked   uint64 `json:"acked"`
	// WindowOldest is the oldest sequence the attribution window still held;
	// DroppedFloor the newest the cap had dropped; BrokenAt the sequence
	// the stream stood at when it last broke.
	WindowOldest uint64 `json:"window_oldest"`
	WindowLen    int    `json:"window_len"`
	DroppedFloor uint64 `json:"dropped_floor"`
	BrokenAt     uint64 `json:"broken_at"`
	Reason       string `json:"reason"`
}

// TimeoutSample is one wait that ran out on this stream.
type TimeoutSample struct {
	AtNs   int64  `json:"at_ns"`
	Target uint64 `json:"target"`
	Acked  uint64 `json:"acked"`
	Sent   uint64 `json:"sent"`
}

// BlockedInterval is a stretch during which the stream was blocked whole.
type BlockedInterval struct {
	FromNs int64 `json:"from_ns"`
	ToNs   int64 `json:"to_ns,omitempty"` // zero while still blocked
}

// OutboundBucket is one traceBucket of a stream's life.
type OutboundBucket struct {
	AtMs      int64            `json:"at_ms"`
	Appends   int64            `json:"appends,omitempty"`
	Snapshots int64            `json:"snapshots,omitempty"`
	Other     int64            `json:"other,omitempty"`
	Acks      int64            `json:"acks,omitempty"`
	Rejected  map[string]int64 `json:"rejected,omitempty"`
	Fails     map[string]int64 `json:"fails,omitempty"`
	MaxDepth  uint64           `json:"max_depth,omitempty"`
	// Sent and Acked are the stream's marks at the bucket's last push and
	// acknowledgement, and MaxQueued the most frames pushed and not yet sent.
	Sent      uint64 `json:"sent,omitempty"`
	Acked     uint64 `json:"acked,omitempty"`
	MaxQueued int    `json:"max_queued,omitempty"`
}

type outboundTrace struct {
	mu        sync.Mutex
	byKind    map[string]int64
	rej       map[string]int64
	fails     map[string]int64
	failFirst map[string]int64
	failLast  map[string]int64
	blocks    int64
	breaks    int64
	queued    int64
	rebroken  int64
	maxDepth  uint64
	maxWindow int
	bulk      []BulkPush
	fbHead    []FallbackSample
	fbTail    []FallbackSample
	fbNext    int
	timeouts  []TimeoutSample
	blocked   []BlockedInterval
	buckets   [traceBuckets]OutboundBucket
	jumps     []AckJump
	waitHead  []WaitSample
	waitTail  []WaitSample
	waitNext  int
}

func (t *outboundTrace) bucket(now time.Time) *OutboundBucket {
	at := now.UnixMilli() / traceBucket.Milliseconds() * traceBucket.Milliseconds()
	b := &t.buckets[(at/traceBucket.Milliseconds())%traceBuckets]
	if b.AtMs != at {
		*b = OutboundBucket{AtMs: at}
	}
	return b
}

func bump(m *map[string]int64, k string, n int64) {
	if *m == nil {
		*m = map[string]int64{}
	}
	(*m)[k] += n
}

// pushed records a frame queued at depth frames above the acknowledged mark
// with window attribution entries held, the stream's sequence sent and the
// frames queued and not yet sent.
func (t *outboundTrace) pushed(kind byte, depth uint64, window int, sent uint64, queued int) {
	now := time.Now()
	t.mu.Lock()
	bump(&t.byKind, kindLabel(kindIndex(kind)), 1)
	b := t.bucket(now)
	b.Sent = sent
	if queued > b.MaxQueued {
		b.MaxQueued = queued
	}
	switch kind {
	case kindAppend:
		b.Appends++
	case kindSnapshot:
		b.Snapshots++
	default:
		b.Other++
	}
	if depth > b.MaxDepth {
		b.MaxDepth = depth
	}
	if depth > t.maxDepth {
		t.maxDepth = depth
	}
	if window > t.maxWindow {
		t.maxWindow = window
	}
	t.mu.Unlock()
}

func (t *outboundTrace) ackedOne(seq, from uint64) {
	now := time.Now()
	t.mu.Lock()
	b := t.bucket(now)
	b.Acks++
	if seq > b.Acked {
		b.Acked = seq
	}
	if seq >= from+ackJumpMin && len(t.jumps) < traceSampleHead*2 {
		t.jumps = append(t.jumps, AckJump{AtNs: now.UnixNano(), Seq: seq, Frames: seq - from})
	}
	t.mu.Unlock()
}

// waited records a barrier wait on this stream that took longWait or longer.
func (t *outboundTrace) waited(s WaitSample) {
	t.mu.Lock()
	t.waitHead, t.waitTail, t.waitNext = keepSample(t.waitHead, t.waitTail, t.waitNext, s)
	t.mu.Unlock()
}

// rejected records what a refusal came to; a fallback also keeps a sample.
func (t *outboundTrace) rejected(outcome string, fb *FallbackSample) {
	now := time.Now()
	t.mu.Lock()
	bump(&t.rej, outcome, 1)
	bump(&t.bucket(now).Rejected, outcome, 1)
	if fb != nil {
		fb.AtNs = now.UnixNano()
		t.fbHead, t.fbTail, t.fbNext = keepSample(t.fbHead, t.fbTail, t.fbNext, *fb)
	}
	t.mu.Unlock()
}

// keepSample keeps the first traceSampleHead samples and a ring of the
// latest traceSampleTail.
func keepSample[T any](head, tail []T, next int, s T) ([]T, []T, int) {
	if len(head) < traceSampleHead {
		return append(head, s), tail, next
	}
	if len(tail) < traceSampleTail {
		return head, append(tail, s), next
	}
	tail[next] = s
	return head, tail, (next + 1) % traceSampleTail
}

func (t *outboundTrace) namedOutsideWindow() {
	now := time.Now()
	t.mu.Lock()
	bump(&t.rej, rejNamed, 1)
	bump(&t.bucket(now).Rejected, rejNamed, 1)
	t.mu.Unlock()
}

func (t *outboundTrace) blockedWhole(now time.Time) {
	t.mu.Lock()
	t.blocks++
	if n := len(t.blocked); n == 0 || t.blocked[n-1].ToNs != 0 {
		if len(t.blocked) < traceSampleHead {
			t.blocked = append(t.blocked, BlockedInterval{FromNs: now.UnixNano()})
		}
	}
	t.mu.Unlock()
}

func (t *outboundTrace) unblocked(now time.Time) {
	t.mu.Lock()
	if n := len(t.blocked); n > 0 && t.blocked[n-1].ToNs == 0 {
		t.blocked[n-1].ToNs = now.UnixNano()
	}
	t.mu.Unlock()
}

func (t *outboundTrace) broke() {
	t.mu.Lock()
	t.breaks++
	t.mu.Unlock()
}

func (t *outboundTrace) resynced(whole bool) {
	t.mu.Lock()
	if whole {
		t.queued++
	} else {
		t.rebroken++
	}
	t.mu.Unlock()
}

func (t *outboundTrace) bulkPushed(b BulkPush) {
	t.mu.Lock()
	if len(t.bulk) < traceSampleHead {
		t.bulk = append(t.bulk, b)
	}
	t.mu.Unlock()
}

func (t *outboundTrace) failed(reason string) {
	now := time.Now()
	t.mu.Lock()
	bump(&t.fails, reason, 1)
	bump(&t.bucket(now).Fails, reason, 1)
	if t.failFirst == nil {
		t.failFirst, t.failLast = map[string]int64{}, map[string]int64{}
	}
	if _, ok := t.failFirst[reason]; !ok {
		t.failFirst[reason] = now.UnixNano()
	}
	t.failLast[reason] = now.UnixNano()
	t.mu.Unlock()
}

func (t *outboundTrace) timedOut(ts TimeoutSample) {
	t.mu.Lock()
	if len(t.timeouts) < traceSampleHead {
		ts.AtNs = time.Now().UnixNano()
		t.timeouts = append(t.timeouts, ts)
	}
	t.mu.Unlock()
}

// bulk times one rebalance or resync per stream it pushes to.
type bulk struct {
	cause   string
	began   time.Time
	streams map[*outbound]*BulkPush
}

func newBulk(cause string) *bulk {
	return &bulk{cause: cause, began: time.Now(), streams: map[*outbound]*BulkPush{}}
}

// note counts one frame about to be pushed to p.
func (b *bulk) note(p *outbound) {
	e := b.streams[p]
	if e == nil {
		p.mu.Lock()
		e = &BulkPush{Cause: b.cause, AtNs: b.began.UnixNano(), SentAt: p.sent, AckedAt: p.ackSeq}
		p.mu.Unlock()
		b.streams[p] = e
	}
	e.Frames++
}

func (b *bulk) done() {
	took := time.Since(b.began)
	for p, e := range b.streams {
		e.TookNs = int64(took)
		p.trace.bulkPushed(*e)
	}
}

func copyCounts(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (t *outboundTrace) stats(windowDropped int64) OutboundTrace {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := OutboundTrace{Pushed: copyCounts(t.byKind), Rejections: copyCounts(t.rej),
		BarrierFailures: copyCounts(t.fails), FailFirstNs: copyCounts(t.failFirst), FailLastNs: copyCounts(t.failLast),
		Blocks: t.blocks, Breaks: t.breaks, ResyncsQueued: t.queued, ResyncsRebroken: t.rebroken,
		MaxDepth: t.maxDepth, MaxWindow: t.maxWindow, WindowDropped: windowDropped,
		Bulk:      append([]BulkPush(nil), t.bulk...),
		Fallbacks: append([]FallbackSample(nil), t.fbHead...),
		Timeouts:  append([]TimeoutSample(nil), t.timeouts...),
		Blocked:   append([]BlockedInterval(nil), t.blocked...),
		AckJumps:  append([]AckJump(nil), t.jumps...)}
	out.FallbacksLatest = ringOrder(t.fbTail, t.fbNext)
	out.Waits = append(append([]WaitSample(nil), t.waitHead...), ringOrder(t.waitTail, t.waitNext)...)
	for _, b := range t.buckets {
		if b.AtMs == 0 {
			continue
		}
		b.Rejected, b.Fails = copyCounts(b.Rejected), copyCounts(b.Fails)
		out.Series = append(out.Series, b)
	}
	sort.Slice(out.Series, func(i, j int) bool { return out.Series[i].AtMs < out.Series[j].AtMs })
	return out
}

// ringOrder returns a full ring oldest first; a ring not yet full is in order.
func ringOrder[T any](ring []T, next int) []T {
	if len(ring) < traceSampleTail {
		return append([]T(nil), ring...)
	}
	out := make([]T, 0, len(ring))
	out = append(out, ring[next:]...)
	return append(out, ring[:next]...)
}

// InboundTrace is what this node's worker for one sending peer applied and
// refused.
type InboundTrace struct {
	Frames   int64            `json:"frames"`
	Rejected map[string]int64 `json:"rejected,omitempty"` // "class/kind"
	FirstNs  map[string]int64 `json:"first_ns,omitempty"` // by class
	LastNs   map[string]int64 `json:"last_ns,omitempty"`
	Samples  []RejectSample   `json:"samples,omitempty"`
	Latest   []RejectSample   `json:"latest,omitempty"`
	Series   []InboundBucket  `json:"series,omitempty"`
}

// RejectSample is one frame this node refused. The lease fields are read for
// the first samples only, so the trace adds no lease-table read per refusal.
type RejectSample struct {
	AtNs       int64  `json:"at_ns"`
	Seq        uint64 `json:"seq"`
	Kind       string `json:"kind"`
	Type       string `json:"type,omitempty"`
	Client     string `json:"client,omitempty"`
	Topic      string `json:"topic,omitempty"`
	Epoch      uint64 `json:"epoch,omitempty"`
	Class      string `json:"class"`
	Reason     string `json:"reason"`
	LeaseOwner string `json:"lease_owner,omitempty"`
	LeaseEpoch uint64 `json:"lease_epoch,omitempty"`
	Seen       uint64 `json:"seen,omitempty"`
}

// InboundBucket is one traceBucket of a worker's life.
type InboundBucket struct {
	AtMs     int64            `json:"at_ms"`
	Frames   int64            `json:"frames,omitempty"`
	Rejected map[string]int64 `json:"rejected,omitempty"`
}

type inboundTrace struct {
	mu      sync.Mutex
	frames  int64
	rej     map[string]int64
	first   map[string]int64
	last    map[string]int64
	head    []RejectSample
	tail    []RejectSample
	next    int
	buckets [traceBuckets]InboundBucket
}

func (t *inboundTrace) bucket(now time.Time) *InboundBucket {
	at := now.UnixMilli() / traceBucket.Milliseconds() * traceBucket.Milliseconds()
	b := &t.buckets[(at/traceBucket.Milliseconds())%traceBuckets]
	if b.AtMs != at {
		*b = InboundBucket{AtMs: at}
	}
	return b
}

func (t *inboundTrace) applied(n int) {
	now := time.Now()
	t.mu.Lock()
	t.frames += int64(n)
	t.bucket(now).Frames += int64(n)
	t.mu.Unlock()
}

// wantsLease reports whether the next refusal will be a head sample, which
// is the only one the lease is read for.
func (t *inboundTrace) wantsLease() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.head) < traceSampleHead
}

func (t *inboundTrace) refusedFrame(s RejectSample) {
	now := time.Now()
	s.AtNs = now.UnixNano()
	t.mu.Lock()
	key := s.Class + "/" + s.Kind
	bump(&t.rej, key, 1)
	bump(&t.bucket(now).Rejected, s.Class, 1)
	if t.first == nil {
		t.first, t.last = map[string]int64{}, map[string]int64{}
	}
	if _, ok := t.first[s.Class]; !ok {
		t.first[s.Class] = s.AtNs
	}
	t.last[s.Class] = s.AtNs
	t.head, t.tail, t.next = keepSample(t.head, t.tail, t.next, s)
	t.mu.Unlock()
}

func (t *inboundTrace) stats() InboundTrace {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := InboundTrace{Frames: t.frames, Rejected: copyCounts(t.rej), FirstNs: copyCounts(t.first), LastNs: copyCounts(t.last),
		Samples: append([]RejectSample(nil), t.head...), Latest: ringOrder(t.tail, t.next)}
	for _, b := range t.buckets {
		if b.AtMs == 0 {
			continue
		}
		b.Rejected = copyCounts(b.Rejected)
		out.Series = append(out.Series, b)
	}
	sort.Slice(out.Series, func(i, j int) bool { return out.Series[i].AtMs < out.Series[j].AtMs })
	return out
}

// rejectClass names why a follower refused a frame, from the reason it sent.
func rejectClass(reason string) string {
	switch {
	case strings.HasPrefix(reason, syncFailedReason):
		return "sync-failed"
	case strings.HasPrefix(reason, gapReason):
		return "gap"
	case strings.HasPrefix(reason, unappliedReason):
		return "unapplied"
	case strings.HasPrefix(reason, levelReason):
		return "feature-level"
	case strings.Contains(reason, "the highest grant seen"):
		return "behind-seen"
	case strings.Contains(reason, "behind lease epoch"):
		return "behind-lease"
	case strings.Contains(reason, "is owned by"):
		return "owned-elsewhere"
	case strings.HasPrefix(reason, "no epoch"):
		return "no-epoch"
	case strings.HasPrefix(reason, "map version"):
		return "map-version"
	case strings.HasPrefix(reason, "followers did not acknowledge"):
		return "retained-unacknowledged"
	case strings.HasPrefix(reason, notJournaledReason):
		return "not-journaled"
	case strings.Contains(reason, "without"):
		return "malformed"
	}
	return "apply"
}

// StreamTrace is one peer's two streams as this node saw them.
type StreamTrace struct {
	Peer string         `json:"peer"`
	Out  *OutboundTrace `json:"out,omitempty"`
	In   *InboundTrace  `json:"in,omitempty"`
}

// StreamTraces reports every stream this node sends on or receives from.
// Nothing reads it to decide.
func (r *Replicator) StreamTraces() []StreamTrace {
	byPeer := map[string]*StreamTrace{}
	at := func(peer string) *StreamTrace {
		s := byPeer[peer]
		if s == nil {
			s = &StreamTrace{Peer: peer}
			byPeer[peer] = s
		}
		return s
	}
	r.mu.Lock()
	peers := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	for _, p := range peers {
		o := p.trace.stats(p.pushedDropped.Load())
		at(p.name).Out = &o
	}
	r.follower.wmu.Lock()
	workers := make([]*worker, 0, len(r.follower.workers))
	for _, w := range r.follower.workers {
		workers = append(workers, w)
	}
	r.follower.wmu.Unlock()
	for _, w := range workers {
		in := w.trace.stats()
		at(w.peer).In = &in
	}
	out := make([]StreamTrace, 0, len(byPeer))
	for _, s := range byPeer {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	return out
}
