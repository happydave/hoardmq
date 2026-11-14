package replication

import (
	"encoding/json"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// wholeCopyLevel is the cluster feature level from which a follower makes a
// replica copy only from a snapshot frame: a lone session record from a frame
// stamped at or above it that finds no copy is refused, not applied.
const wholeCopyLevel uint64 = 2

// Why a session was written whole, as hoardmq_replication_whole_writes_total
// labels it.
const (
	causeImport     = "import"
	causeRewrite    = "rewrite"
	causeCleanStart = "clean-start"
	causeCreate     = "create"
	causeResync     = "resync"
	causeRepair     = "repair"
	causeRebalance  = "rebalance"
)

// How a whole write is sent, by its size against the frame limit.
const (
	formSnapshot       = "snapshot"         // one snapshot frame
	formHeadAndRecords = "head-and-records" // a snapshot without entries, then each entry
	formTooLarge       = "refused-too-large"
)

// Why a repair was made, as hoardmq_replication_repairs_total labels it.
const (
	repairUnapplied    = "unapplied"
	repairEarlierGrant = "earlier-grant"
)

// openWholes is the whole writes the session manager has open: while one is
// open for a key, the tap sends none of its records, and its close sends the
// session whole. Every record of a session is written under the session's
// lock, which the manager holds from open to close, so no other writer of the
// key runs meanwhile.
type openWholes struct {
	n sync.Map // bare key → struct{}
	// open counts the whole writes open, so a tap while none is pays one load.
	open atomic.Int64
}

// OpenWhole begins a whole write of a session this node owns: an import, a
// rewrite, a Clean Start or a brand-new session's creation. Until the
// returned close is called its records reach the tap as usual and run their
// owner-side effects, but none is sent; close sends the session whole, as one
// snapshot frame where it fits (wholeForm), from st, the state the write
// wrote, or holds the key refused at each target when appended is false. The
// caller holds the session's lock from open to close. A nil close means the
// write's records are sent as records.
func (r *Replicator) OpenWhole(ns, id, cause string) func(st *store.SessionState, appended bool) {
	if r.o.LoneMetaMakesCopy || r.o.PreWholeCopies || r.o.Transport == nil {
		return nil
	}
	bare := store.Scope{Namespace: ns, ClientID: id}
	if _, dup := r.wholes.n.LoadOrStore(bare, struct{}{}); dup {
		return nil
	}
	r.wholes.open.Add(1)
	return func(st *store.SessionState, appended bool) {
		r.wholes.n.Delete(bare)
		r.wholes.open.Add(-1)
		m := r.o.Map.Current()
		if m.Version == 0 {
			return
		}
		targets := recordTargets(m, r.o.Map.ShardOf(ns, id), r.o.Node, false)
		if len(targets) == 0 {
			return
		}
		if !appended || st == nil {
			// The owner's journal does not hold the write whole, so neither
			// may a target be taken to: its copy stays the one it had, and
			// every barrier naming the key fails until the rewrite is sent.
			for _, t := range targets {
				if p := r.outboundFor(t); p.setHeld(bare, heldUnwritten) {
					r.resync(t)
				}
			}
			r.count("hoardmq_replication_whole_writes_total", map[string]string{"cause": cause, "form": "unwritten"}, 1)
			return
		}
		r.pushWhole(st, st.Epoch, targets, cause, nil)
	}
}

// wholeOpen reports whether a whole write of the key is open.
func (r *Replicator) wholeOpen(bare store.Scope) bool {
	if r.wholes.open.Load() == 0 {
		return false
	}
	_, ok := r.wholes.n.Load(bare)
	return ok
}

// SessionLeft tells the replication layer a session this node held refused at
// its targets, its whole write unwritten, is no longer this node's: those
// barriers are no longer this node's to fail.
func (r *Replicator) SessionLeft(ns, id string) {
	bare := store.Scope{Namespace: ns, ClientID: id}
	for _, p := range r.streams() {
		p.clearHeld(bare, heldUnwritten)
	}
}

// streams lists this node's outbound streams.
func (r *Replicator) streams() []*outbound {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*outbound, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, p)
	}
	return out
}

// wholeEncoding is a session's whole write, encoded once, with the form its
// size takes against the frame limit.
type wholeEncoding struct {
	st      *store.SessionState
	epoch   uint64
	form    string
	size    int             // the snapshot frame's encoded bytes
	full    json.RawMessage // the state, form 1
	head    json.RawMessage // the state without its entries, form 2
	entries []store.Record  // form 2's enqueue records
}

// encodeWhole encodes st once and picks its form.
func (r *Replicator) encodeWhole(st *store.SessionState, epoch uint64) wholeEncoding {
	e := wholeEncoding{st: st, epoch: epoch}
	e.full, _ = json.Marshal(st)
	limit := r.frameLimit()
	e.size = snapshotFrameSize(st.Namespace, st.ClientID, epoch, len(e.full))
	headSize, entryMax := e.size, 0
	if e.size > limit {
		head := *st
		head.Entries = make([]store.Entry, 0)
		e.head, _ = json.Marshal(&head)
		headSize = snapshotFrameSize(st.Namespace, st.ClientID, epoch, len(e.head))
		recs := store.RecordsFor(st)
		e.entries = recs[len(recs)-len(st.Entries):]
		for _, rec := range e.entries {
			if n := appendFrameSize(rec, epoch); n > entryMax {
				entryMax = n
			}
		}
	}
	e.form = wholeForm(e.size, headSize, entryMax, limit)
	return e
}

// wholeForm decides how a whole write is sent: one snapshot frame when it
// fits the limit; a head without the entries and each entry on its own when
// those fit; otherwise the key is refused at the peer, its delete sent.
func wholeForm(snapshot, head, largestEntry, limit int) string {
	switch {
	case snapshot <= limit:
		return formSnapshot
	case head <= limit && largestEntry <= limit:
		return formHeadAndRecords
	}
	return formTooLarge
}

// snapshotFrameSize is a snapshot frame's encoded size with a state of n
// bytes, its sequence and level at their widest.
func snapshotFrameSize(ns, id string, epoch uint64, n int) int {
	body := encodeFrame(frame{Kind: kindSnapshot, Seq: math.MaxUint64, Namespace: ns, ClientID: id, Epoch: epoch, Level: math.MaxUint64, stateJSON: json.RawMessage("{}")})
	return len(body) - 2 + n
}

// appendFrameSize is an append frame's encoded size for rec.
func appendFrameSize(rec store.Record, epoch uint64) int {
	return len(encodeFrame(frame{Kind: kindAppend, Seq: math.MaxUint64, Type: rec.Type, Namespace: rec.Key.Namespace, ClientID: rec.Key.ClientID, Epoch: epoch, Level: math.MaxUint64, Payload: rec.Payload}))
}

// frameLimit is the largest payload the transport sends.
func (r *Replicator) frameLimit() int {
	if r.o.MaxFrame > 0 {
		return r.o.MaxFrame
	}
	return 16 << 20
}

// pushWhole queues st whole to each of peers in the form its size takes. A
// write that fits ends the key's refusals of this node's own on every stream,
// not only on those it went to. The caller holds the session's lock.
func (r *Replicator) pushWhole(st *store.SessionState, epoch uint64, peers []string, cause string, b *bulk) {
	e := r.encodeWhole(st, epoch)
	bare := store.Scope{Namespace: st.Namespace, ClientID: st.ClientID}
	for _, name := range peers {
		p := r.outboundFor(name)
		if b != nil {
			b.note(p)
		}
		r.pushWholeTo(name, p, bare, e)
	}
	r.count("hoardmq_replication_whole_writes_total", map[string]string{"cause": cause, "form": e.form}, 1)
	if e.form == formTooLarge {
		return
	}
	for _, p := range r.streams() {
		p.clearHeld(bare, "")
	}
}

// pushWholeTo queues one encoded whole write to one peer.
func (r *Replicator) pushWholeTo(name string, p *outbound, bare store.Scope, e wholeEncoding) {
	st := e.st
	switch e.form {
	case formSnapshot:
		p.push(frame{Kind: kindSnapshot, Namespace: st.Namespace, ClientID: st.ClientID, Epoch: e.epoch, State: st, stateJSON: e.full})
		r.count("hoardmq_replication_records_total", map[string]string{"direction": "out"}, 1)
		return
	case formHeadAndRecords:
		first := p.push(frame{Kind: kindSnapshot, Namespace: st.Namespace, ClientID: st.ClientID, Epoch: e.epoch, stateJSON: e.head})
		last := first
		for _, rec := range e.entries {
			last = p.push(frame{Kind: kindAppend, Type: rec.Type, Namespace: st.Namespace, ClientID: st.ClientID, Epoch: e.epoch, Payload: rec.Payload})
		}
		p.extendWhole(bare, first, last)
		r.count("hoardmq_replication_records_total", map[string]string{"direction": "out"}, float64(1+len(e.entries)))
	default:
		del := store.DeleteRecord(st.Namespace, st.ClientID)
		p.push(frame{Kind: kindAppend, Type: del.Type, Namespace: st.Namespace, ClientID: st.ClientID, Epoch: e.epoch, Payload: del.Payload})
		r.count("hoardmq_replication_records_total", map[string]string{"direction": "out"}, 1)
		if p.setHeld(bare, heldTooLarge) {
			r.resync(name)
		}
	}
	if ok, n := p.warnDue(&p.oversizeWarn, r.o.Clock()); ok {
		r.log.Warn("a session's whole write passes the frame limit", "peer", name, "client", st.ClientID, "bytes", e.size, "form", e.form, "since", n)
	}
}

// redecisions rate-limits the fresh decisions of a key held refused for its
// size, one per acknowledgement timeout, with one left to run at the
// interval's end when one was skipped.
type redecisions struct {
	mu   sync.Mutex
	keys map[store.Scope]*redecision
}

type redecision struct {
	at       time.Time
	trailing bool
}

// redecideDue decides whether a key last decided at last may be decided now,
// and if not, how long until the trailing decision should run (0 when one is
// already arranged).
func redecideDue(last, at time.Time, interval time.Duration, trailing bool) (run bool, after time.Duration) {
	if last.IsZero() || at.Sub(last) >= interval {
		return true, 0
	}
	if trailing {
		return false, 0
	}
	return false, interval - at.Sub(last)
}

// shrank answers a record that takes from a session held refused at some peer
// for its size: the session may fit a frame now, so it is decided again by a
// whole write, off the appending goroutine.
func (r *Replicator) shrank(bare store.Scope) {
	now := r.o.Clock()
	r.redecides.mu.Lock()
	if r.redecides.keys == nil {
		r.redecides.keys = map[store.Scope]*redecision{}
	}
	d := r.redecides.keys[bare]
	if d == nil {
		d = &redecision{}
		r.redecides.keys[bare] = d
	}
	run, after := redecideDue(d.at, now, r.o.AckTimeout, d.trailing)
	switch {
	case run:
		d.at = now
	case after > 0:
		d.trailing = true
	}
	r.redecides.mu.Unlock()
	if !run && after == 0 {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if after > 0 {
			t := time.NewTimer(after)
			defer t.Stop()
			select {
			case <-t.C:
			case <-r.stop:
				return
			}
			r.redecides.mu.Lock()
			d.at, d.trailing = r.o.Clock(), false
			r.redecides.mu.Unlock()
		}
		r.redecideNow(bare)
	}()
}

// redecideNow writes a key whole to the peers holding it refused for its
// size.
func (r *Replicator) redecideNow(bare store.Scope) {
	var peers []string
	for _, p := range r.streams() {
		if why, ok := p.heldWhy(bare); ok && why == heldTooLarge {
			peers = append(peers, p.name)
		}
	}
	if len(peers) == 0 {
		return
	}
	r.wholeWrite(bare, peers, nil, causeRepair)
}

// endHeldOffTargets ends this node's own refusals on the streams of peers no
// longer the key's targets under m: a refusal there protects no copy, and a
// barrier reads it all the same. Run once the rebalance's whole writes to the
// gained targets are queued.
func (r *Replicator) endHeldOffTargets(m interface {
	Targets(shard int, owner string) []string
}) {
	for _, p := range r.streams() {
		for _, k := range p.heldKeys() {
			if !contains(m.Targets(r.o.Map.ShardOf(k.Namespace, k.ClientID), r.o.Node), p.name) {
				p.clearHeld(k, "")
			}
		}
	}
}
