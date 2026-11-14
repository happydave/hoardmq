package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// A full route set larger than one transport frame travels in parts to a
// peer whose data connection runs framing version 6 or above, and in one
// frame, as before, to any other. Every part of a set carries its snapshot,
// its cover and its index, and the last says it is last; the receiver applies
// the set only once every index from 0 to the last has arrived, in order, and
// acknowledges it (held). The sender sends parts in rounds, and a peer's next
// round waits until the link's writer has taken the previous round's last
// part from the ordered queue, so a set never puts more than one round ahead
// of the deltas, forwards and replication frames behind it.

// Route-update kinds after a delta (1) and a one-frame set (2).
const (
	routePart byte = 3 // one part of a full set
	routeHeld byte = 4 // a full set received in parts is held
)

// DefaultPartSize is the encoded size a part of a full set, or a delta batch,
// stays under: half the smallest frame limit the configuration allows, so a
// part fits every node's limit whatever each configures. An entry larger than
// it travels alone.
const DefaultPartSize = 32 << 10

// roundParts is how many parts go to a peer before the flusher waits for the
// link's writer to take them.
const roundParts = 32

// maxPartIndex bounds a part's index; one above it is malformed.
const maxPartIndex = 1 << 20

// partsVersion is the framing version from which a peer's route plane reads a
// set in parts.
const partsVersion = transport.Version6

// setPart is the body of a routePart message.
type setPart struct {
	Snapshot uint64  `json:"snapshot"`
	Cover    uint64  `json:"cover"`
	Index    uint32  `json:"index"`
	Last     bool    `json:"last,omitempty"`
	Entries  []Entry `json:"entries"`
}

// setHeld is the body of a routeHeld message.
type setHeld struct {
	Snapshot uint64 `json:"snapshot"`
}

// The largest encoding of an entry's fields other than its three strings,
// and of a part's envelope with its kind byte: an entry's size is bounded by
// these plus its strings' JSON encodings.
const (
	entryEnvelope = len(`{"ns":"","filter":"","owner":"","version":18446744073709551615,"tombstone":true,"count":9223372036854775807,"batch":18446744073709551615},`)
	partEnvelope  = 1 + len(`{"snapshot":18446744073709551615,"cover":18446744073709551615,"index":1048576,"last":true,"entries":[]}`)
)

// EntryFits reports whether a route entry for a filter can travel in a frame
// of limit bytes in its largest encoding: a tombstone, the widest count and
// batch number, inside a part's envelope. A limit of zero or less is none.
// The JSON encoder writes <, >, & and control bytes as six bytes each, so a
// filter's length alone does not decide it near the limit.
func EntryFits(ns, filter, owner string, limit int) bool {
	if limit <= 0 {
		return true
	}
	if 6*(len(ns)+len(filter)+len(owner))+entryEnvelope+partEnvelope <= limit {
		return true
	}
	return jsonLen(ns)+jsonLen(filter)+jsonLen(owner)+entryEnvelope+partEnvelope <= limit
}

// jsonLen is the length of a string's JSON encoding without its quotes.
func jsonLen(s string) int {
	b, _ := json.Marshal(s)
	return len(b) - 2
}

// composition is one read of this node's own entries for full sets: shared
// by the peers whose set starts in the flush that read it.
type composition struct {
	snapshot, cover uint64
	entries         []Entry
}

// outSet is a set in progress to one peer, the flusher's alone.
type outSet struct {
	comp   *composition
	pos    int    // the next entry to send
	index  uint32 // the next part's index
	gen    uint64 // the data link's generation when the set started
	target uint64 // DataTaken must reach it before the next round
	sent   bool   // the last part was admitted
	owed   bool   // a periodic set was asked for while this one ran
}

// inSet is a set in progress from one peer, under its lock.
type inSet struct {
	mu       sync.Mutex
	active   bool
	snapshot uint64
	cover    uint64
	next     uint32
	entries  []Entry
	bytes    int
	chain    uint64 // the last delta batch held since the set's cover
	broken   bool   // a delta came that was not the chain's next
	why      string // the last reason a set from this peer was abandoned
}

// sendError classifies a route send's failure.
func sendError(err error) string {
	switch {
	case errors.Is(err, transport.ErrTooLarge):
		return "too-large"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, transport.ErrPeerDown), errors.Is(err, transport.ErrUnknownPeer), errors.Is(err, transport.ErrClosed):
		return "down"
	}
	return "other"
}

// sendRoute sends one route-update payload to a peer with the one-second
// admission bound, counting and logging a failure under kind.
func (p *Plane) sendRoute(peer, kind string, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := p.o.Transport.Send(ctx, peer, transport.RouteUpdate, payload)
	cancel()
	if err != nil {
		p.sendFailed(peer, kind, sendError(err), len(payload), err)
	}
	return err
}

// warnState is one rate-limited Warn's state.
type warnState struct {
	last  time.Time
	since int
}

// sendFailed counts a refused or failed route send and logs it at Warn: at
// the first of a peer, kind and reason, then at most once per warnEvery while
// they continue, with how many since.
func (p *Plane) sendFailed(peer, kind, reason string, size int, err error) {
	p.count("hoardmq_route_send_failures_total", map[string]string{"kind": kind, "reason": reason}, 1)
	key := peer + "\x00" + kind + "\x00" + reason
	p.warnMu.Lock()
	w := p.warns[key]
	if w == nil {
		w = &warnState{}
		p.warns[key] = w
	}
	w.since++
	due := w.last.IsZero() || time.Since(w.last) >= p.warnEvery
	n := w.since
	if due {
		w.last, w.since = time.Now(), 0
	}
	p.warnMu.Unlock()
	if due {
		p.log.Warn("route update not sent", "peer", peer, "kind", kind, "reason", reason, "bytes", size,
			"limit", p.o.MaxFrame, "failures", n, "err", err)
	}
}

// abandonOut ends a set to a peer, counted with its reason.
func (p *Plane) abandonOut(peer, reason string) {
	delete(p.outSets, peer)
	p.count("hoardmq_route_sets_abandoned_total", map[string]string{"side": "send", "reason": reason}, 1)
}

// compose reads this node's own entries for full sets, leaving out any no
// frame can carry; cover was fixed before the read.
func (p *Plane) compose(cover uint64) *composition {
	limit := p.o.MaxFrame
	keep := func(e Entry) bool { return EntryFits(e.Namespace, e.Filter, e.Owner, limit) }
	if limit <= 0 {
		keep = nil
	}
	entries, snapshot := p.table.ReadOwn(&p.version, keep)
	c := &composition{snapshot: snapshot, cover: cover, entries: entries}
	if p.o.Metrics != nil {
		p.o.Metrics.Set("hoardmq_route_set_entries", nil, float64(len(entries)))
	}
	return c
}

// sendSingle sends a composition in one frame, the form for a peer that does
// not read parts.
func (p *Plane) sendSingle(peer string, c *composition) {
	body, _ := json.Marshal(fullSync{Snapshot: c.snapshot, Entries: c.entries, Batch: c.cover})
	if p.sendRoute(peer, "full", append([]byte{routeFull}, body...)) == nil {
		p.count("hoardmq_route_syncs_total", map[string]string{"direction": "out"}, 1)
	}
}

// encodePart encodes the next part of a set from entry pos: entries in order
// until the next would take it past the part size, at least one. It returns
// the payload and the position after it.
func (p *Plane) encodePart(c *composition, pos int, index uint32) ([]byte, int) {
	buf := make([]byte, 0, p.partSize)
	n := pos
	size := partEnvelope
	var body []byte
	for n < len(c.entries) {
		b, _ := json.Marshal(c.entries[n])
		if n > pos && size+len(b)+1 > p.partSize {
			break
		}
		if n > pos {
			body = append(body, ',')
		}
		body = append(body, b...)
		size += len(b) + 1
		n++
	}
	buf = append(buf, routePart)
	buf = append(buf, `{"snapshot":`...)
	buf = strconv.AppendUint(buf, c.snapshot, 10)
	buf = append(buf, `,"cover":`...)
	buf = strconv.AppendUint(buf, c.cover, 10)
	buf = append(buf, `,"index":`...)
	buf = strconv.AppendUint(buf, uint64(index), 10)
	if n == len(c.entries) {
		buf = append(buf, `,"last":true`...)
	}
	buf = append(buf, `,"entries":[`...)
	buf = append(buf, body...)
	buf = append(buf, "]}"...)
	return buf, n
}

// peerInfo is one peer's transport snapshot.
func (p *Plane) peerInfo(peer string) (transport.PeerInfo, bool) {
	for _, pi := range p.o.Transport.Peers() {
		if pi.Name == peer {
			return pi, true
		}
	}
	return transport.PeerInfo{}, false
}

// readsParts is the form decision: a peer whose data connection runs at
// partsVersion or above reads a set in parts. A legacy plane sends none.
func readsParts(legacy bool, info transport.PeerInfo) bool {
	return !legacy && info.DataState == transport.Up && info.DataVersion >= partsVersion
}

// startSet begins a set to a peer from a composition, in the form that
// applies now.
func (p *Plane) startSet(peer string, c *composition) {
	info, ok := p.peerInfo(peer)
	if !ok || info.State != transport.Up {
		return
	}
	if !readsParts(p.legacy(), info) {
		p.sendSingle(peer, c)
		return
	}
	s := &outSet{comp: c, gen: info.DataGeneration}
	p.outSets[peer] = s
	p.sendRound(peer, s)
}

// sendRound sends a set's next round to a peer, checking first that the data
// link it started on is still there and still reads parts. It reports whether
// a new set must be composed at once (the set was superseded).
func (p *Plane) sendRound(peer string, s *outSet) (recompose bool) {
	info, _ := p.peerInfo(peer)
	switch {
	case info.State != transport.Up || info.DataState != transport.Up:
		p.abandonOut(peer, "down")
		return false
	case info.DataGeneration != s.gen || !readsParts(p.legacy(), info):
		p.abandonOut(peer, "superseded")
		return true
	}
	for i := 0; i < p.roundParts && !s.sent; i++ {
		if s.index >= maxPartIndex {
			p.abandonOut(peer, "too-large")
			return false
		}
		payload, next := p.encodePart(s.comp, s.pos, s.index)
		if err := p.sendRoute(peer, "part", payload); err != nil {
			p.abandonOut(peer, sendError(err))
			return false
		}
		p.count("hoardmq_route_set_parts_total", map[string]string{"direction": "out"}, 1)
		s.index++
		s.pos = next
		if next == len(s.comp.entries) {
			s.sent = true
		}
	}
	after, _ := p.peerInfo(peer)
	if after.DataGeneration != s.gen {
		p.abandonOut(peer, "superseded")
		return true
	}
	s.target = after.DataAdmitted
	if s.sent {
		p.count("hoardmq_route_syncs_total", map[string]string{"direction": "out"}, 1)
		if p.o.Metrics != nil {
			p.o.Metrics.Set("hoardmq_route_set_parts", nil, float64(s.index))
		}
	}
	return false
}

// advanceSets moves every set in progress on by a round where the link has
// taken the previous one, and ends those whose last part has been taken. It
// returns the peers whose set must be composed again at once and those owed a
// set at the end of theirs.
func (p *Plane) advanceSets() (again, owed []string) {
	for peer, s := range p.outSets {
		info, _ := p.peerInfo(peer)
		if info.State != transport.Up || info.DataState != transport.Up {
			p.abandonOut(peer, "down")
			continue
		}
		if info.DataGeneration != s.gen {
			p.abandonOut(peer, "superseded")
			again = append(again, peer)
			continue
		}
		if info.DataTaken < s.target {
			continue
		}
		if s.sent {
			delete(p.outSets, peer)
			if s.owed {
				owed = append(owed, peer)
			}
			continue
		}
		if p.sendRound(peer, s) {
			again = append(again, peer)
		}
	}
	return again, owed
}

// receivePart takes one part of a peer's set, and applies the set when the
// part completes it.
func (p *Plane) receivePart(peer string, body []byte) {
	var part setPart
	if err := json.Unmarshal(body, &part); err != nil || part.Snapshot == 0 || part.Index > maxPartIndex {
		p.dropPart(peer, nil, "malformed")
		in := p.inSet(peer)
		in.mu.Lock()
		p.discardLocked(in, "malformed")
		in.mu.Unlock()
		return
	}
	p.count("hoardmq_route_set_parts_total", map[string]string{"direction": "in"}, 1)
	in := p.inSet(peer)
	in.mu.Lock()
	switch {
	case part.Index == 0 && (!in.active || part.Snapshot > in.snapshot):
		if p.appliedSnapshot(peer) >= part.Snapshot {
			in.mu.Unlock()
			p.dropPart(peer, in, "stale")
			return
		}
		if in.active {
			p.discardLocked(in, "superseded")
		}
		in.active, in.snapshot, in.cover, in.next = true, part.Snapshot, part.Cover, 0
		in.entries, in.bytes, in.chain, in.broken = nil, 0, part.Cover, part.Cover == 0
	case !in.active || part.Snapshot < in.snapshot:
		in.mu.Unlock()
		p.dropPart(peer, in, "stale")
		return
	case part.Snapshot == in.snapshot && (part.Index != in.next || part.Cover != in.cover):
		p.discardLocked(in, "part-missing")
		in.mu.Unlock()
		return
	case part.Snapshot > in.snapshot:
		p.discardLocked(in, "part-missing")
		in.mu.Unlock()
		return
	}
	in.entries = append(in.entries, part.Entries...)
	in.bytes += len(body)
	in.next++
	p.gaugeInProgress(len(body))
	if !part.Last {
		in.mu.Unlock()
		return
	}
	entries, snapshot, cover, chain, whole := in.entries, in.snapshot, in.cover, in.chain, !in.broken
	p.gaugeInProgress(-in.bytes)
	in.active, in.entries, in.bytes = false, nil, 0
	in.mu.Unlock()
	p.applyFull(peer, entries, snapshot, cover, chain, whole, true)
}

// inSet is a peer's assembly state.
func (p *Plane) inSet(peer string) *inSet {
	p.inMu.Lock()
	defer p.inMu.Unlock()
	in := p.inSets[peer]
	if in == nil {
		in = &inSet{}
		p.inSets[peer] = in
	}
	return in
}

// dropPart counts a part dropped without touching the set in progress.
func (p *Plane) dropPart(peer string, in *inSet, reason string) {
	p.count("hoardmq_route_sets_abandoned_total", map[string]string{"side": "receive", "reason": reason}, 1)
	if in != nil {
		in.mu.Lock()
		in.why = reason
		in.mu.Unlock()
	}
	p.log.Debug("a part of a peer's route set dropped", "peer", peer, "reason", reason)
}

// discardLocked ends a set in progress from a peer, counted with its reason.
// Callers hold in.mu.
func (p *Plane) discardLocked(in *inSet, reason string) {
	if !in.active {
		return
	}
	p.gaugeInProgress(-in.bytes)
	in.active, in.entries, in.bytes, in.why = false, nil, 0, reason
	p.count("hoardmq_route_sets_abandoned_total", map[string]string{"side": "receive", "reason": reason}, 1)
}

// discardIn ends any set in progress from a peer: it went down, or was
// forgotten.
func (p *Plane) discardIn(peer, reason string) {
	p.inMu.Lock()
	in := p.inSets[peer]
	p.inMu.Unlock()
	if in == nil {
		return
	}
	in.mu.Lock()
	p.discardLocked(in, reason)
	in.mu.Unlock()
}

// supersedeIn discards a peer's set in progress older than a complete set
// that arrived in one frame.
func (p *Plane) supersedeIn(peer string, snapshot uint64) {
	p.inMu.Lock()
	in := p.inSets[peer]
	p.inMu.Unlock()
	if in == nil {
		return
	}
	in.mu.Lock()
	if in.active && in.snapshot < snapshot {
		p.discardLocked(in, "superseded")
	}
	in.mu.Unlock()
}

// chainDelta records a delta batch against a peer's set in progress: the
// chain from the set's cover is extended by the next number, a number at or
// below it is covered, and any other number breaks it.
func (p *Plane) chainDelta(peer string, number uint64) {
	p.inMu.Lock()
	in := p.inSets[peer]
	p.inMu.Unlock()
	if in == nil {
		return
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.active || in.broken {
		return
	}
	switch {
	case number != 0 && number <= in.chain:
	case number != 0 && number == in.chain+1:
		in.chain = number
	default:
		in.broken = true
	}
}

// appliedSnapshot is the snapshot of the last complete set applied from a
// peer.
func (p *Plane) appliedSnapshot(peer string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.applied[peer]
}

// applyFull applies a complete set from a peer, in either form: unless a set
// at or above its snapshot was applied already, it merges the entries,
// withdraws by absence, counts the sync, records the proof (whole up to last,
// covering cover, when whole) and the sync, and owes the peer a held for a
// set that came in parts.
func (p *Plane) applyFull(peer string, entries []Entry, snapshot, cover, last uint64, whole, parts bool) {
	for i := range entries {
		if entries[i].Owner == "" {
			entries[i].Owner = peer
		}
	}
	p.fullMu.Lock()
	defer p.fullMu.Unlock()
	p.mu.Lock()
	stale := snapshot <= p.applied[peer]
	if !stale {
		p.applied[peer] = snapshot
	}
	p.mu.Unlock()
	if stale {
		p.count("hoardmq_route_sets_abandoned_total", map[string]string{"side": "receive", "reason": "stale"}, 1)
		return
	}
	slice := p.partSize / 64
	if p.legacy() {
		slice = 0
	}
	started := time.Now()
	_, withdrawn, tombs := p.table.MergeFull(peer, entries, snapshot, slice)
	if p.legacy() {
		tombs = 0
	}
	if p.o.Metrics != nil {
		p.o.Metrics.Set("hoardmq_route_set_apply_seconds", nil, time.Since(started).Seconds())
	}
	p.count("hoardmq_route_syncs_total", map[string]string{"direction": "in"}, 1)
	if n := len(withdrawn); n > 0 {
		p.count("hoardmq_routes_withdrawn_total", map[string]string{"reason": "absent-from-sync"}, float64(n))
		p.log.Info("routes withdrawn by a peer's sync", "peer", peer, "count", n)
	}
	if tombs > 0 {
		p.count("hoardmq_route_tombstones_removed_total", nil, float64(tombs))
	}
	// Recorded after the merge: the proof vouches for the entries.
	p.noteFull(peer, cover, last, whole)
	p.markSync(peer)
	if parts && !p.legacy() {
		p.mu.Lock()
		p.heldOwed[peer] = snapshot
		p.mu.Unlock()
		p.wakeFlusher()
	}
}

// receiveHeld records a peer's acknowledgement of a set it received in
// parts: the highest is kept.
func (p *Plane) receiveHeld(peer string, body []byte) {
	var h setHeld
	if err := json.Unmarshal(body, &h); err != nil || h.Snapshot == 0 {
		p.count("hoardmq_route_updates_dropped_total", map[string]string{"reason": "malformed"}, 1)
		return
	}
	p.mu.Lock()
	if h.Snapshot > p.acks[peer] {
		p.acks[peer] = h.Snapshot
		p.ackAt[peer] = time.Now()
		p.pruneDue = true
	}
	p.mu.Unlock()
}

// sendHelds sends every held owed, from the flusher.
func (p *Plane) sendHelds() {
	p.mu.Lock()
	owed := p.heldOwed
	p.heldOwed = map[string]uint64{}
	p.mu.Unlock()
	for peer, snapshot := range owed {
		body, _ := json.Marshal(setHeld{Snapshot: snapshot})
		_ = p.sendRoute(peer, "held", append([]byte{routeHeld}, body...))
	}
}

// pruneBelow decides how far this node's own tombstones may be pruned: below
// the lowest snapshot that every member other than self has acknowledged. A
// membership that cannot be read (nil or empty), or a member with no
// acknowledgement, prunes nothing. It is pure.
func pruneBelow(members []string, self string, acks map[string]uint64) (uint64, bool) {
	var low uint64
	any := false
	for _, m := range members {
		if m == self {
			continue
		}
		a := acks[m]
		if a == 0 {
			return 0, false
		}
		if !any || a < low {
			low = a
		}
		any = true
	}
	return low, any
}

// maybePrune prunes this node's own tombstones once every member has
// acknowledged a set past them.
func (p *Plane) maybePrune() {
	if p.legacy() || p.o.Members == nil {
		return
	}
	p.mu.Lock()
	due := p.pruneDue
	p.pruneDue = false
	acks := make(map[string]uint64, len(p.acks))
	for k, v := range p.acks {
		acks[k] = v
	}
	p.mu.Unlock()
	if due {
		if below, ok := pruneBelow(p.o.Members(), p.o.Node, acks); ok {
			if n := p.table.PruneOwnTombstones(below); n > 0 {
				p.count("hoardmq_route_tombstones_pruned_total", nil, float64(n))
			}
		}
	}
	if p.o.Metrics != nil {
		p.o.Metrics.Set("hoardmq_route_tombstones", map[string]string{"scope": "own"}, float64(p.table.OwnTombstones()))
		now := time.Now()
		p.mu.Lock()
		for peer, at := range p.ackAt {
			p.o.Metrics.Set("hoardmq_route_set_held_age_seconds", map[string]string{"peer": peer}, now.Sub(at).Seconds())
		}
		p.mu.Unlock()
	}
}

// gaugeInProgress adjusts the bytes held in sets in progress.
func (p *Plane) gaugeInProgress(d int) {
	p.mu.Lock()
	p.inBytes += d
	v := p.inBytes
	p.mu.Unlock()
	if p.o.Metrics != nil {
		p.o.Metrics.Set("hoardmq_route_set_bytes_in_progress", nil, float64(v))
	}
}

// reportPeers sets each up peer's sync age and proof gauges and warns, at
// most once per warnEvery a peer, when its routes are not proved: no
// complete set for three sync intervals, or its proof broken for two.
func (p *Plane) reportPeers() {
	if p.o.Transport == nil {
		return
	}
	now := time.Now()
	for _, info := range p.o.Transport.Peers() {
		if info.State != transport.Up {
			continue
		}
		peer := info.Name
		p.mu.Lock()
		at, synced := p.lastSync[peer]
		if !synced {
			at = p.started
		}
		r := p.proofs[peer]
		whole := r != nil && r.whole
		brokeAt := p.brokeAt[peer]
		lastWarn := p.provedWarn[peer]
		p.mu.Unlock()
		age := now.Sub(at)
		if p.o.Metrics != nil {
			p.o.Metrics.Set("hoardmq_route_sync_age_seconds", map[string]string{"peer": peer}, age.Seconds())
			v := 0.0
			if whole {
				v = 1
			}
			p.o.Metrics.Set("hoardmq_route_proof_whole", map[string]string{"peer": peer}, v)
		}
		stuck := age > 3*p.o.SyncInterval || (!whole && !brokeAt.IsZero() && now.Sub(brokeAt) > 2*p.o.SyncInterval)
		if !stuck || (!lastWarn.IsZero() && now.Sub(lastWarn) < p.warnEvery) {
			continue
		}
		p.mu.Lock()
		p.provedWarn[peer] = now
		p.mu.Unlock()
		why := ""
		p.inMu.Lock()
		if in := p.inSets[peer]; in != nil {
			in.mu.Lock()
			why = in.why
			in.mu.Unlock()
		}
		p.inMu.Unlock()
		p.log.Warn("a peer's routes are not proved here: no complete set from it, or a gap in its updates since its last",
			"peer", peer, "syncAge", age.Round(time.Millisecond).String(), "proofWhole", whole, "lastAbandoned", why)
	}
}

// warnUnsendable logs the entries no frame can carry, at most once per
// warnEvery while any exist.
func (p *Plane) warnUnsendable(first *Entry) {
	p.mu.Lock()
	n := len(p.unsendable)
	due := n > 0 && (first != nil || p.unsendWarn.IsZero() || time.Since(p.unsendWarn) >= p.warnEvery)
	if first != nil && !p.unsendWarn.IsZero() && time.Since(p.unsendWarn) < p.warnEvery {
		due = false
	}
	if due {
		p.unsendWarn = time.Now()
	}
	p.mu.Unlock()
	if !due {
		return
	}
	attrs := []any{"entries", n, "limit", p.o.MaxFrame}
	if first != nil {
		head := first.Filter
		if len(head) > 32 {
			head = head[:32]
		}
		attrs = append(attrs, "namespace", first.Namespace, "filterBytes", len(first.Filter), "filterStart", fmt.Sprintf("%q", head))
	}
	p.log.Warn("route entries no cluster frame can carry are left out of route sync; the other nodes do not route those filters here", attrs...)
}
