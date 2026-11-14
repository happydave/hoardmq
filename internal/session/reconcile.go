package session

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

// The reconciler compares the sessions this node holds, its router and its
// own advertised routes, per session object and filter, and corrects a
// difference only when the next pass sees it again with the same evidence.
// Once two of the three disagree nothing else notices, and a missing route or a
// subscriber left for a session no map holds lasts until the process restarts.
//
// A pass is three walks: H, every held session's filters against the router;
// D, every router subscriber against the sessions held and, with a routing
// plane, each filter's own entry against the router; R, every live own entry
// against the router. A correction goes through the paths the live system
// uses, each decided by a pure predicate under the lock that orders the write,
// so a correction that lost a race to a live path writes nothing.
const (
	// reconcileInterval is the time between passes; a difference lasts at
	// most two of them and a pass, three when a router removal on the same
	// filter comes between its sightings.
	reconcileInterval = 60 * time.Second
	// reconcilePaced is the share of the interval a pass's steps are spread
	// over, leaving the rest for a pass that runs long.
	reconcilePaced = 0.8
	// reconcileWarnCap is how many corrections a pass logs one by one.
	reconcileWarnCap = 100
	// reconcileSkipStreak is how many consecutive passes a filter is skipped
	// for before it is reported: a move or import mark that never clears.
	reconcileSkipStreak = 5
	// reconcileListStep is how many own filters a walk-R step reads.
	reconcileListStep = 4096
	filterLogBytes    = 128
)

// reconcileStepBounds are the step-duration histogram's bounds, in seconds.
var reconcileStepBounds = []float64{0.0001, 0.001, 0.01, 0.1, 1}

// OwnRoute is this node's own route entry for a filter as the reconciler reads
// it, with the filter's move state here: Skip is "held" (a withdrawal waiting
// out a move), "moving", "ending" (a move other sessions keep alive), or empty.
type OwnRoute struct {
	Exists  bool
	Live    bool
	Version uint64
	Count   int
	Skip    string
}

// OwnRouteCorrection is an own-route write and the evidence it stands on.
// Count 0 merges a tombstone. FromNotLive is an announce from an entry that was
// not live at the first sighting: it stands while the entry is still not live
// at a version at most Version, absent included. Otherwise it stands only while
// the entry is at exactly Version, live as Live says, with Count of EvidenceCount
// when live.
type OwnRouteCorrection struct {
	Namespace     string
	Filter        string
	Count         int
	FromNotLive   bool
	Version       uint64
	Live          bool
	EvidenceCount int
}

// OwnRoutes is the routing plane as the reconciler reads and corrects it.
type OwnRoutes interface {
	OwnRoute(ns, filter string) OwnRoute
	// LiveOwnFilters calls add for every live own entry under one read hold
	// of the route table, and returns how long it held it.
	LiveOwnFilters(add func(ns, filter string)) time.Duration
	// CorrectOwn writes c if its predicates stand, and reports whether it
	// merged, or the reason it did not.
	CorrectOwn(c OwnRouteCorrection) (merged bool, refused string)
}

// The kinds of difference.
const (
	kindHeldUndispatched       = iota + 1 // a held session's filter missing from the router
	kindDispatchedUnheld                  // a router subscriber not held with that filter
	kindUnadvertised                      // a dispatched filter whose own entry is not live
	kindCount                             // a group whose own count differs from its members
	kindAdvertisedUndispatched            // a live own entry with no router subscriber
)

var kindNames = map[int]string{
	kindHeldUndispatched:       "held-undispatched",
	kindDispatchedUnheld:       "dispatched-unheld",
	kindUnadvertised:           "unadvertised",
	kindCount:                  "count",
	kindAdvertisedUndispatched: "advertised-undispatched",
}

type suspectKey struct {
	kind   int
	ns     string
	filter string
	obj    *Session // K1 and K2
}

// evidence is what a sighting saw; a confirmation compares it with the next.
type evidence struct {
	exists  bool
	live    bool
	version uint64
	count   int // the own entry's count
	members int // the router's subscribers: 1 for any on a direct filter
}

// sameEvidence decides whether the second sighting of a suspect saw what the
// first did. An announce from an entry that was not live also accepts that
// entry's tombstone pruned: pruning removes only a tombstone, and own versions
// only increase. It is pure.
func sameEvidence(kind int, first, second evidence) bool {
	switch kind {
	case kindHeldUndispatched, kindDispatchedUnheld:
		return true
	case kindUnadvertised:
		return !first.live && !second.live && (!second.exists || second.version == first.version) && first.members == second.members
	default:
		return first == second
	}
}

// k1Refusal decides a K1 correction, the router add: it stands only while the
// object is the map's for its key, the key is not importing, the session is not
// removed and still holds the filter. It returns "" when it stands. It is pure.
func k1Refusal(isMapObject, importing, removed, holds bool) string {
	switch {
	case !isMapObject:
		return "replaced"
	case importing:
		return "importing"
	case !mayRoute(removed, nil, nil):
		return "removed"
	case !holds:
		return "unsubscribed"
	}
	return ""
}

// k2Refusal decides a K2 correction, the router removal: it stands when the
// object is not the map's for its key while the key is not importing, or is the
// map's and does not hold the filter. A subscription the object holds while a
// connection is attached is a connected client's, which the reconciler never
// removes. It returns "" when it stands. It is pure.
func k2Refusal(isMapObject, importing, holds, connected bool) string {
	switch {
	case !isMapObject && importing:
		return "importing"
	case isMapObject && holds:
		return "now-held"
	case holds && connected:
		return "connected"
	}
	return ""
}

type reconciler struct {
	m     *Manager
	index RouterIndex
	own   OwnRoutes

	// mu makes one pass at a time.
	mu         sync.Mutex
	suspects   map[suspectKey]evidence
	skipStreak map[routeKey]int
	skipWarnAt time.Time
	lastSteps  int

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// pass is one pass's working state.
type pass struct {
	paced      bool
	steps      int
	estimate   int
	next       map[suspectKey]evidence
	seen       map[suspectKey]bool
	skipped    map[routeKey]bool
	warns      int
	suppressed map[string]int
	longest    time.Duration
}

// newReconciler returns nil when the router has no index to read.
func newReconciler(m *Manager) *reconciler {
	idx, ok := m.router.(RouterIndex)
	if !ok {
		return nil
	}
	return &reconciler{m: m, index: idx, own: m.opts.OwnRoutes, suspects: map[suspectKey]evidence{},
		skipStreak: map[routeKey]int{}, stop: make(chan struct{}), done: make(chan struct{})}
}

// StartReconciler starts this node's reconciler, a pass every interval. The
// caller starts it once the store is loaded and the routing plane, if any,
// has been told so. A router without RouterIndex leaves it off, counted.
func (m *Manager) StartReconciler() {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	if m.closed || m.reconciling {
		return
	}
	r := m.reconcilerFor()
	if r == nil {
		m.count("hoardmq_route_reconcile_unavailable_total", nil, 1)
		m.log.Warn("the route reconciler is off: the router cannot be walked")
		return
	}
	m.reconciling = true
	go r.loop()
}

// reconcilerFor returns the reconciler ReconcilePass and the loop share.
func (m *Manager) reconcilerFor() *reconciler {
	m.reconcileOnce.Do(func() { m.reconciler = newReconciler(m) })
	return m.reconciler
}

// StopReconciler ends the reconciler's loop and waits for a pass in progress
// to stop at its next step. The routing plane it reads is stopped after it.
func (m *Manager) StopReconciler() {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.stopReconcilerLocked()
}

// stopReconcilerLocked is StopReconciler with the lifecycle mutex held.
func (m *Manager) stopReconcilerLocked() {
	if m.reconciling {
		m.reconciler.stopOnce.Do(func() { close(m.reconciler.stop) })
		<-m.reconciler.done
	}
}

// ReconcilePass runs one pass at once, unpaced, and reports whether the router
// can be reconciled. Tests run passes with it rather than waiting on the
// interval.
func (m *Manager) ReconcilePass() bool {
	r := m.reconcilerFor()
	if r == nil {
		return false
	}
	r.run(false)
	return true
}

func (r *reconciler) loop() {
	defer close(r.done)
	t := time.NewTimer(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
		}
		began := time.Now()
		r.run(true)
		next := reconcileInterval - time.Since(began)
		if next < 0 {
			next = 0
		}
		t.Reset(next)
	}
}

func (r *reconciler) stopped() bool {
	select {
	case <-r.stop:
		return true
	default:
		return false
	}
}

// run makes one pass.
func (r *reconciler) run(paced bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	began := time.Now()
	p := &pass{paced: paced, next: map[suspectKey]evidence{}, seen: map[suspectKey]bool{}, skipped: map[routeKey]bool{}, suppressed: map[string]int{}}

	var owned []routeKey
	if r.own != nil {
		hold := r.own.LiveOwnFilters(func(ns, filter string) { owned = append(owned, routeKey{ns, filter}) })
		p.hold(hold)
	}
	p.estimate = len(r.m.shards) + r.lastSteps + (len(owned)+reconcileListStep-1)/reconcileListStep
	if p.estimate < 1 {
		p.estimate = 1
	}

	for _, sh := range r.m.shards {
		if !r.step(p, func() { r.walkShard(p, sh) }) {
			return
		}
	}
	dSteps := 0
	for cursor, done := 0, false; !done; {
		var fs []RoutedFilter
		var hold time.Duration
		ok := r.step(p, func() {
			fs, cursor, done, hold = r.index.WalkStep(cursor)
			p.hold(hold)
			for _, f := range fs {
				r.walkFilter(p, f)
			}
		})
		dSteps++
		if !ok {
			return
		}
	}
	r.lastSteps = dSteps
	for i := 0; i < len(owned); i += reconcileListStep {
		end := i + reconcileListStep
		if end > len(owned) {
			end = len(owned)
		}
		slice := owned[i:end]
		if !r.step(p, func() {
			for _, k := range slice {
				r.walkOwn(p, k)
			}
		}) {
			return
		}
	}
	r.finish(p, began)
}

// step runs one step, timed, after the pacing wait; it reports false once the
// reconciler is stopped.
func (r *reconciler) step(p *pass, fn func()) bool {
	if p.paced && p.steps > 0 {
		wait := time.Duration(float64(reconcileInterval) * reconcilePaced / float64(p.estimate))
		select {
		case <-r.stop:
			return false
		case <-time.After(wait):
		}
	} else if r.stopped() {
		return false
	}
	p.steps++
	began := time.Now()
	fn()
	r.observeStep(time.Since(began))
	return true
}

func (p *pass) hold(d time.Duration) {
	if d > p.longest {
		p.longest = d
	}
}

// walkShard is walk H's step for one shard.
func (r *reconciler) walkShard(p *pass, sh *shard) {
	sh.mu.Lock()
	began := time.Now()
	sessions := make([]*Session, 0, len(sh.sessions))
	for _, s := range sh.sessions {
		sessions = append(sessions, s)
	}
	importing := make(map[key]bool, len(sh.importing))
	for k := range sh.importing {
		importing[k] = true
	}
	p.hold(time.Since(began))
	sh.mu.Unlock()
	for _, s := range sessions {
		s.mu.Lock()
		removed := s.removed
		filters := make([]string, 0, len(s.subs))
		for f := range s.subs {
			filters = append(filters, f)
		}
		s.mu.Unlock()
		if removed {
			continue
		}
		for _, f := range filters {
			if r.index.Holds(s, f) {
				continue
			}
			if importing[s.key] {
				r.skip(p, routeKey{s.key.ns, f}, "importing")
				continue
			}
			if r.moveSkip(p, s.key.ns, f) {
				continue
			}
			k := suspectKey{kind: kindHeldUndispatched, ns: s.key.ns, filter: f, obj: s}
			if r.sight(p, k, evidence{}) {
				r.correctRouter(p, k)
			}
		}
	}
}

// walkFilter is walk D's work for one filter a step copied.
func (r *reconciler) walkFilter(p *pass, f RoutedFilter) {
	differs := false
	for _, s := range f.Subs {
		sh := r.m.shardFor(s.key)
		sh.mu.Lock()
		isMap := sh.sessions[s.key] == s
		importing := sh.importing[s.key] != nil
		sh.mu.Unlock()
		holds := false
		if isMap {
			s.mu.Lock()
			_, holds = s.subs[f.Filter]
			s.mu.Unlock()
		}
		if isMap && holds {
			continue
		}
		differs = true
		if importing {
			r.skip(p, routeKey{f.Namespace, f.Filter}, "importing")
			continue
		}
		if r.moveSkip(p, f.Namespace, f.Filter) {
			continue
		}
		k := suspectKey{kind: kindDispatchedUnheld, ns: f.Namespace, filter: f.Filter, obj: s}
		if r.sight(p, k, evidence{}) {
			r.correctRouter(p, k)
		}
	}
	// A subscriber not held is settled first: its removal's own notification
	// usually settles the route, and a route judged beside it would follow the
	// leftover.
	if differs || r.own == nil || len(f.Subs) == 0 {
		return
	}
	own := r.own.OwnRoute(f.Namespace, f.Filter)
	members := 1
	if f.Group {
		members = len(f.Subs)
	}
	ev := evidence{exists: own.Exists, live: own.Live, version: own.Version, count: own.Count, members: members}
	kind := 0
	switch {
	case !own.Live:
		kind = kindUnadvertised
	case f.Group && own.Count != members:
		kind = kindCount
	}
	if kind == 0 {
		return
	}
	if own.Skip != "" {
		r.skip(p, routeKey{f.Namespace, f.Filter}, own.Skip)
		return
	}
	k := suspectKey{kind: kind, ns: f.Namespace, filter: f.Filter}
	if first, ok := r.confirmed(p, k, ev); ok {
		r.correctOwn(p, k, first, ev)
	}
}

// walkOwn is walk R's work for one live own entry the pass listed.
func (r *reconciler) walkOwn(p *pass, k routeKey) {
	own := r.own.OwnRoute(k.ns, k.filter)
	if !own.Live || r.index.SubscriberCount(k.ns, k.filter) > 0 {
		return
	}
	if own.Skip != "" {
		r.skip(p, k, own.Skip)
		return
	}
	sk := suspectKey{kind: kindAdvertisedUndispatched, ns: k.ns, filter: k.filter}
	ev := evidence{exists: true, live: true, version: own.Version, count: own.Count}
	if first, ok := r.confirmed(p, sk, ev); ok {
		r.correctOwn(p, sk, first, ev)
	}
}

// moveSkip reports, and counts, a filter moving on this node.
func (r *reconciler) moveSkip(p *pass, ns, filter string) bool {
	if r.own == nil {
		return false
	}
	if why := r.own.OwnRoute(ns, filter).Skip; why != "" {
		r.skip(p, routeKey{ns, filter}, why)
		return true
	}
	return false
}

func (r *reconciler) skip(p *pass, k routeKey, why string) {
	p.skipped[k] = true
	r.m.count("hoardmq_route_reconcile_skipped_total", map[string]string{"reason": why}, 1)
}

// sight records a sighting and reports whether it confirms the previous pass's
// suspect.
func (r *reconciler) sight(p *pass, k suspectKey, ev evidence) bool {
	_, ok := r.confirmed(p, k, ev)
	return ok
}

// confirmed is sight, also returning the first sighting's evidence.
func (r *reconciler) confirmed(p *pass, k suspectKey, ev evidence) (evidence, bool) {
	first, had := r.suspects[k]
	if had {
		p.seen[k] = true
		if sameEvidence(k.kind, first, ev) {
			return first, true
		}
	}
	p.next[k] = ev
	return evidence{}, false
}

// correctRouter makes a confirmed K1 or K2 correction: a router add or removal
// for the object, decided under the session's mutex.
func (r *reconciler) correctRouter(p *pass, k suspectKey) {
	s := k.obj
	if r.m.reconcileBefore != nil {
		r.m.reconcileBefore(kindNames[k.kind], k.ns, k.filter, s)
	}
	sh := r.m.shardFor(s.key)
	sh.mu.Lock()
	isMap := sh.sessions[s.key] == s
	importing := sh.importing[s.key] != nil
	sh.mu.Unlock()
	if r.m.reconcileBetween != nil {
		r.m.reconcileBetween(kindNames[k.kind], k.ns, k.filter, s)
	}
	s.mu.Lock()
	began := time.Now()
	sub, holds := s.subs[k.filter]
	var why string
	if k.kind == kindHeldUndispatched {
		why = k1Refusal(isMap, importing, s.removed, holds)
		if why == "" && r.m.router.Add(s, sub) {
			why = "settled"
		}
	} else {
		why = k2Refusal(isMap, importing, holds, s.conn != nil)
		if why == "" && !r.m.router.Remove(s, k.filter) {
			why = "settled"
		}
	}
	p.hold(time.Since(began))
	s.mu.Unlock()
	if r.m.reconcileAfter != nil {
		r.m.reconcileAfter(kindNames[k.kind], k.ns, k.filter, s, why == "")
	}
	direction := "announce"
	if k.kind == kindDispatchedUnheld {
		direction = "withdraw"
	}
	r.outcome(p, k, direction, s.key.id, why)
}

// correctOwn makes a confirmed K3 or K4 correction through the plane.
func (r *reconciler) correctOwn(p *pass, k suspectKey, first, second evidence) {
	c := OwnRouteCorrection{Namespace: k.ns, Filter: k.filter, Version: second.version, Live: second.live, EvidenceCount: second.count}
	direction := "announce"
	switch k.kind {
	case kindUnadvertised:
		c.FromNotLive, c.Version, c.Count = true, first.version, second.members
	case kindCount:
		c.Count = second.members
		if second.members < second.count {
			direction = "withdraw"
		}
	case kindAdvertisedUndispatched:
		direction = "withdraw"
	}
	if r.m.reconcileBefore != nil {
		r.m.reconcileBefore(kindNames[k.kind], k.ns, k.filter, nil)
	}
	merged, why := r.own.CorrectOwn(c)
	if merged {
		why = ""
	} else if why == "" {
		why = "refused"
	}
	if r.m.reconcileAfter != nil {
		r.m.reconcileAfter(kindNames[k.kind], k.ns, k.filter, nil, merged)
	}
	r.outcome(p, k, direction, "", why)
}

// outcome counts and logs a correction made, or one a live path overtook.
func (r *reconciler) outcome(p *pass, k suspectKey, direction, client, refused string) {
	kind := kindNames[k.kind]
	if refused == "connected" {
		// A split the net cannot mend without taking a subscription from a
		// connected client.
		r.m.count("hoardmq_route_reconcile_refused_total", map[string]string{"kind": kind, "reason": refused}, 1)
		if p.warns >= reconcileWarnCap {
			p.suppressed["refused-connected "+kind]++
			r.m.count("hoardmq_route_reconcile_warns_suppressed_total", nil, 1)
			return
		}
		p.warns++
		r.m.log.Warn("route reconciler left a connected client's subscriber that no map holds", "kind", kind, "namespace", k.ns, "filterBytes", len(k.filter), "filterStart", logFilter(k.filter), "client", client)
		return
	}
	if refused != "" {
		r.m.count("hoardmq_route_reconcile_refused_total", map[string]string{"kind": kind, "reason": refused}, 1)
		r.m.log.Debug("route reconciler correction refused: a live path changed it first", "kind", kind, "reason", refused, "namespace", k.ns, "filter", logFilter(k.filter))
		return
	}
	r.m.count("hoardmq_route_reconcile_corrections_total", map[string]string{"direction": direction, "kind": kind}, 1)
	if p.warns >= reconcileWarnCap {
		p.suppressed[direction+" "+kind]++
		r.m.count("hoardmq_route_reconcile_warns_suppressed_total", nil, 1)
		return
	}
	p.warns++
	attrs := []any{"direction", direction, "kind", kind, "namespace", k.ns, "filterBytes", len(k.filter), "filterStart", logFilter(k.filter)}
	if client != "" {
		attrs = append(attrs, "client", client)
	}
	r.m.log.Warn("route reconciler corrected a difference seen at two passes", attrs...)
}

func logFilter(f string) string {
	if len(f) > filterLogBytes {
		f = f[:filterLogBytes]
	}
	return fmt.Sprintf("%q", f)
}

// finish ends a pass: suspects not seen again are cleared, the next pass's
// suspects kept, and the pass's figures set.
func (r *reconciler) finish(p *pass, began time.Time) {
	for k := range r.suspects {
		if !p.seen[k] {
			r.m.count("hoardmq_route_reconcile_cleared_total", map[string]string{"kind": kindNames[k.kind]}, 1)
			r.m.log.Debug("route reconciler suspect not seen again", "kind", kindNames[k.kind], "namespace", k.ns, "filter", logFilter(k.filter))
		}
	}
	r.suspects = p.next
	if len(p.suppressed) > 0 {
		keys := make([]string, 0, len(p.suppressed))
		for k := range p.suppressed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		attrs := make([]any, 0, 2*len(keys))
		for _, k := range keys {
			attrs = append(attrs, k, p.suppressed[k])
		}
		r.m.log.Warn("route reconciler corrections and connected refusals past the per-pass log limit, by direction and kind", attrs...)
	}
	for k := range r.skipStreak {
		if !p.skipped[k] {
			delete(r.skipStreak, k)
		}
	}
	lasting := 0
	var first routeKey
	for k := range p.skipped {
		r.skipStreak[k]++
		if r.skipStreak[k] >= reconcileSkipStreak {
			if lasting == 0 || k.ns < first.ns || (k.ns == first.ns && k.filter < first.filter) {
				first = k
			}
			lasting++
		}
	}
	if lasting > 0 && (r.skipWarnAt.IsZero() || time.Since(r.skipWarnAt) >= time.Hour) {
		r.skipWarnAt = time.Now()
		r.m.log.Warn("route reconciler has skipped filters at five consecutive passes: a move or import mark that does not clear", "filters", lasting, "namespace", first.ns, "filterStart", logFilter(first.filter))
	}
	r.m.set("hoardmq_route_reconcile_skipped_persistently", nil, float64(lasting))
	r.m.set("hoardmq_route_reconcile_suspects", nil, float64(len(r.suspects)))
	r.m.count("hoardmq_route_reconcile_passes_total", nil, 1)
	r.m.set("hoardmq_route_reconcile_pass_seconds", nil, time.Since(began).Seconds())
	r.m.set("hoardmq_route_reconcile_hold_seconds", nil, p.longest.Seconds())
}

// observeStep counts one step's duration in the step histogram.
func (r *reconciler) observeStep(d time.Duration) {
	sec := d.Seconds()
	for _, le := range reconcileStepBounds {
		if sec <= le {
			r.m.count("hoardmq_route_reconcile_step_seconds_bucket", map[string]string{"le": strconv.FormatFloat(le, 'g', -1, 64)}, 1)
		}
	}
	r.m.count("hoardmq_route_reconcile_step_seconds_bucket", map[string]string{"le": "+Inf"}, 1)
	r.m.count("hoardmq_route_reconcile_step_seconds_sum", nil, sec)
	r.m.count("hoardmq_route_reconcile_step_seconds_count", nil, 1)
}

// set sets a gauge when a provider is configured.
func (m *Manager) set(name string, labels map[string]string, value float64) {
	if m.opts.Metrics != nil {
		m.opts.Metrics.Set(name, labels, value)
	}
}
