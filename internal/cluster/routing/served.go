package routing

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// A down peer's route entries are hidden, and a QoS 1 or 2 publish matching
// one is refused: the peer's subscriber cannot receive it. What ends that is
// not the peer's sessions being owned elsewhere but their being served
// elsewhere. A reassignment moves every lease the moment it applies; the
// adopter imports each session afterwards, from a queue, and only the import
// advertises the session's routes. Forgetting the lost owner's entries in
// between would leave a publish matching nothing anywhere, acknowledged with
// no subscriber. So an owner is forgotten only once the lease table shows it
// owning nothing, and every member that owns leases has put in the lease log
// that its adoptions up to an index at or past the one the owner was emptied
// at are finished, and this node holds that member's routes as of then
// (routeproof.go).

// servedSelf is this node's own part: the lease index its change hooks have
// run to, and the adoptions it has not finished.
type servedSelf struct {
	index    uint64
	adopting int
}

// servedMember is what this node knows about one other member when it judges
// whether a lost owner's sessions are served again.
type servedMember struct {
	name   string
	owns   int    // leases the table shows it owning
	up     bool   // the transport has it connected
	served uint64 // the index its served fact in the log names; zero for none
	routes bool   // this node holds its route updates up to its fact's mark
}

// Reasons a hidden owner's entries are kept.
const (
	keepUnknown     = "unknown"     // this node cannot read the lease state
	keepLeases      = "leases"      // the owner still owns leases
	keepAdopting    = "adopting"    // this node has an adoption unfinished
	keepUnsynced    = "unsynced"    // this node, or a member it can reach, has not caught up: a fact or routes missing
	keepUnreachable = "unreachable" // a member that owns leases is down and this node lacks its fact or routes
)

// servedElsewhere decides whether a hidden owner's entries may be forgotten.
// zeroAt is the lease log index the owner was emptied at. A member that owns
// nothing is not consulted: an adopter owns what it adopts from the moment
// the reassignment applies, so it can be adopting nothing, and waiting on it
// would let a second lost node hold the first one's refusal for good. A
// member that is down but whose fact and routes are held is not waited for:
// its own entries here are hidden and refuse on their own account.
func servedElsewhere(owns int, known bool, zeroAt uint64, self servedSelf, members []servedMember) (bool, string) {
	switch {
	case !known:
		return false, keepUnknown
	case owns > 0:
		return false, keepLeases
	case zeroAt == 0:
		return false, keepUnknown
	}
	if ok, why := servedHere(zeroAt, self); !ok {
		return false, why
	}
	for _, m := range members {
		switch {
		case m.owns == 0:
			continue
		case m.served >= zeroAt && m.routes:
			continue
		case !m.up:
			return false, keepUnreachable
		default:
			return false, keepUnsynced
		}
	}
	return true, ""
}

// ownerRoutesKnown decides whether the routes of an owner this node has had no
// full sync from can matter to a publish here. An owner holding a lease may
// have a subscriber for any topic. One that holds none was emptied by a
// reassignment or a takeover, and its sessions have no route anywhere until
// their new owners import them: until servedElsewhere says they are served,
// with their routes here, a publish this node cannot match may be one of
// theirs. emptiedAt zero means nothing was ever taken from it;
// settledAt is the emptied index at which this node last found it served.
func ownerRoutesKnown(owns int, known bool, emptiedAt, settledAt uint64, self servedSelf, members []servedMember) (bool, string) {
	switch {
	case !known:
		return false, keepUnknown
	case owns > 0:
		return false, keepLeases
	case emptiedAt == 0, settledAt >= emptiedAt:
		return true, ""
	}
	return servedElsewhere(owns, known, emptiedAt, self, members)
}

// servedHere is this node's own part of the question, and what it puts in
// the log once true: its hooks have run up to the index and it has no
// adoption unfinished.
func servedHere(zeroAt uint64, self servedSelf) (bool, string) {
	switch {
	case self.index < zeroAt:
		return false, keepUnsynced
	case self.adopting > 0:
		return false, keepAdopting
	}
	return true, ""
}

// adoptionState reads this node's own state; index zero when it cannot.
func (p *Plane) adoptionState() (uint64, int) {
	if p.o.AdoptionState == nil {
		return 0, 0
	}
	index, unfinished, known := p.o.AdoptionState()
	if !known {
		return 0, 0
	}
	return index, unfinished
}

// WakeServed asks for a served check soon: a fact was applied, this node's
// adoptions ended, or a member's routes arrived. It never blocks.
func (p *Plane) WakeServed() {
	select {
	case p.servedWake <- struct{}{}:
	default:
	}
}

// servedLoop judges the hidden owners on a short tick, and sooner when woken,
// but no closer together than a tenth of the tick: a storm's adoptions can
// end and begin many times a second, and each check counts leases under the
// lease table's lock.
func (p *Plane) servedLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.o.ServedInterval)
	defer ticker.Stop()
	gap := p.o.ServedInterval / 10
	var last time.Time
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
		case <-p.servedWake:
			if d := gap - time.Since(last); d > 0 {
				select {
				case <-p.stop:
					return
				case <-time.After(d):
				}
			}
		}
		p.checkServed()
		last = time.Now()
	}
}

// servedProposeTimeout bounds one proposal of this node's served fact; a
// failed one is proposed again at the next check.
const servedProposeTimeout = 10 * time.Second

// checkServed forgets every hidden owner whose sessions are served again, and
// puts this node's own served fact in the log once it is of use to the
// others: each survivor waits on every other member that owns leases,
// adopter or not.
func (p *Plane) checkServed() {
	p.servedMu.Lock()
	defer p.servedMu.Unlock()
	// A fact or a member's routes arriving can end an unknown owner's
	// refusal; nothing else re-judges it before the next peer event or sync.
	defer func() {
		if p.unknownOwners.Load() {
			p.refreshOwnerKnowledge()
		}
	}()
	// Before the return for no hidden owner: a peer's hand-ons wait on this
	// node's cover whether or not an owner is hidden here, and the others'
	// refusals for an owner this node never had routes from wait on its fact.
	p.proposeCover()
	p.proposeForUnknown()
	hidden := p.table.HiddenOwners()
	now := time.Now().UnixNano()
	p.forgets.hidden(hidden, now)
	p.mu.Lock()
	for owner := range p.zeroAt {
		if !contains(hidden, owner) {
			delete(p.zeroAt, owner)
		}
	}
	if len(hidden) == 0 {
		clear(p.awaitRoutes)
	}
	p.mu.Unlock()
	held := map[string]int{}
	defer func() { p.gaugeHidden(held) }()
	if len(hidden) == 0 || p.o.OwnedSessions == nil || p.o.AdoptionState == nil || p.o.LeaseIndex == nil ||
		p.o.ServedFact == nil || p.o.ProposeServed == nil {
		for _, owner := range hidden {
			held[keepUnknown]++
			p.forgets.held(owner, keepUnknown, now)
		}
		return
	}
	selfIndex, selfAdopting := p.adoptionState()
	self := servedSelf{index: selfIndex, adopting: selfAdopting}
	for _, owner := range hidden {
		owns, known := p.o.OwnedSessions(owner)
		p.mu.Lock()
		if !known || owns > 0 {
			delete(p.zeroAt, owner)
		} else if p.zeroAt[owner] == 0 {
			// The index the owner was emptied at, from the lease table: the
			// same on every node. The fallback is this node's applied index,
			// never its hooked one — that trails the state, and a count read
			// at zero before the reassignment's hooks have run would be paired
			// with an index this node and every peer already looks idle at.
			if p.o.EmptiedAt != nil {
				p.zeroAt[owner] = p.o.EmptiedAt(owner)
			}
			if p.zeroAt[owner] == 0 {
				p.zeroAt[owner] = p.o.LeaseIndex()
			}
		}
		zeroAt := p.zeroAt[owner]
		p.mu.Unlock()
		if zeroAt != 0 {
			p.forgets.note(owner, now, func(e *ForgetEvent) {
				if e.EmptiedNs == 0 {
					e.EmptiedNs = now
				}
				e.emptiedAt = zeroAt
			})
			if here, _ := servedHere(zeroAt, self); here {
				p.forgets.note(owner, now, func(e *ForgetEvent) {
					if e.ServedHereNs == 0 {
						e.ServedHereNs = now
					}
				})
				p.proposeServed(zeroAt, self.index)
			}
		}
		// The members are read only once everything nearer is settled: each
		// costs a count over the lease table, under the lock a takeover storm
		// starves, and a storm is when an owner is hidden.
		ok, why := servedElsewhere(owns, known, zeroAt, self, nil)
		if ok {
			ok, why = servedElsewhere(owns, known, zeroAt, self, p.servedMembers(owner, zeroAt))
		}
		if !ok {
			held[why]++
			p.forgets.held(owner, why, now)
			continue
		}
		p.table.Forget(owner)
		p.forgets.forgotten(owner, now)
		p.mu.Lock()
		delete(p.lastSync, owner)
		delete(p.zeroAt, owner)
		p.mu.Unlock()
		p.count("hoardmq_route_owners_forgotten_total", map[string]string{"reason": "sessions-served"}, 1)
		p.log.Info("a lost owner's routes forgotten: its sessions are served elsewhere", "owner", owner, "leaseIndex", zeroAt)
		p.refreshOwnerKnowledge()
		p.gauge()
	}
}

// proposeServed puts this node's served fact in the log, at the index its
// hooks have run to, unless a fact at or past zeroAt is already there or on
// its way. The route mark is read here, after the adoption count the caller
// read: every route an adoption counted as finished advertised is queued by
// then. One proposal is in flight at a time.
func (p *Plane) proposeServed(zeroAt, index uint64) {
	need := zeroAt
	if p.o.AdoptedThrough != nil {
		need = max(need, p.o.AdoptedThrough())
	}
	if own, _ := p.o.ServedFact(p.o.Node); own >= need || index < need {
		return
	}
	p.mu.Lock()
	if p.proposing || p.servedProposed >= need {
		p.mu.Unlock()
		return
	}
	p.proposing = true
	mark := p.routeMarkLocked()
	p.mu.Unlock()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ctx, cancel := context.WithTimeout(p.ctx, servedProposeTimeout)
		err := p.o.ProposeServed(ctx, index, mark)
		cancel()
		// The outcome is recorded before proposing clears: whoever finds no
		// proposal in flight finds it counted and, if committed, stamped.
		if err != nil {
			p.count("hoardmq_route_served_proposals_total", map[string]string{"outcome": "failed"}, 1)
			p.log.Debug("this node's served fact was not committed; proposed again at the next check", "index", index, "err", err)
		} else {
			p.count("hoardmq_route_served_proposals_total", map[string]string{"outcome": "committed"}, 1)
			p.forgets.proposed(index, time.Now().UnixNano())
		}
		p.mu.Lock()
		p.proposing = false
		if err == nil && index > p.servedProposed {
			p.servedProposed = index
		}
		p.mu.Unlock()
	}()
}

// proposeForUnknown puts this node's served fact in the log for each lost
// owner it has had no full sync from, as checkServed does for a hidden one:
// a node that restarted beside the owner has nothing of it to hide, and every
// other node's forget, and refusal, waits on this node's fact as on any
// member's. The lease table is counted only while a fact is owed.
func (p *Plane) proposeForUnknown() {
	if p.o.EmptiedAt == nil || p.o.OwnedSessions == nil || p.o.ServedFact == nil || p.o.ProposeServed == nil {
		return
	}
	own, _ := p.o.ServedFact(p.o.Node)
	var through uint64
	if p.o.AdoptedThrough != nil {
		through = p.o.AdoptedThrough()
	}
	for _, owner := range p.candidates() {
		if p.syncedFrom(owner) {
			continue
		}
		zeroAt := p.o.EmptiedAt(owner)
		if zeroAt == 0 || own >= max(zeroAt, through) {
			continue
		}
		if owns, known := p.o.OwnedSessions(owner); !known || owns > 0 {
			continue
		}
		index, adopting := p.adoptionState()
		if here, _ := servedHere(zeroAt, servedSelf{index: index, adopting: adopting}); here {
			p.proposeServed(zeroAt, index)
		}
	}
}

// proposeCover puts this node's cover in the log when the lease service says
// one is due. The route mark is read after the due state, so every route a
// held takeover advertised is at or below it. One proposal is in flight at a
// time.
func (p *Plane) proposeCover() {
	if p.o.CoverDue == nil || p.o.ProposeCover == nil {
		return
	}
	grant, due := p.o.CoverDue()
	if !due {
		return
	}
	p.mu.Lock()
	if p.coverProposing || p.coverProposed >= grant {
		p.mu.Unlock()
		return
	}
	p.coverProposing = true
	mark := p.routeMarkLocked()
	p.mu.Unlock()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ctx, cancel := context.WithTimeout(p.ctx, servedProposeTimeout)
		err := p.o.ProposeCover(ctx, grant, mark)
		cancel()
		outcome := "committed"
		if err != nil {
			outcome = "failed"
			p.log.Debug("this node's cover was not committed; proposed again at the next check", "grant", grant, "err", err)
		} else {
			p.forgets.covered(time.Now().UnixNano())
		}
		p.count("hoardmq_route_cover_proposals_total", map[string]string{"outcome": outcome}, 1)
		p.mu.Lock()
		p.coverProposing = false
		if err == nil && grant > p.coverProposed {
			p.coverProposed = grant
		}
		p.mu.Unlock()
	}()
}

// servedMembers is every candidate other than this node and the owner being
// judged, as this node sees it now. It notes, per member whose fact is in
// the log and whose routes are not yet held, the mark to wake on.
func (p *Plane) servedMembers(owner string, zeroAt uint64) []servedMember {
	up := map[string]bool{}
	if p.o.Transport != nil {
		for _, peer := range p.o.Transport.Peers() {
			up[peer.Name] = peer.State == transport.Up
		}
	}
	var out []servedMember
	for _, name := range p.candidates() {
		if name == owner {
			continue
		}
		m := servedMember{name: name, up: up[name]}
		m.owns, _ = p.o.OwnedSessions(name)
		var mark uint64
		m.served, mark = p.o.ServedFact(name)
		if p.o.Cover != nil {
			// The member's latest claim carries its highest mark: a cover can
			// follow its finished index.
			_, coverMark := p.o.Cover(name)
			mark = max(mark, coverMark)
		}
		m.routes = p.routesHeld(name, mark)
		p.mu.Lock()
		if m.served >= zeroAt && !m.routes {
			p.awaitRoutes[name] = mark
		} else {
			delete(p.awaitRoutes, name)
		}
		p.mu.Unlock()
		out = append(out, m)
	}
	return out
}

// gaugeHidden reports how many hidden owners are held, by why.
func (p *Plane) gaugeHidden(held map[string]int) {
	if p.o.Metrics == nil {
		return
	}
	for _, why := range []string{keepUnknown, keepLeases, keepAdopting, keepUnsynced, keepUnreachable} {
		p.o.Metrics.Set("hoardmq_route_owners_hidden", map[string]string{"reason": why}, float64(held[why]))
	}
}

// contains searches a sorted list, as HiddenOwners returns.
func contains(list []string, s string) bool {
	i := sort.SearchStrings(list, s)
	return i < len(list) && list[i] == s
}

// ForgetEvent is what this node's served checks saw of one hidden owner, in
// wall-clock Unix nanoseconds; zero means the step did not happen. Nothing
// reads it to decide.
type ForgetEvent struct {
	Owner string `json:"owner"`
	// HiddenNs is the first check that found the owner hidden, EmptiedNs the
	// first that knew the index it was emptied at, ServedHereNs the first
	// that found this node's own adoptions up to that index finished,
	// ProposedNs when this node's served fact covering it was committed, and
	// ForgottenNs when its routes were forgotten. ReturnedNs is the first
	// check that found it no longer hidden without this node having
	// forgotten it: it came back.
	HiddenNs     int64 `json:"hidden_ns,omitempty"`
	EmptiedNs    int64 `json:"emptied_ns,omitempty"`
	ServedHereNs int64 `json:"served_here_ns,omitempty"`
	ProposedNs   int64 `json:"proposed_ns,omitempty"`
	ForgottenNs  int64 `json:"forgotten_ns,omitempty"`
	ReturnedNs   int64 `json:"returned_ns,omitempty"`
	// CoveredNs is when this node's cover was last committed while the
	// owner was hidden.
	CoveredNs int64 `json:"covered_ns,omitempty"`
	// LastHeldNs is, per reason the owner was kept, the last check at which
	// that reason held.
	LastHeldNs map[string]int64 `json:"last_held_ns,omitempty"`
	emptiedAt  uint64
}

// forgetKept is how many ended events are kept beside the open ones.
const forgetKept = 8

type forgetTimeline struct {
	mu   sync.Mutex
	open map[string]*ForgetEvent
	done []ForgetEvent
}

func (f *forgetTimeline) event(owner string, now int64) *ForgetEvent {
	if f.open == nil {
		f.open = map[string]*ForgetEvent{}
	}
	e := f.open[owner]
	if e == nil {
		e = &ForgetEvent{Owner: owner, HiddenNs: now}
		f.open[owner] = e
	}
	return e
}

func (f *forgetTimeline) end(owner string) {
	e := f.open[owner]
	if e == nil {
		return
	}
	delete(f.open, owner)
	f.done = append(f.done, *e)
	if len(f.done) > forgetKept {
		f.done = f.done[len(f.done)-forgetKept:]
	}
}

// hidden notes every owner a check found hidden, and ends the events of
// those it no longer does.
func (f *forgetTimeline) hidden(owners []string, now int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for owner, e := range f.open {
		if !contains(owners, owner) {
			e.ReturnedNs = now
			f.end(owner)
		}
	}
	for _, owner := range owners {
		f.event(owner, now)
	}
}

func (f *forgetTimeline) note(owner string, now int64, step func(e *ForgetEvent)) {
	f.mu.Lock()
	step(f.event(owner, now))
	f.mu.Unlock()
}

func (f *forgetTimeline) held(owner, why string, now int64) {
	f.note(owner, now, func(e *ForgetEvent) {
		if e.LastHeldNs == nil {
			e.LastHeldNs = map[string]int64{}
		}
		e.LastHeldNs[why] = now
	})
}

// proposed stamps every event whose owner this node's committed fact covers:
// one served here, whose emptied index the fact reaches.
func (f *forgetTimeline) proposed(index uint64, now int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	covers := func(e *ForgetEvent) bool { return e.ServedHereNs != 0 && e.ProposedNs == 0 && e.emptiedAt <= index }
	for _, e := range f.open {
		if covers(e) {
			e.ProposedNs = now
		}
	}
	// This node's own fact does not hold up its own forget, so an event can
	// end before the proposal it made returns.
	for i := range f.done {
		if covers(&f.done[i]) {
			f.done[i].ProposedNs = now
		}
	}
}

// covered stamps every open event with the commit of this node's cover.
func (f *forgetTimeline) covered(now int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.open {
		e.CoveredNs = now
	}
}

func (f *forgetTimeline) forgotten(owner string, now int64) {
	f.mu.Lock()
	f.event(owner, now).ForgottenNs = now
	f.end(owner)
	f.mu.Unlock()
}

// ForgetTimeline reports the hidden owners this node's served checks have
// seen: the last few that ended, then those still hidden.
func (p *Plane) ForgetTimeline() []ForgetEvent {
	f := &p.forgets
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ForgetEvent, 0, len(f.done)+len(f.open))
	copyEvent := func(e ForgetEvent) ForgetEvent {
		if e.LastHeldNs != nil {
			held := make(map[string]int64, len(e.LastHeldNs))
			for k, v := range e.LastHeldNs {
				held[k] = v
			}
			e.LastHeldNs = held
		}
		return e
	}
	for _, e := range f.done {
		out = append(out, copyEvent(e))
	}
	open := make([]ForgetEvent, 0, len(f.open))
	for _, e := range f.open {
		open = append(open, copyEvent(*e))
	}
	sort.Slice(open, func(i, j int) bool { return open[i].Owner < open[j].Owner })
	return append(out, open...)
}
