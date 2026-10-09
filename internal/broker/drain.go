package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

// drainer runs a node's graceful leave: listeners close, connected clients
// are redirected at a bounded rate to live nodes, their takeovers are
// awaited, every lease still owned here is handed to one live node, the node
// leaves the shard map, and the drain then waits until each session it handed
// off is confirmed held by its receiver, or until the confirmation timeout.
// It runs once; later calls join.
type drainer struct {
	s *Server

	once     sync.Once
	mu       sync.Mutex
	status   module.DrainStatus
	started  time.Time
	drained  chan struct{}
	draining chan struct{}
	// owned is every lease this node owned while draining, at the token it
	// held it at; confirmed is the highest token each client's session was
	// confirmed held at, from any sender, since the drain began.
	owned     map[drainKey]uint64
	confirmed map[drainKey]uint64
}

type drainKey struct{ ns, id string }

func newDrainer(s *Server) *drainer {
	return &drainer{s: s, drained: make(chan struct{}), draining: make(chan struct{}),
		owned: map[drainKey]uint64{}, confirmed: map[drainKey]uint64{}}
}

// noteConfirmed records a new owner's confirmation that it holds a client's
// session at token. Nothing is recorded unless a drain is running.
func (d *drainer) noteConfirmed(ns, id string, token uint64) {
	if !d.s.Draining() {
		return
	}
	k := drainKey{ns, id}
	d.mu.Lock()
	if token > d.confirmed[k] {
		d.confirmed[k] = token
	}
	d.mu.Unlock()
}

// noteOwned records the leases this node owns now.
func (d *drainer) noteOwned() {
	s := d.s
	owned := s.leases.Table().OwnedBy(s.config().Node.Name)
	d.mu.Lock()
	for _, l := range owned {
		k := drainKey{l.Namespace, l.ClientID}
		if l.Epoch > d.owned[k] {
			d.owned[k] = l.Epoch
		}
	}
	d.mu.Unlock()
}

// drainKeySettled decides whether a drain may stop waiting for one session it
// handed off. It is pure: a drain that stops waiting lets the process exit,
// and the process holds the only copy of a session nobody has fetched. held
// is the token this node held the session at, confirmed the highest token a
// confirmation named for it (0 for none). Only a later grant's confirmation
// settles it; a session whose lease is gone has nothing left to hand over.
func drainKeySettled(held, confirmed uint64, leaseExists bool) bool {
	return confirmed > held || !leaseExists
}

// drainMayEnd decides whether the confirmation wait is over: every session
// settled, or the deadline reached. expired says the deadline ended it with
// sessions unconfirmed. It is pure, for the reason drainKeySettled is.
func drainMayEnd(unconfirmed int, now, deadline time.Time) (end, expired bool) {
	if unconfirmed == 0 {
		return true, false
	}
	if now.Before(deadline) {
		return false, false
	}
	return true, true
}

// Drain starts the node's graceful leave and returns at once; Drained
// closes when it is complete.
func (s *Server) Drain() {
	s.drainer.once.Do(func() {
		close(s.drainer.draining)
		s.ready.Store(false)
		go s.drainer.run()
	})
}

// Draining reports whether a drain has started.
func (s *Server) Draining() bool {
	select {
	case <-s.drainer.draining:
		return true
	default:
		return false
	}
}

// Drained closes when the drain is complete; the serve loop then shuts the
// process down.
func (s *Server) Drained() <-chan struct{} { return s.drainer.drained }

// DrainStatus reports the drain's progress.
func (s *Server) DrainStatus() module.DrainStatus {
	d := s.drainer
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.status
	st.Draining = s.Draining()
	return st
}

func (d *drainer) phase(name string) {
	d.mu.Lock()
	d.status.Phase = name
	d.mu.Unlock()
	d.s.record("server", "server.drain", d.s.config().Node.Name, name, nil)
	d.s.log.Info("drain", "phase", name)
}

func (d *drainer) setRemaining(clients, leases int) {
	d.mu.Lock()
	d.status.ClientsRemaining, d.status.LeasesRemaining = clients, leases
	d.mu.Unlock()
}

func (d *drainer) setSessions(awaiting, confirmed, unconfirmed int) {
	d.mu.Lock()
	d.status.SessionsAwaiting, d.status.SessionsConfirmed, d.status.SessionsUnconfirmed = awaiting, confirmed, unconfirmed
	d.mu.Unlock()
}

func (d *drainer) run() {
	s := d.s
	d.started = time.Now()
	cfg := s.config().Cluster.Drain
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	defer func() {
		d.mu.Lock()
		d.status.Complete = true
		d.status.Phase = "complete"
		confirmed, unconfirmed := d.status.SessionsConfirmed, d.status.SessionsUnconfirmed
		d.mu.Unlock()
		secs := time.Since(d.started).Seconds()
		s.count("hoardmq_drain_seconds_total", nil, secs)
		s.record("server", "server.drain", s.config().Node.Name, "complete", map[string]string{"seconds": fmt.Sprintf("%.1f", secs),
			"sessionsConfirmed": fmt.Sprint(confirmed), "sessionsUnconfirmed": fmt.Sprint(unconfirmed)})
		s.log.Info("drain complete", "seconds", secs, "sessionsConfirmed", confirmed, "sessionsUnconfirmed", unconfirmed)
		close(d.drained)
	}()
	if s.leases != nil {
		d.noteOwned()
	}

	// 1. Stop accepting.
	d.phase("closing-listeners")
	for _, l := range s.listeners {
		_ = l.Close()
	}

	// 2. Redirect connected clients at the configured rate, or faster where
	// that is what ends the redirect by its deadline.
	d.phase("redirecting-clients")
	s.mu.Lock()
	conns := make([]*conn.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	sort.Slice(conns, func(i, j int) bool { return conns[i].ClientID() < conns[j].ClientID() })
	redirected := map[string]struct{}{} // namespace/client identifiers told to move
	targets := d.targets()
	next := 0
	began := time.Now()
	takeover := time.Duration(s.config().Cluster.Leases.TakeoverTimeoutMs) * time.Millisecond
	budget := d.started.Add(redirectBudget(timeout, takeover)).Sub(began)
	for i, c := range conns {
		if wait := time.Until(began.Add(redirectDueAt(i, len(conns), cfg.RatePerSecond, budget))); wait > 0 {
			time.Sleep(wait)
		}
		d.setRemaining(len(conns)-i, 0)
		id := c.Identity()
		if len(targets) == 0 {
			c.Close(packet.ServerBusy)
		} else {
			c.Redirect(targets[next%len(targets)])
			next++
			if c.Version() == packet.V5 {
				s.count("hoardmq_drain_clients_redirected_total", nil, 1)
			}
		}
		redirected[id.Namespace+"/"+c.ClientID()] = struct{}{}
		if i+1 < len(conns) && i%max(cfg.RatePerSecond, 1) == max(cfg.RatePerSecond, 1)-1 {
			targets = d.targets() // at least once a second
		}
	}
	d.setRemaining(0, 0)

	// 3. Wait for the redirected clients' takeovers, up to the timeout.
	if s.leases != nil && len(redirected) > 0 {
		d.phase("awaiting-takeovers")
		for time.Now().Before(deadline) {
			left := 0
			for _, l := range s.leases.Table().OwnedBy(s.config().Node.Name) {
				if _, ok := redirected[l.Namespace+"/"+l.ClientID]; ok {
					left++
				}
			}
			d.setRemaining(0, left)
			if left == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// 4. Hand every remaining lease to one live node. The hand-off is also
	// this node's word that it is leaving the shard map, so it is made even
	// with nothing left to hand, and no alive record follows it: the record
	// that clears the mark is the next incarnation's.
	handedOff := false
	if s.leases != nil {
		d.phase("handing-off-leases")
		paused := false
		attempts := 0
		for {
			owned := len(s.leases.Table().OwnedBy(s.config().Node.Name))
			d.setRemaining(0, owned)
			if owned == 0 && (handedOff || s.repl == nil) {
				break
			}
			to := d.handoffTarget()
			if to == "" {
				if owned > 0 {
					s.log.Warn("drain: no live node to hand leases to; they lapse for the leader to adopt", "leases", owned)
				}
				break
			}
			if !paused && s.repl != nil {
				if n := s.leases.PauseRenewals(d.ackTimeout()); n > 0 {
					s.log.Debug("drain: renewals still in flight at the hand-off", "inflight", n)
				}
				paused = true
			}
			d.noteOwned()
			attempts++
			if s.hooks.DrainHandoff != nil {
				s.hooks.DrainHandoff(attempts)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			moved, err := s.leases.Handoff(ctx, to)
			cancel()
			if err == nil {
				handedOff = true
				s.count("hoardmq_drain_leases_handed_off_total", nil, float64(moved))
				s.log.Info("drain: leases handed off", "to", to, "moved", moved)
				continue
			}
			if !handoffRetries(err, time.Now(), deadline) {
				if owned > 0 {
					s.log.Warn("drain: lease hand-off did not succeed; leases lapse for the leader to adopt", "err", err, "leases", owned)
				} else {
					s.log.Info("drain: hand-off with nothing to move did not succeed", "err", err)
				}
				break
			}
			s.count("hoardmq_drain_handoff_retries_total", nil, 1)
			s.log.Info("drain: hand-off failed; trying again", "to", to, "attempt", attempts, "err", err)
			time.Sleep(time.Second)
		}
		if paused && !handedOff {
			s.leases.ResumeRenewals()
		}
		if !handedOff && s.repl != nil {
			// Without a hand-off nothing marks the node leaving, so the map
			// keeps it through its down window.
			s.count("hoardmq_drain_placement_total", map[string]string{"outcome": "not-handed-off"}, 1)
			if attempts > 0 {
				s.log.Warn("drain: no hand-off applied; the node stays in the shard map", "attempts", attempts)
			} else {
				s.log.Info("drain: no live node to hand off to; the node stays in the shard map")
			}
		}
	}

	// 5. Leave the shard map, then wait for every session handed off to be
	// confirmed held by its receiver, up to the confirmation timeout.
	if handedOff && s.repl != nil {
		if s.hooks.DrainHandedOff != nil {
			s.hooks.DrainHandedOff()
		}
		d.leavePlacement()
	}
	if s.leases != nil && !s.hooks.Defects.DrainSkipsConfirmations {
		if s.hooks.DrainGate != nil {
			s.hooks.DrainGate()
		}
		d.awaitConfirmations(time.Duration(cfg.ConfirmTimeoutMs) * time.Millisecond)
	}
}

// redirectDueAt is how long after the redirect phase begins the client at
// position i of n is due: at rate clients a second (at least one), or sooner
// where that has every client due within budget, the time left to the
// redirect's deadline; with none left, at once. The loop sleeps only until a
// client is due: a sleep under a millisecond takes about a millisecond, which
// would cap a fixed per-client sleep near 1,000 a second.
func redirectDueAt(i, n, rate int, budget time.Duration) time.Duration {
	if budget <= 0 {
		return 0
	}
	byRate := time.Duration(i) * time.Second / time.Duration(max(rate, 1))
	byDeadline := time.Duration(float64(budget) * float64(i) / float64(max(n, 1)))
	return min(byRate, byDeadline)
}

// redirectBudget is how long after its start a drain with this timeout has
// to finish redirecting: the timeout less a reserve, the takeover timeout or
// half the timeout if that is less. The redirected clients' takeovers and the
// hand-off's retries are bounded by the same timeout, so the reserve leaves
// the last client its takeover and a failed hand-off a retry.
func redirectBudget(timeout, takeover time.Duration) time.Duration {
	return timeout - min(takeover, timeout/2)
}

// handoffRetries decides whether a drain proposes a failed hand-off again.
// The table would refuse it again, and a stopped or removed core cannot
// propose; every other failure is a proposal no leader applied, or one whose
// wait ended first, and a second hand-off of the same node moves nothing twice.
func handoffRetries(err error, now, deadline time.Time) bool {
	if errors.Is(err, lease.ErrHandoffRefused) || errors.Is(err, consensus.ErrStopped) || errors.Is(err, consensus.ErrRemoved) {
		return false
	}
	return now.Before(deadline)
}

// ackTimeout is the replication acknowledgement timeout.
func (d *drainer) ackTimeout() time.Duration {
	return time.Duration(d.s.config().Cluster.Replication.AckTimeoutMs) * time.Millisecond
}

// leavePlacement waits until the shard map has left this node out and every
// replica copy it kept for an alive owner has been confirmed held elsewhere,
// so its down window needs it for nothing. The map wait is short: a hand-off
// wakes the leader's driver at once. A node the copy count keeps in the map
// does not wait at all. If a renewal from before the hand-off applied after
// it and cleared the mark, the hand-off is made again. Each wait is bounded by
// twice the acknowledgement timeout, inside the chart's termination grace
// margin.
func (d *drainer) leavePlacement() {
	s := d.s
	d.phase("leaving-placement")
	ack := d.ackTimeout()
	began := time.Now()
	mapDeadline := began.Add(2 * ack)
	lastHandoff := began
	outcome := ""
	for outcome == "" {
		willPlace, left, marked := s.repl.Placement()
		switch {
		case left && marked:
			outcome = "left"
		case marked && willPlace:
			outcome = "placed"
		case !time.Now().Before(mapDeadline):
			outcome = "map-expired"
		default:
			if !marked && time.Since(lastHandoff) >= ack {
				if to := d.handoffTarget(); to != "" {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					_, err := s.leases.Handoff(ctx, to)
					cancel()
					s.log.Info("drain: leaving mark cleared before the map moved; hand-off made again", "to", to, "err", err)
				}
				lastHandoff = time.Now()
			}
			time.Sleep(drainPoll)
		}
	}
	if outcome == "left" {
		deadline := time.Now().Add(2 * ack)
		for {
			kept, retained := s.repl.KeptMoved(), s.repl.KeptRetained()
			d.mu.Lock()
			d.status.CopiesKept = kept
			d.status.RetainedShardsKept = retained
			d.mu.Unlock()
			if kept == 0 && retained == 0 {
				break
			}
			if !time.Now().Before(deadline) {
				outcome = "copies-expired"
				break
			}
			time.Sleep(drainPoll)
		}
	}
	s.count("hoardmq_drain_placement_total", map[string]string{"outcome": outcome}, 1)
	switch outcome {
	case "map-expired", "copies-expired":
		s.log.Warn("drain: the shard map did not leave this node out in time, or copies it kept were not confirmed held elsewhere",
			"outcome", outcome, "copiesKept", s.repl.KeptMoved(), "retainedShardsKept", s.repl.KeptRetained(), "seconds", time.Since(began).Seconds(), "sample", s.repl.KeptMovedSample(20))
	default:
		s.log.Info("drain: placement", "outcome", outcome, "seconds", time.Since(began).Seconds())
	}
}

// drainPoll is how often the confirmation wait looks again.
const drainPoll = 50 * time.Millisecond

// awaitConfirmations waits on each lease this node owned during the drain:
// the node it was handed to confirms once it holds the session durably. A
// lease still owned here has no receiver and is not waited on. A session no
// longer held here is waited on too: a confirmation that came before this
// wait dropped the copy and settles it on the first poll, and one that left
// without a confirmation is reported unconfirmed at the timeout.
func (d *drainer) awaitConfirmations(timeout time.Duration) {
	s := d.s
	self := s.config().Node.Name
	table := s.leases.Table()
	d.phase("awaiting-confirmations")
	deadline := time.Now().Add(timeout)
	d.mu.Lock()
	owned := make(map[drainKey]uint64, len(d.owned))
	for k, tok := range d.owned {
		owned[k] = tok
	}
	d.mu.Unlock()
	waiting := map[drainKey]uint64{}
	for k, tok := range owned {
		if l, ok := table.Lookup(k.ns, k.id); ok && l.Owner == self {
			continue
		}
		waiting[k] = tok
	}
	var stopped <-chan struct{}
	if s.ctx != nil {
		stopped = s.ctx.Done()
	}
	confirmed := 0
	for {
		for k, held := range waiting {
			_, exists := table.Lookup(k.ns, k.id)
			d.mu.Lock()
			top := d.confirmed[k]
			d.mu.Unlock()
			if !drainKeySettled(held, top, exists) {
				continue
			}
			delete(waiting, k)
			if top > held {
				confirmed++
				s.count("hoardmq_drain_sessions_confirmed_total", nil, 1)
			}
		}
		d.setSessions(len(waiting), confirmed, 0)
		end, expired := drainMayEnd(len(waiting), time.Now(), deadline)
		if !end {
			select {
			case <-time.After(drainPoll):
				continue
			case <-stopped:
				// Shut down without waiting for the drain: what is still
				// unconfirmed is reported the same way.
				d.expire(waiting, confirmed, "shutdown", timeout)
				return
			}
		}
		if expired {
			d.expire(waiting, confirmed, "timeout", timeout)
		} else if confirmed > 0 {
			s.log.Info("drain: every session handed off is confirmed held by its receiver", "sessions", confirmed)
		}
		return
	}
}

// expire counts and logs the sessions the wait ended with unconfirmed, at
// its timeout or at a shutdown. Those still held here stop with this node: a
// restart keeps them, and a node that never comes back loses them.
func (d *drainer) expire(waiting map[drainKey]uint64, confirmed int, ended string, timeout time.Duration) {
	s := d.s
	ids := make([]string, 0, len(waiting))
	held := 0
	for k := range waiting {
		ids = append(ids, k.id)
		if d.holds(k) {
			held++
		}
	}
	sort.Strings(ids)
	if len(ids) > 20 {
		ids = ids[:20]
	}
	d.setSessions(0, confirmed, len(waiting))
	s.count("hoardmq_drain_sessions_unconfirmed_total", nil, float64(len(waiting)))
	s.record("server", "server.drain", s.config().Node.Name, "unconfirmed", map[string]string{"sessions": fmt.Sprint(len(waiting)), "held": fmt.Sprint(held), "ended": ended})
	s.log.Warn("drain: sessions not confirmed held by their receiver when the wait ended",
		"ended", ended, "unconfirmed", len(waiting), "heldHere", held, "timeout", timeout.String(), "clients", ids)
}

// holds reports whether this node holds the client's session, in its
// session map or sealed.
func (d *drainer) holds(k drainKey) bool {
	if _, ok := d.s.sessions.HeldAt(k.ns, k.id); ok {
		return true
	}
	_, _, ok := d.s.sessions.Sealed(k.ns, k.id)
	return ok
}

// targets lists the client addresses of the other live nodes that
// advertise one, sorted by name.
func (d *drainer) targets() []string {
	s := d.s
	if s.leases == nil || s.consensus == nil {
		return nil
	}
	var out []string
	for _, n := range d.liveOthers() {
		if info, ok := s.leases.Table().Node(n); ok && info.Advertise != "" {
			out = append(out, info.Advertise)
		}
	}
	return out
}

// liveOthers lists the other members the lease table says are alive and not
// leaving, sorted by name: a node that is leaving takes neither clients nor
// leases from a drain.
func (d *drainer) liveOthers() []string {
	s := d.s
	var names []string
	for _, m := range s.consensus.Members() {
		names = append(names, m.Name)
	}
	t := s.leases.Table()
	return liveAmong(names, s.config().Node.Name, func(n string) bool { return t.Live(n) && !t.Leaving(n) })
}

// liveAmong is the members other than self that live reports alive, sorted.
func liveAmong(members []string, self string, live func(string) bool) []string {
	var out []string
	for _, m := range members {
		if m != self && live(m) {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

// handoffTarget is the consensus leader when it is another live node, else
// the lowest-name live member; empty when none.
func (d *drainer) handoffTarget() string {
	live := d.liveOthers()
	if len(live) == 0 {
		return ""
	}
	if leader, ok := d.s.consensus.Leader(); ok {
		for _, n := range live {
			if n == leader {
				return n
			}
		}
	}
	return live[0]
}
