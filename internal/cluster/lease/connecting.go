package lease

import (
	"context"
	"sync"

	"github.com/happydave/hoardmq/internal/packet"
)

// A CONNECT that found its client's lease and proposed an acquire is in flight
// on this node until its answer shows it took nothing from another node, or,
// when it took the lease over, until its caller has installed the session it
// pulled (AcquireResult.Settle). Another CONNECT of the client here that finds
// the lease already this node's waits for every such CONNECT first: answered
// sooner, it would be served a fresh session that the takeover's import then
// replaces, evicting it.

// Two CONNECTs of a client can wait on the same takeover, and both wake when
// it ends. The newer one, by its arrival at AcquireFrom, is served; an older
// one is refused, since its client has connected again since (MQTT-3.1.4-3):
// connected last, it would displace the newer connection.

// connecting counts this node's CONNECTs in flight per client, and its
// waiting CONNECTs. Its mutex is taken with no other lock held, and none is
// taken under it.
type connecting struct {
	mu      sync.Mutex
	m       map[key]*inFlight
	waiting map[key]*waiters
}

// waiters is one client's waiting CONNECTs: how many, and the newest arrival
// among those that have waited since the client last had none.
type waiters struct {
	n      int
	newest uint64
}

// inFlight is one client's count; idle closes when it falls to zero.
type inFlight struct {
	n    int
	idle chan struct{}
}

func newConnecting() *connecting {
	return &connecting{m: map[key]*inFlight{}, waiting: map[key]*waiters{}}
}

// overtaken decides whether a waiting CONNECT that arrived at arrival is
// refused on waking: a newer CONNECT of its client waited beside it.
func overtaken(newest, arrival uint64) bool { return newest > arrival }

func (c *connecting) wait(k key, arrival uint64) {
	c.mu.Lock()
	w := c.waiting[k]
	if w == nil {
		w = &waiters{}
		c.waiting[k] = w
	}
	w.n++
	w.newest = max(w.newest, arrival)
	c.mu.Unlock()
}

// woken ends a CONNECT's wait and reports whether a newer one overtook it.
// The newest arrival is kept until the client's last waiter has woken, so an
// older waiter that wakes after the newer one has gone still sees it.
func (c *connecting) woken(k key, arrival uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.waiting[k]
	if w == nil {
		return false
	}
	over := overtaken(w.newest, arrival)
	if w.n--; w.n == 0 {
		delete(c.waiting, k)
	}
	return over
}

func (c *connecting) begin(k key) {
	c.mu.Lock()
	e := c.m[k]
	if e == nil {
		e = &inFlight{idle: make(chan struct{})}
		c.m[k] = e
	}
	e.n++
	c.mu.Unlock()
}

func (c *connecting) end(k key) {
	c.mu.Lock()
	if e := c.m[k]; e != nil {
		if e.n--; e.n == 0 {
			delete(c.m, k)
			close(e.idle)
		}
	}
	c.mu.Unlock()
}

// idle returns the channel that closes when the client has no CONNECT in
// flight here, or nil when it has none now.
func (c *connecting) idle(k key) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.m[k]; e != nil {
		return e.idle
	}
	return nil
}

// total sums the clients' counts; a service built without the set has none.
func (c *connecting) total() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.m {
		n += e.n
	}
	return n
}

// waitingTotal sums the clients' waiting CONNECTs.
func (c *connecting) waitingTotal() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, w := range c.waiting {
		n += w.n
	}
	return n
}

// awaitTakeovers waits until no other CONNECT of the client is in flight on
// this node; arrival is the CONNECT's arrival at AcquireFrom. A CONNECT whose
// handshake ends first, or that a newer CONNECT of its client overtook while
// it waited, is refused Server Busy; the caller holds no count of its own.
func (s *Service) awaitTakeovers(ctx context.Context, k key, arrival uint64) error {
	idle := s.connecting.idle(k)
	if idle == nil {
		return nil
	}
	s.connecting.wait(k, arrival)
	for ; idle != nil; idle = s.connecting.idle(k) {
		select {
		case <-idle:
		case <-ctx.Done():
			s.connecting.woken(k, arrival)
			s.takeoverWaitsRefused.Add(1)
			s.count("hoardmq_lease_takeover_waits_total", map[string]string{"outcome": "refused"}, 1)
			s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "takeover-in-flight"}, 1)
			return &Refusal{Reason: packet.ServerBusy}
		case <-s.ctx.Done():
			s.connecting.woken(k, arrival)
			return &Refusal{Reason: packet.ServerUnavailable}
		}
	}
	if s.connecting.woken(k, arrival) {
		s.takeoverWaitsOvertaken.Add(1)
		s.count("hoardmq_lease_takeover_waits_total", map[string]string{"outcome": "overtaken"}, 1)
		s.count("hoardmq_lease_ops_total", map[string]string{"op": "acquire", "outcome": "takeover-overtaken"}, 1)
		return &Refusal{Reason: packet.ServerBusy}
	}
	s.takeoverWaitsServed.Add(1)
	s.count("hoardmq_lease_takeover_waits_total", map[string]string{"outcome": "served"}, 1)
	return nil
}
