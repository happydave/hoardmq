package session

import (
	"sort"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/conn"
	"github.com/happydave/hoardmq/internal/packet"
)

// fakeConn records what a session sends and models the send quota.
type fakeConn struct {
	mu       sync.Mutex
	id       string
	version  packet.Version
	quota    int
	sent     []packet.Packet
	closed   []packet.ReasonCode
	override *uint32
	identity auth.Identity
	// busy answers every delivery ErrBusy, as a connection whose outbound
	// queue is full does.
	busy bool
}

func newFake(id string, v packet.Version, quota int) *fakeConn {
	return &fakeConn{id: id, version: v, quota: quota, identity: auth.Identity{ClientID: id, Username: "u-" + id}}
}

func (f *fakeConn) ClientID() string               { return f.id }
func (f *fakeConn) Version() packet.Version        { return f.version }
func (f *fakeConn) Identity() *auth.Identity       { return &f.identity }
func (f *fakeConn) SessionExpiryOverride() *uint32 { return f.override }

func (f *fakeConn) Deliver(p *packet.Publish) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.busy {
		return conn.ErrBusy
	}
	if p.QoS > 0 {
		if f.quota <= 0 {
			return conn.ErrQuota
		}
		f.quota--
	}
	f.sent = append(f.sent, p)
	return nil
}

func (f *fakeConn) Send(p packet.Packet) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, p)
	return nil
}

func (f *fakeConn) Close(r packet.ReasonCode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = append(f.closed, r)
}

func (f *fakeConn) SendQuota() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.quota
}

// ack models the client acknowledging: the quota returns as the connection
// layer would return it.
func (f *fakeConn) ack() {
	f.mu.Lock()
	f.quota++
	f.mu.Unlock()
}

func (f *fakeConn) packets() []packet.Packet {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]packet.Packet, len(f.sent))
	copy(out, f.sent)
	return out
}

func (f *fakeConn) publishes() []*packet.Publish {
	var out []*packet.Publish
	for _, p := range f.packets() {
		if pub, ok := p.(*packet.Publish); ok {
			out = append(out, pub)
		}
	}
	return out
}

// fakeClock drives timers by hand.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at      time.Time
	f       func()
	stopped bool
	fired   bool
}

func (t *fakeTimer) Stop() bool {
	was := !t.stopped && !t.fired
	t.stopped = true
	return was
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

// advance moves time forward and fires due timers in order.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	c.mu.Unlock()
	for {
		c.mu.Lock()
		var due *fakeTimer
		sort.Slice(c.timers, func(i, j int) bool { return c.timers[i].at.Before(c.timers[j].at) })
		for _, t := range c.timers {
			if !t.stopped && !t.fired && !t.at.After(now) {
				due = t
				break
			}
		}
		if due != nil {
			due.fired = true
		}
		c.mu.Unlock()
		if due == nil {
			return
		}
		due.f()
	}
}

func (f *fakeConn) setBusy(b bool) {
	f.mu.Lock()
	f.busy = b
	f.mu.Unlock()
}
