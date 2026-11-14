package session

import (
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// heldAddRouter is the exact router whose next Add for client waits until
// released, after telling entered.
type heldAddRouter struct {
	*exactRouter
	client  string
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (r *heldAddRouter) Add(s *Session, sub Subscription) bool {
	if s.key.id == r.client {
		r.once.Do(func() {
			r.entered <- struct{}{}
			<-r.release
		})
	}
	return r.exactRouter.Add(s, sub)
}

// matches is how many subscribers the router matches for a QoS 1 message on
// topic.
func matches(m *Manager, topic string) int {
	return len(m.Router().Match(&message.Message{Namespace: m.opts.Namespace, Topic: topic, QoS: 1}, nil))
}

// sealedForResume connects "c" through r at token 5, subscribes it to "t",
// takes it offline and seals it at token 9, then arms r to hold the next add,
// so a CONNECT at a later grant resumes the copy and waits at its first add.
// It returns the manager and the release of the held add.
func sealedForResume(t *testing.T, r *heldAddRouter) (*Manager, func()) {
	t.Helper()
	m, _ := newManager(t, func(o *Options) { o.Journal = newFakeJournal(); o.Router = r })
	c := newFake("c", packet.V5, 100)
	r.once.Do(func() {}) // the fixture's own subscribe passes
	connectAt(m, c, true, 600, 5)
	subscribe(m, c, 1, "t")
	m.Disconnect(c, false, packet.NormalDisconnection, false)
	if res := m.Seal(m.opts.Namespace, "c", packet.SessionTakenOver, 9); res.Outcome != SealSealed {
		t.Fatalf("seal: %+v", res)
	}
	r.once = sync.Once{}
	unblock := sync.OnceFunc(func() { close(r.release) })
	t.Cleanup(unblock)
	return m, unblock
}

func newHeldAddRouter() *heldAddRouter {
	return &heldAddRouter{exactRouter: newExactRouter(), client: "c", entered: make(chan struct{}, 1), release: make(chan struct{})}
}

// removeWhileAdding runs remove while the resume's add is held, then releases
// the add. A removal that has to wait for the add (the add made under the
// session's mutex) is given removeWait to show it is waiting before the add is
// released; the wait is bounded, not asserted. It reports what remove did.
func removeWhileAdding(release func(), remove func() bool) bool {
	var removed bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		removed = remove()
	}()
	select {
	case <-done:
	case <-time.After(removeWait):
	}
	release()
	<-done
	return removed
}

const removeWait = 500 * time.Millisecond

// TestAResumedSealedCopyRemovedBeforeItsRoutesLeavesNoRoute: a CONNECT
// resumes a sealed copy and installs it in the map. An eviction while its
// subscriptions are reaching the router must leave none there for a session
// the manager no longer holds.
func TestAResumedSealedCopyRemovedBeforeItsRoutesLeavesNoRoute(t *testing.T) {
	r := newHeldAddRouter()
	m, release := sealedForResume(t, r)
	done := make(chan struct{})
	go func() {
		defer close(done)
		connectAt(m, newFake("c", packet.V5, 100), false, 600, 12)
	}()
	<-r.entered
	if !removeWhileAdding(release, func() bool { return m.Evict(m.opts.Namespace, "c", packet.SessionTakenOver) }) {
		t.Fatal("the resumed session was not in the map to evict")
	}
	<-done

	_, held := m.Export(m.opts.Namespace, "c")
	if n := matches(m, "t"); !held && n > 0 {
		t.Fatalf("the router matches %d subscriber(s) for a session the manager no longer holds", n)
	}
}

// TestADisplacedResumeKeepsItsRoutes: a second CONNECT without Clean Start
// replaces the resuming connection before the resume's subscriptions reach the
// router. The session is still held, so its subscriptions must reach the
// router.
func TestADisplacedResumeKeepsItsRoutes(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.Journal = newFakeJournal() })
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, true, 600, 5)
	subscribe(m, c, 1, "t")
	m.Disconnect(c, false, packet.NormalDisconnection, false)
	if res := m.Seal(m.opts.Namespace, "c", packet.SessionTakenOver, 9); res.Outcome != SealSealed {
		t.Fatalf("seal: %+v", res)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	m.resumePause = func() {
		close(entered)
		<-release
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		connectAt(m, newFake("c", packet.V5, 100), false, 600, 12)
	}()
	<-entered
	connectAt(m, newFake("c", packet.V5, 100), false, 600, 12)
	unblock()
	<-done

	if _, held := m.Export(m.opts.Namespace, "c"); !held {
		t.Fatal("the premise: the displaced session should still be held")
	}
	if n := matches(m, "t"); n != 1 {
		t.Fatalf("the session is held but the router matches %d subscriber(s) for its filter, want 1", n)
	}
}

// TestASubscribeRacingAnEvictionLeavesNoRoute: Subscribe finds the session,
// then an eviction removes it before Subscribe takes the session's mutex. The
// subscribe must add nothing and answer as for a connection with no session.
func TestASubscribeRacingAnEvictionLeavesNoRoute(t *testing.T) {
	for _, race := range []bool{true, false} {
		m, _ := newManager(t, nil)
		c := newFake("c", packet.V5, 100)
		connectAt(m, c, true, 600, 5)
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		t.Cleanup(unblock)
		if race {
			m.subscribePause = func() {
				close(entered)
				<-release
			}
		}
		var codes []packet.ReasonCode
		done := make(chan struct{})
		go func() {
			defer close(done)
			codes = m.Subscribe(c, &packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "t", QoS: 1}}})
		}()
		if race {
			<-entered
		} else {
			<-done // the control: the eviction after the subscribe
		}
		if !m.Evict(m.opts.Namespace, "c", packet.SessionTakenOver) {
			t.Fatalf("race %v: the session was not in the map to evict", race)
		}
		unblock()
		<-done

		if n := matches(m, "t"); n != 0 {
			t.Fatalf("race %v: the router matches %d subscriber(s) for a session the manager no longer holds", race, n)
		}
		if race && (len(codes) != 1 || codes[0] != packet.UnspecifiedError) {
			t.Fatalf("a subscribe for a session removed since its lookup answered %v, want Unspecified Error", codes)
		}
	}
}

func TestMayRoute(t *testing.T) {
	a, b := newFake("a", packet.V5, 1), newFake("b", packet.V5, 1)
	for _, c := range []struct {
		name            string
		removed         bool
		current, asking Connection
		want            bool
	}{
		{"held, no connection asked", false, a, nil, true},
		{"held, offline, no connection asked", false, nil, nil, true},
		{"removed, no connection asked", true, nil, nil, false},
		{"the asking connection is current", false, a, a, true},
		{"another connection is current", false, b, a, false},
		{"removed with the asking connection current", true, a, a, false},
	} {
		if got := mayRoute(c.removed, c.current, c.asking); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestARemovedSessionIsMarked: every way a session leaves the manager marks
// the object removed, so no later router add is made for it.
func TestARemovedSessionIsMarked(t *testing.T) {
	for _, c := range []struct {
		name   string
		remove func(m *Manager)
	}{
		{"evicted", func(m *Manager) { m.Evict(m.opts.Namespace, "c", packet.SessionTakenOver) }},
		{"taken", func(m *Manager) { m.Take(m.opts.Namespace, "c", packet.SessionTakenOver) }},
		{"sealed", func(m *Manager) { m.Seal(m.opts.Namespace, "c", packet.SessionTakenOver, 9) }},
		{"replaced by a Clean Start", func(m *Manager) { connectAt(m, newFake("c", packet.V5, 100), true, 600, 5) }},
	} {
		m, _ := newManager(t, nil)
		connectAt(m, newFake("c", packet.V5, 100), true, 600, 5)
		k := key{m.opts.Namespace, "c"}
		sh := m.shardFor(k)
		sh.mu.Lock()
		s := sh.sessions[k]
		sh.mu.Unlock()
		c.remove(m)
		s.mu.Lock()
		removed := s.removed
		s.mu.Unlock()
		if !removed {
			t.Errorf("%s: the session that left is not marked removed", c.name)
		}
	}
}
