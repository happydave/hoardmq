package session

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// An import evicts what the node holds for the client, then load adds the
// imported session's subscriptions to the router and installs it in the map.
// These tests hold load's first router add, which falls between the eviction
// and the install, and run a client's operation there on its own goroutine:
// a CONNECT waits for the import and connects to what it installed.

// importHoldRouter holds, once armed, the first router add of filter for
// client id until released.
type importHoldRouter struct {
	*exactRouter
	id, filter string
	armed      atomic.Bool
	entered    chan struct{}
	release    chan struct{}
	unblock    func()
}

func newImportHoldRouter(id, filter string) *importHoldRouter {
	r := &importHoldRouter{exactRouter: newExactRouter(), id: id, filter: filter, entered: make(chan struct{}), release: make(chan struct{})}
	r.unblock = sync.OnceFunc(func() { close(r.release) })
	return r
}

func (r *importHoldRouter) Add(s *Session, sub Subscription) bool {
	if s.key.id == r.id && sub.Filter == r.filter && r.armed.CompareAndSwap(true, false) {
		close(r.entered)
		<-r.release
	}
	return r.exactRouter.Add(s, sub)
}

// importedState is client c's session as an import receives it: persistent,
// subscribed to imp.
func importedState(m *Manager) *store.SessionState {
	return &store.SessionState{
		SessionMeta:   store.SessionMeta{Namespace: m.opts.Namespace, ClientID: "c", ExpiryInterval: 600, Version: byte(packet.V5), Epoch: 7},
		Subscriptions: []store.SubscriptionRecord{{Filter: "imp", QoS: 1}},
	}
}

type overwriteFixture struct {
	t *testing.T
	m *Manager
	r *importHoldRouter
}

func newOverwriteFixture(t *testing.T, mutate func(*Options)) *overwriteFixture {
	t.Helper()
	r := newImportHoldRouter("c", "imp")
	t.Cleanup(r.unblock)
	m, _ := newManager(t, func(o *Options) {
		o.Router = r
		if mutate != nil {
			mutate(o)
		}
	})
	return &overwriteFixture{t: t, m: m, r: r}
}

// connectWaits is how many CONNECTs waited for an import.
func (f *overwriteFixture) connectWaits() int64 {
	_, _, n := f.m.ImportWaits()
	return n
}

// importAround imports c's state with inside started on its own goroutine
// while the import's load is held between its eviction and its install. The
// import is released once inside is counted waiting for it, or has returned
// without waiting; held, if set, runs just before the release. A nil inside
// holds nothing.
func (f *overwriteFixture) importAround(inside, held func()) {
	f.t.Helper()
	if inside == nil {
		f.m.Import(importedState(f.m))
		return
	}
	f.r.armed.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.m.Import(importedState(f.m))
	}()
	<-f.r.entered
	opDone := make(chan struct{})
	go func() {
		defer close(opDone)
		inside()
	}()
	deadline := time.After(5 * time.Second)
	for waiting := false; !waiting; {
		select {
		case <-opDone:
			waiting = true
		case <-deadline:
			f.r.unblock()
			f.t.Fatal("the operation inside the import neither waited for it nor returned")
		case <-time.After(time.Millisecond):
			waiting = f.connectWaits() > 0
		}
	}
	if held != nil {
		held()
	}
	f.r.unblock()
	<-done
	<-opDone
}

// held is the map's object for c.
func (f *overwriteFixture) held() *Session {
	k := key{f.m.opts.Namespace, "c"}
	sh := f.m.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.sessions[k]
}

// strays lists every router subscriber that is not the map's object for its
// key holding that filter, each read under its own lock.
func strays(m *Manager) []string {
	var out []string
	for c, done := 0, false; !done; {
		var fs []RoutedFilter
		fs, c, done, _ = m.router.(RouterIndex).WalkStep(c)
		for _, rf := range fs {
			for _, s := range rf.Subs {
				sh := m.shardFor(s.key)
				sh.mu.Lock()
				isMap := sh.sessions[s.key] == s
				sh.mu.Unlock()
				s.mu.Lock()
				_, holds := s.subs[rf.Filter]
				s.mu.Unlock()
				if !isMap || !holds {
					out = append(out, fmt.Sprintf("%s on %s (the map's object %v, holding it %v)", s.key.id, rf.Filter, isMap, holds))
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// TestAConnectInsideAnImportLeavesNoStraySubscriber: a CONNECT of the client
// between the import's eviction and its install waits for the import and
// resumes the imported session, so a SUBSCRIBE on its connection routes the
// session the map holds. Controls: the CONNECT before the import (the
// eviction takes its session; the SUBSCRIBE is refused) and after it.
func TestAConnectInsideAnImportLeavesNoStraySubscriber(t *testing.T) {
	for _, when := range []string{"inside", "before", "after"} {
		t.Run(when, func(t *testing.T) {
			f := newOverwriteFixture(t, nil)
			c2 := newFake("c", packet.V5, 100)
			var present bool
			connect := func() { present = connectAt(f.m, c2, false, 600, 7).SessionPresent }
			switch when {
			case "inside":
				f.importAround(connect, nil)
				if !present || f.connectWaits() != 1 {
					t.Fatalf("CONNECT inside the import: Session Present %v after %d wait(s), want the imported session after 1", present, f.connectWaits())
				}
			case "before":
				connect()
				f.importAround(nil, nil)
			case "after":
				f.importAround(nil, nil)
				connect()
			}
			subscribe(f.m, c2, 1, "t")
			if s := f.held(); s == nil || !f.m.router.(RouterIndex).Holds(s, "imp") {
				t.Fatalf("precondition: the imported session is not held and routed on imp")
			}
			if got := strays(f.m); len(got) > 0 {
				t.Fatalf("CONNECT %s the import: the router holds %d subscriber(s) for session objects no map holds with the filter: %q", when, len(got), got)
			}
		})
	}
}

// TestASubscribeInsideAnImportLeavesNoStraySubscriber: the CONNECT and its
// SUBSCRIBE both start between the import's eviction and its install. Control:
// both before the import.
func TestASubscribeInsideAnImportLeavesNoStraySubscriber(t *testing.T) {
	for _, when := range []string{"inside", "before"} {
		t.Run(when, func(t *testing.T) {
			f := newOverwriteFixture(t, nil)
			c2 := newFake("c", packet.V5, 100)
			var present bool
			op := func() {
				present = connectAt(f.m, c2, false, 600, 7).SessionPresent
				subscribe(f.m, c2, 1, "t")
			}
			if when == "inside" {
				f.importAround(op, nil)
				s := f.held()
				if !present || s == nil || !f.m.router.(RouterIndex).Holds(s, "t") {
					t.Fatalf("CONNECT and SUBSCRIBE inside the import: Session Present %v, and the map's session routed on the subscribed t %v; want both", present, s != nil && f.m.router.(RouterIndex).Holds(s, "t"))
				}
			} else {
				op()
				f.importAround(nil, nil)
			}
			if got := strays(f.m); len(got) > 0 {
				t.Fatalf("CONNECT and SUBSCRIBE %s the import: the router holds %d subscriber(s) for session objects no map holds with the filter: %q", when, len(got), got)
			}
		})
	}
}

// TestAConnectWaitingForAnImportHoldsNoAdmission: with one admission slot, a
// CONNECT waiting for an import of its client leaves the slot to others: a
// second client connects while the import is still held.
func TestAConnectWaitingForAnImportHoldsNoAdmission(t *testing.T) {
	f := newOverwriteFixture(t, func(o *Options) { o.StoreAdmission = 1 })
	c2 := newFake("c", packet.V5, 100)
	other := func() {
		d := newFake("d", packet.V5, 100)
		done := make(chan struct{})
		go func() {
			defer close(done)
			connectAt(f.m, d, false, 600, 1)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			f.r.unblock()
			t.Fatal("another client's CONNECT did not complete while a CONNECT waited for an import")
		}
	}
	f.importAround(func() { connectAt(f.m, c2, false, 600, 7) }, other)
	if f.connectWaits() != 1 {
		t.Fatalf("precondition: %d CONNECT wait(s), want 1", f.connectWaits())
	}
}

// TestAnExpiryZeroConnectInsideAnImportLeavesNoStraySubscriber: the CONNECT
// inside the import asks for no session expiry; it resumes the imported
// session, which ends with its connection, leaving nothing routed. Control: the
// CONNECT before the import.
func TestAnExpiryZeroConnectInsideAnImportLeavesNoStraySubscriber(t *testing.T) {
	for _, when := range []string{"inside", "before"} {
		t.Run(when, func(t *testing.T) {
			f := newOverwriteFixture(t, nil)
			c2 := newFake("c", packet.V5, 100)
			var present bool
			connect := func() { present = connectAt(f.m, c2, false, 0, 7).SessionPresent }
			if when == "inside" {
				f.importAround(connect, nil)
				if !present {
					t.Errorf("CONNECT inside the import: Session Present false, want the imported session")
				}
			} else {
				connect()
				f.importAround(nil, nil)
			}
			f.m.Disconnect(c2, false, packet.NormalDisconnection, false)
			if got := strays(f.m); len(got) > 0 {
				t.Fatalf("CONNECT %s the import: the router holds %d subscriber(s) for session objects no map holds with the filter: %q", when, len(got), got)
			}
		})
	}
}

// orphanOf connects client c, subscribes it to t, then puts a new object in
// its map entry by a plain assignment: the connection's object is then one no
// map holds. It returns the connection and the map's new object.
func orphanOf(t *testing.T, m *Manager, expiry uint32) (*fakeConn, *Session) {
	t.Helper()
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, false, expiry, 7)
	subscribe(m, c, 1, "t")
	k := key{m.opts.Namespace, "c"}
	held := newSession(k, m.log)
	held.mgr = m
	sh := m.shardFor(k)
	sh.mu.Lock()
	sh.sessions[k] = held
	sh.mu.Unlock()
	return c, held
}

// TestADisconnectOfAnOrphanKeepsTheMapsSession: a connection whose object the
// map no longer holds ends; the map's object for the key stays, nothing it
// left is routed, and nothing is journaled for the client.
func TestADisconnectOfAnOrphanKeepsTheMapsSession(t *testing.T) {
	for _, expiry := range []uint32{0, 600} {
		t.Run(fmt.Sprint(expiry), func(t *testing.T) {
			j := newFakeJournal()
			m, _ := newManager(t, func(o *Options) { o.Journal = j })
			c, held := orphanOf(t, m, expiry)
			j.mu.Lock()
			before := len(j.calls)
			j.mu.Unlock()
			m.Disconnect(c, false, packet.NormalDisconnection, false)
			k := key{m.opts.Namespace, "c"}
			sh := m.shardFor(k)
			sh.mu.Lock()
			cur := sh.sessions[k]
			sh.mu.Unlock()
			if cur != held {
				t.Errorf("expiry %d: after the orphan's connection ended the map holds %p, want its own object %p", expiry, cur, held)
			}
			if got := strays(m); len(got) > 0 {
				t.Errorf("expiry %d: the router holds %d subscriber(s) for session objects no map holds with the filter: %q", expiry, len(got), got)
			}
			j.mu.Lock()
			after := append([]string(nil), j.calls[before:]...)
			j.mu.Unlock()
			for _, call := range after {
				if strings.HasSuffix(call, " c") || strings.Contains(call, " c ") {
					t.Errorf("expiry %d: the orphan's disconnect journaled %q for the client", expiry, call)
				}
			}
		})
	}
}

// TestALoadOverAnotherObjectRemovesIt: an install that finds another object
// for the client removes it as an eviction does, counts it and logs it.
func TestALoadOverAnotherObjectRemovesIt(t *testing.T) {
	logs := &lockedBuffer{}
	m, _ := newManager(t, func(o *Options) {
		o.Logger = slog.New(slog.NewTextHandler(logs, nil))
	})
	c := newFake("c", packet.V5, 100)
	connectAt(m, c, false, 600, 7)
	subscribe(m, c, 1, "t")
	k := key{m.opts.Namespace, "c"}
	sh := m.shardFor(k)
	sh.mu.Lock()
	first := sh.sessions[k]
	sh.mu.Unlock()
	m.load(&store.State{Sessions: []*store.SessionState{importedState(m)}}, true)
	sh.mu.Lock()
	cur := sh.sessions[k]
	sh.mu.Unlock()
	first.mu.Lock()
	removed, conn := first.removed, first.conn
	first.mu.Unlock()
	c.mu.Lock()
	closed := append([]packet.ReasonCode(nil), c.closed...)
	c.mu.Unlock()
	switch {
	case cur == first || cur == nil:
		t.Fatal("the load did not install its session")
	case !removed:
		t.Fatal("the object the load replaced is not marked removed")
	case conn != nil:
		t.Fatal("the object the load replaced keeps its connection")
	case len(closed) != 1 || closed[0] != packet.SessionTakenOver:
		t.Fatalf("the replaced object's connection closed with %v, want Session Taken Over once", closed)
	case m.InstallsReplaced() != 1:
		t.Fatalf("installs replaced %d, want 1", m.InstallsReplaced())
	case len(logs.lines("session install found another session object")) != 1:
		t.Fatal("the replacement was not logged")
	}
	if got := strays(m); len(got) > 0 {
		t.Fatalf("the router holds %d subscriber(s) for session objects no map holds with the filter: %q", len(got), got)
	}
}

func TestHeldAsAndReplacesAnother(t *testing.T) {
	a, b := &Session{}, &Session{}
	for _, c := range []struct {
		name          string
		current, s    *Session
		held, replace bool
	}{
		{"none held", nil, a, false, false},
		{"the same object", a, a, true, false},
		{"another object", b, a, false, true},
	} {
		if got := heldAs(c.current, c.s); got != c.held {
			t.Errorf("%s: heldAs %v, want %v", c.name, got, c.held)
		}
		if got := replacesAnother(c.current, c.s); got != c.replace {
			t.Errorf("%s: replacesAnother %v, want %v", c.name, got, c.replace)
		}
	}
}
