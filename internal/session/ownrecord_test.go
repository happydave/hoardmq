package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// switchReplication is a replication layer whose durability wait fails while
// down is set, as one whose followers are unreachable does. during, when set,
// runs inside each wait.
type switchReplication struct {
	down   atomic.Bool
	during func()
}

func (r *switchReplication) Barrier(context.Context, []store.Scope, []message.ForwardRef) error {
	return r.wait()
}
func (r *switchReplication) Durable(context.Context, []store.Scope) error { return r.wait() }
func (r *switchReplication) wait() error {
	if r.during != nil {
		r.during()
	}
	if r.down.Load() {
		return errors.New("followers unreachable")
	}
	return nil
}
func (r *switchReplication) StoreRetained(*message.Message) bool { return true }
func (r *switchReplication) QueryRetained(string, string, time.Time) []*message.Message {
	return nil
}

func pubrels(c *fakeConn) []uint16 {
	var ids []uint16
	for _, p := range c.packets() {
		if r, ok := p.(*packet.Pubrel); ok {
			ids = append(ids, r.PacketID)
		}
	}
	return ids
}

// recovered closes the store under m, opens it again and recovers it.
func recovered(t *testing.T, dir string, st *store.Store) *store.State {
	t.Helper()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := store.Open(dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Close() })
	got, err := again.Recover()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// rollingFixture is a manager over a rolling journal (one record per segment,
// so the disk failing its next call fails the next append) with a connected
// durable subscriber "sub" to t at QoS 2 and a connected publisher "pub" whose
// own session journals nothing.
type rollingFixture struct {
	m        *Manager
	clock    *fakeClock
	d        *faultdisk.Disk
	st       *store.Store
	dir      string
	sub, pub *fakeConn
}

func newRollingFixture(t *testing.T, mutate func(*Options)) *rollingFixture {
	t.Helper()
	f := &rollingFixture{d: faultdisk.New(), dir: t.TempDir()}
	st, err := store.Open(f.dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Sync: f.d.Sync, SegmentBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	f.st = st
	f.m, f.clock = newManager(t, func(o *Options) {
		o.Journal = st
		if mutate != nil {
			mutate(o)
		}
	})
	f.sub = newFake("sub", packet.V5, 100)
	connectV5(f.m, f.sub, true, 600, nil)
	subscribe(f.m, f.sub, 2, "t")
	f.pub = newFake("pub", packet.V5, 100)
	connectV5(f.m, f.pub, true, 0, nil)
	return f
}

// delivered publishes on t at qos and returns the packet identifier the
// subscriber received it under.
func (f *rollingFixture) delivered(t *testing.T, qos byte, id uint16, payload string) uint16 {
	t.Helper()
	before := len(f.sub.publishes())
	if code := publish(f.m, f.pub, "t", qos, id, payload); code.IsError() {
		t.Fatalf("precondition: publish %q answered %v", payload, code)
	}
	got := f.sub.publishes()
	if len(got) != before+1 || string(got[len(got)-1].Payload) != payload {
		t.Fatalf("precondition: the subscriber did not receive %q", payload)
	}
	return got[len(got)-1].PacketID
}

// TestPubrelHeldWhileItsPubrecStateIsUnjournaled: the PUBREC state's append
// fails, so a PUBREL now would let the client complete an exchange no journal
// or replica knows past its PUBLISH. It is held, a drain does not send it, and
// it goes out once the session has been written whole.
func TestPubrelHeldWhileItsPubrecStateIsUnjournaled(t *testing.T) {
	f := newRollingFixture(t, nil)
	x := f.delivered(t, 2, 1, "exactly-once")
	f.d.FailNext(1)
	if code, send := f.m.Pubrec(f.sub, x, packet.Success); code != packet.Success || send {
		t.Fatalf("Pubrec with its state unjournaled = %v, send PUBREL %v; want Success and held", code, send)
	}
	y := f.delivered(t, 1, 2, "another")
	f.m.Puback(f.sub, y, packet.Success) // drains
	if got := pubrels(f.sub); len(got) != 0 {
		t.Fatalf("PUBREL sent for %v while the PUBREC state was unjournaled", got)
	}
	f.clock.advance(pubrelRetry)
	if got := pubrels(f.sub); len(got) != 1 || got[0] != x {
		t.Fatalf("after the rewrite the PUBRELs sent are %v, want exactly [%d]", got, x)
	}
	ss := recovered(t, f.dir, f.st).Find("default", "sub")
	if ss == nil {
		t.Fatal("the recovered journal has no session sub")
	}
	for _, e := range ss.Entries {
		if e.PacketID == x {
			if e.State != store.StatePubrecReceived {
				t.Fatalf("the recovered entry %d is in state %d, want PUBREC received", x, e.State)
			}
			return
		}
	}
	t.Fatalf("the recovered journal has no entry %d: %+v", x, ss.Entries)
}

// TestDrainDoesNotSendAHeldPubrel: a PUBREL held because the replicas do not
// hold the PUBREC state yet is not sent by the next drain, and is sent once
// when they do.
func TestDrainDoesNotSendAHeldPubrel(t *testing.T) {
	rep := &switchReplication{}
	m, clock := newManager(t, nil)
	m.SetReplication(rep)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 600, nil)
	subscribe(m, sub, 2, "t")
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	publish(m, pub, "t", 2, 1, "exactly-once")
	x := sub.publishes()[0].PacketID
	rep.down.Store(true)
	if _, send := m.Pubrec(sub, x, packet.Success); send {
		t.Fatal("precondition: the PUBREL was not held with the replicas down")
	}
	rep.down.Store(false)
	publish(m, pub, "t", 1, 2, "another")
	m.Puback(sub, sub.publishes()[1].PacketID, packet.Success) // drains
	if got := pubrels(sub); len(got) != 0 {
		t.Fatalf("a drain sent the held PUBREL %v before the retry found the state replicated", got)
	}
	clock.advance(pubrelRetry)
	if got := pubrels(sub); len(got) != 1 || got[0] != x {
		t.Fatalf("after the retry the PUBRELs sent are %v, want exactly [%d]", got, x)
	}
}

// TestHeldPubrelOfAnOfflineSessionIsSentOnReconnect: the replicas take the
// PUBREC state while the subscriber is offline; the hold clears, and the next
// connection's resumption sends the PUBREL once.
func TestHeldPubrelOfAnOfflineSessionIsSentOnReconnect(t *testing.T) {
	rep := &switchReplication{}
	m, clock := newManager(t, nil)
	m.SetReplication(rep)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 600, nil)
	subscribe(m, sub, 2, "t")
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	publish(m, pub, "t", 2, 1, "exactly-once")
	x := sub.publishes()[0].PacketID
	rep.down.Store(true)
	if _, send := m.Pubrec(sub, x, packet.Success); send {
		t.Fatal("precondition: the PUBREL was not held with the replicas down")
	}
	m.Disconnect(sub, false, packet.NormalDisconnection, false)
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	again := newFake("sub", packet.V5, 100)
	if !connectV5(m, again, false, 600, nil) {
		t.Fatal("precondition: the session was not present on reconnect")
	}
	if got := pubrels(again); len(got) != 1 || got[0] != x {
		t.Fatalf("on reconnect the PUBRELs sent are %v, want exactly [%d]", got, x)
	}
	if got := pubrels(sub); len(got) != 0 {
		t.Fatalf("the old connection was sent PUBRELs %v", got)
	}
}

// TestOwnRecordFailureRefusesNoUnrelatedPublish: a reconnect's session record
// (a new expiry) fails to append. The session's next QoS 1 publish, whose own
// records are fine, is not refused for it, and the session is written whole so
// the journal holds the new expiry.
func TestOwnRecordFailureRefusesNoUnrelatedPublish(t *testing.T) {
	f := newRollingFixture(t, nil)
	again := newFake("sub", packet.V5, 100)
	f.d.FailNext(1)
	if !connectV5(f.m, again, false, 900, nil) {
		t.Fatal("precondition: the session was not present on reconnect")
	}
	if code := publish(f.m, again, "t", 1, 7, "unrelated"); code.IsError() {
		t.Fatalf("a publish after the session's own record failed answered %v", code)
	}
	f.clock.advance(pubrelRetry)
	ss := recovered(t, f.dir, f.st).Find("default", "sub")
	if ss == nil {
		t.Fatal("the recovered journal has no session sub")
	}
	if ss.ExpiryInterval != 900 {
		t.Fatalf("the recovered session's expiry is %d, want the reconnect's 900, whose record failed", ss.ExpiryInterval)
	}
}

// injectedQueued queues a QoS 1 message for the offline subscriber through
// Inject, a delivery nobody waits on, with its record's append failing.
func (f *rollingFixture) injectedQueued(t *testing.T) *Session {
	t.Helper()
	f.m.Disconnect(f.sub, false, packet.NormalDisconnection, false)
	f.d.FailNext(1)
	f.m.Inject(&message.Message{Topic: "t", QoS: 1, Payload: []byte("queued")})
	sh := f.m.shardFor(key{"default", "sub"})
	sh.mu.Lock()
	s := sh.sessions[key{"default", "sub"}]
	sh.mu.Unlock()
	if s == nil {
		t.Fatal("precondition: no session sub")
	}
	s.mu.Lock()
	marked, queued := s.unwritten, len(s.queue)
	s.mu.Unlock()
	if !marked || queued != 1 {
		t.Fatalf("precondition: marked %v, queued %d; want the delivery's record failed and its copy kept", marked, queued)
	}
	return s
}

// recoveredOnce recovers the journal and returns sub, checking it is there
// once, subscribed to t, and holds the queued message.
func (f *rollingFixture) recoveredOnce(t *testing.T) {
	t.Helper()
	st := recovered(t, f.dir, f.st)
	n := 0
	for _, ss := range st.Sessions {
		if ss.ClientID == "sub" {
			n++
		}
	}
	ss := st.Find("default", "sub")
	if n != 1 || ss == nil || len(ss.Subscriptions) != 1 || ss.Subscriptions[0].Filter != "t" {
		t.Fatalf("recovered %d copies of sub (%+v); want one, subscribed to t", n, ss)
	}
	var got []string
	for _, e := range ss.Entries {
		got = append(got, string(e.Message.Payload))
	}
	if len(got) != 1 || got[0] != "queued" {
		t.Fatalf("recovered entries %v, want [queued]", got)
	}
}

// TestUnwaitedDeliveryFailureIsRewritten: a delivery nobody waits on (a
// module's message into an offline session) fails to append. The copy stays
// queued, and the rewrite puts it in the journal.
func TestUnwaitedDeliveryFailureIsRewritten(t *testing.T) {
	f := newRollingFixture(t, nil)
	f.injectedQueued(t)
	f.clock.advance(pubrelRetry)
	f.recoveredOnce(t)
}

// TestRewriteRetriesUntilWhole: the rewrite's second append fails, after its
// delete. The mark stays, and the next attempt writes the session whole.
func TestRewriteRetriesUntilWhole(t *testing.T) {
	f := newRollingFixture(t, nil)
	s := f.injectedQueued(t)
	base := f.d.Calls()
	f.d.Before = func(call int, _ string) {
		if call == base+2 {
			f.d.FailNext(1)
		}
	}
	f.clock.advance(pubrelRetry)
	f.d.Before = nil
	s.mu.Lock()
	marked := s.unwritten
	s.mu.Unlock()
	if !marked {
		t.Fatal("precondition: the first rewrite did not fail partway")
	}
	f.clock.advance(pubrelRetry)
	s.mu.Lock()
	marked = s.unwritten
	s.mu.Unlock()
	if marked {
		t.Fatal("the second rewrite left the session marked")
	}
	f.recoveredOnce(t)
}

// TestQoS2PublishWhoseIdentifierFailsIsRefused: a QoS 2 PUBLISH from a durable
// session whose inbound identifier fails to append is refused, and the
// identifier is released, so the client's retry under it is routed as a new
// message rather than dropped as a duplicate.
func TestQoS2PublishWhoseIdentifierFailsIsRefused(t *testing.T) {
	f := newRollingFixture(t, nil)
	listener := newFake("listener", packet.V5, 100)
	connectV5(f.m, listener, true, 0, nil)
	subscribe(f.m, listener, 0, "q")
	f.d.FailNext(1)
	if code := publish(f.m, f.sub, "q", 2, 9, "first"); code != packet.ImplementationSpecificError {
		t.Fatalf("a QoS 2 publish whose identifier failed to append answered %v, want 0x83", code)
	}
	if code := publish(f.m, f.sub, "q", 2, 9, "retry"); code.IsError() {
		t.Fatalf("the retry under the same identifier answered %v", code)
	}
	var got []string
	for _, p := range listener.publishes() {
		got = append(got, string(p.Payload))
	}
	if len(got) == 0 || got[len(got)-1] != "retry" {
		t.Fatalf("the listener received %v; want the retry routed", got)
	}
}

// TestRewriteStopsForATakenSession: a marked session taken by another node
// before its rewrite is not written here; the new owner writes it whole.
func TestRewriteStopsForATakenSession(t *testing.T) {
	f := newRollingFixture(t, nil)
	f.injectedQueued(t)
	if _, ok := f.m.Take("default", "sub", packet.SessionTakenOver); !ok {
		t.Fatal("precondition: the session was not taken")
	}
	before := f.st.LastIndex()
	f.clock.advance(pubrelRetry)
	if after := f.st.LastIndex(); after != before {
		t.Fatalf("the journal moved from %d to %d after the session was taken: the rewrite wrote it", before, after)
	}
}

// TestHeldPubrelWaitsForItsOwnRewrite: the retry writes the session whole
// itself before it checks the PUBREC state is durable, whatever else is armed
// to: a real clock runs timers due together in any order.
func TestHeldPubrelWaitsForItsOwnRewrite(t *testing.T) {
	f := newRollingFixture(t, nil)
	x := f.delivered(t, 2, 1, "exactly-once")
	sh := f.m.shardFor(key{"default", "sub"})
	sh.mu.Lock()
	s := sh.sessions[key{"default", "sub"}]
	sh.mu.Unlock()
	s.mu.Lock()
	s.rewriteArmed = true // no separate rewrite is armed by the failure below
	s.mu.Unlock()
	f.d.FailNext(1)
	if _, send := f.m.Pubrec(f.sub, x, packet.Success); send {
		t.Fatal("precondition: the PUBREL was not held")
	}
	f.clock.advance(pubrelRetry)
	ss := recovered(t, f.dir, f.st).Find("default", "sub")
	if ss == nil {
		t.Fatal("the recovered journal has no session sub")
	}
	for _, e := range ss.Entries {
		if e.PacketID == x && e.State == store.StatePubrecReceived {
			if got := pubrels(f.sub); len(got) != 1 || got[0] != x {
				t.Fatalf("PUBRELs sent %v, want exactly [%d]", got, x)
			}
			return
		}
	}
	t.Fatalf("the PUBREL went out (%v) with the recovered journal lacking entry %d at PUBREC received: %+v", pubrels(f.sub), x, ss.Entries)
}
