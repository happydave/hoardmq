package session

import (
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// pubcomp is one PUBCOMP a connection was sent.
type pubcomp struct {
	id   uint16
	code packet.ReasonCode
}

func pubcomps(c *fakeConn) []pubcomp {
	var out []pubcomp
	for _, p := range c.packets() {
		if r, ok := p.(*packet.Pubcomp); ok {
			out = append(out, pubcomp{r.PacketID, r.ReasonCode})
		}
	}
	return out
}

// pending is how many of the clock's timers have neither fired nor stopped.
func (c *fakeClock) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped && !t.fired {
			n++
		}
	}
	return n
}

// heldEnv is a manager whose replication's durability wait fails while down
// is set, with a durable publisher "pub" that has completed the first half of
// a QoS 2 exchange under identifier 5.
func heldEnv(t *testing.T) (*Manager, *fakeClock, *switchReplication, *fakeConn) {
	t.Helper()
	rep := &switchReplication{}
	m, clock := newManager(t, nil)
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	if code := publish(m, pub, "t", 2, 5, "exactly-once"); code.IsError() {
		t.Fatalf("precondition: the QoS 2 publish answered %v", code)
	}
	return m, clock, rep, pub
}

// TestPubcompFollowsADurableRelease: the PUBCOMP goes out only once the
// identifier's release is synced, so a power cut after it leaves the
// identifier closed and the client's next QoS 2 message under it is
// delivered, not answered as a duplicate and dropped (WI 2031).
func TestPubcompFollowsADurableRelease(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(dir, store.Options{Logger: quiet, Sync: d.Sync})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 3600, nil)
	subscribe(m, sub, 1, "t")
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 3600, nil)
	if code := publish(m, pub, "t", 2, 7, "first"); code.IsError() {
		t.Fatalf("precondition: the first publish answered %v", code)
	}
	if code, send := m.Pubrel(pub, 7); code != packet.Success || !send {
		t.Fatalf("PUBREL answered %v, send %v; want Success, sent", code, send)
	}

	d.Crash(t)
	again, err := store.Open(dir, store.Options{Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Close() })
	rec, err := again.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if ps := rec.Find("default", "pub"); ps == nil || slices.Contains(ps.InboundQoS2, 7) {
		t.Fatalf("after a power cut that followed the PUBCOMP, the recovered session %+v has identifier 7 open", ps)
	}
	m2, _ := newManager(t, func(o *Options) { o.Journal = again })
	m2.Load(rec)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(m2, sub2, false, 3600, nil)
	pub2 := newFake("pub", packet.V5, 100)
	connectV5(m2, pub2, false, 3600, nil)
	if code := publish(m2, pub2, "t", 2, 7, "second"); code.IsError() {
		t.Fatalf("the next QoS 2 publish under 7 answered %v", code)
	}
	if n := countPayload(sub2.publishes(), "second"); n != 1 {
		t.Fatalf("the next QoS 2 message under 7 was delivered %d times, want once", n)
	}
}

// TestPubcompHeldUntilTheReleaseIsReplicated: with the replicas not holding
// the release, no PUBCOMP goes out; once they do, the retry sends one.
func TestPubcompHeldUntilTheReleaseIsReplicated(t *testing.T) {
	m, clock, rep, pub := heldEnv(t)
	rep.down.Store(true)
	if code, send := m.Pubrel(pub, 5); code != packet.Success || send {
		t.Fatalf("PUBREL with the replicas down answered %v, send %v; want Success, held", code, send)
	}
	clock.advance(pubrelRetry)
	if got := pubcomps(pub); len(got) != 0 {
		t.Fatalf("PUBCOMPs %v sent while the replicas do not hold the release", got)
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if got := pubcomps(pub); !slices.Equal(got, []pubcomp{{5, packet.Success}}) {
		t.Fatalf("after the replicas took the release the PUBCOMPs sent are %v, want exactly [5 Success]", got)
	}
}

// TestPubcompHeldUntilAnUnwrittenReleaseIsRewritten: the release's append
// fails, so the journal lacks it; the PUBCOMP waits for the retry's own
// rewrite of the session, and the recovered journal then has the identifier
// closed.
func TestPubcompHeldUntilAnUnwrittenReleaseIsRewritten(t *testing.T) {
	f := newRollingFixture(t, nil)
	if code := publish(f.m, f.sub, "q", 2, 9, "x"); code.IsError() {
		t.Fatalf("precondition: the QoS 2 publish answered %v", code)
	}
	sh := f.m.shardFor(key{"default", "sub"})
	sh.mu.Lock()
	s := sh.sessions[key{"default", "sub"}]
	sh.mu.Unlock()
	s.mu.Lock()
	s.rewriteArmed = true // no separate rewrite is armed by the failure below
	s.mu.Unlock()
	f.d.FailNext(1)
	if _, send := f.m.Pubrel(f.sub, 9); send {
		t.Fatal("PUBREL whose release failed to append was answered at once")
	}
	f.clock.advance(pubrelRetry)
	got := pubcomps(f.sub)
	ss := recovered(t, f.dir, f.st).Find("default", "sub")
	if ss == nil {
		t.Fatal("the recovered journal has no session sub")
	}
	if slices.Contains(ss.InboundQoS2, 9) {
		t.Fatalf("PUBCOMPs %v sent with the recovered journal still holding identifier 9 open", got)
	}
	if !slices.Equal(got, []pubcomp{{9, packet.Success}}) {
		t.Fatalf("after the rewrite the PUBCOMPs sent are %v, want exactly [9 Success]", got)
	}
}

// TestPubcompWaitsForARewriteThatFailsOnce: the retry's own rewrite fails
// too, so the retry tries again, and the PUBCOMP follows the rewrite that
// succeeds.
func TestPubcompWaitsForARewriteThatFailsOnce(t *testing.T) {
	f := newRollingFixture(t, nil)
	if code := publish(f.m, f.sub, "q", 2, 9, "x"); code.IsError() {
		t.Fatalf("precondition: the QoS 2 publish answered %v", code)
	}
	sh := f.m.shardFor(key{"default", "sub"})
	sh.mu.Lock()
	s := sh.sessions[key{"default", "sub"}]
	sh.mu.Unlock()
	s.mu.Lock()
	s.rewriteArmed = true // no separate rewrite is armed by the failures below
	s.mu.Unlock()
	f.d.FailNext(2) // the release, then the first rewrite's first record
	if _, send := f.m.Pubrel(f.sub, 9); send {
		t.Fatal("precondition: the PUBCOMP was not held")
	}
	f.clock.advance(pubrelRetry)
	if got := pubcomps(f.sub); len(got) != 0 {
		t.Fatalf("PUBCOMPs %v sent after a rewrite that failed", got)
	}
	f.clock.advance(pubrelRetry)
	got := pubcomps(f.sub)
	ss := recovered(t, f.dir, f.st).Find("default", "sub")
	if ss == nil || slices.Contains(ss.InboundQoS2, 9) {
		t.Fatalf("PUBCOMPs %v with the recovered session %+v not showing identifier 9 released", got, ss)
	}
	if !slices.Equal(got, []pubcomp{{9, packet.Success}}) {
		t.Fatalf("after the second rewrite the PUBCOMPs sent are %v, want exactly [9 Success]", got)
	}
}

// TestPubcompNotSentToAConnectionReplacedDuringItsWait: the client
// reconnects while the retry waits for the replicas; the PUBCOMP held for the
// old connection is not sent to it once the wait passes.
func TestPubcompNotSentToAConnectionReplacedDuringItsWait(t *testing.T) {
	m, clock, rep, pub := heldEnv(t)
	rep.down.Store(true)
	if _, send := m.Pubrel(pub, 5); send {
		t.Fatal("precondition: the PUBCOMP was not held with the replicas down")
	}
	again := newFake("pub", packet.V5, 100)
	rep.during = func() {
		rep.during = nil
		if !connectV5(m, again, false, 600, nil) {
			t.Error("precondition: the session was not present on reconnect")
		}
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if rep.during != nil {
		t.Fatal("precondition: the retry did not wait for the replicas")
	}
	if got := pubcomps(pub); len(got) != 0 {
		t.Fatalf("the connection replaced during the wait was sent PUBCOMPs %v", got)
	}
}

// TestResentPubrelWaitsForTheRelease: a PUBCOMP held for a connection that
// has gone is dropped, not sent; the client's resent PUBREL on its next
// connection finds the identifier closed and waits for the same release
// before answering not found.
func TestResentPubrelWaitsForTheRelease(t *testing.T) {
	m, clock, rep, pub := heldEnv(t)
	rep.down.Store(true)
	if _, send := m.Pubrel(pub, 5); send {
		t.Fatal("precondition: the PUBCOMP was not held with the replicas down")
	}
	again := newFake("pub", packet.V5, 100)
	if !connectV5(m, again, false, 600, nil) {
		t.Fatal("precondition: the session was not present on reconnect")
	}
	clock.advance(pubrelRetry)
	if code, send := m.Pubrel(again, 5); code != packet.PacketIdentifierNotFound || send {
		t.Fatalf("the resent PUBREL with the replicas down answered %v, send %v; want not found, held", code, send)
	}
	clock.advance(pubrelRetry)
	if got := append(pubcomps(pub), pubcomps(again)...); len(got) != 0 {
		t.Fatalf("PUBCOMPs %v sent while the replicas do not hold the release", got)
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if got := pubcomps(again); !slices.Equal(got, []pubcomp{{5, packet.PacketIdentifierNotFound}}) {
		t.Fatalf("the new connection was sent PUBCOMPs %v, want exactly [5 not found]", got)
	}
	if got := pubcomps(pub); len(got) != 0 {
		t.Fatalf("the old connection was sent PUBCOMPs %v", got)
	}
}

// TestPubrelResentBeforeTheRetryIsAnsweredOnTheNewConnection: the client
// reconnects and resends the PUBREL before the retry has dropped the hold
// for its old connection; the PUBCOMP goes to the new connection once the
// release is durable, and none to the old.
func TestPubrelResentBeforeTheRetryIsAnsweredOnTheNewConnection(t *testing.T) {
	m, clock, rep, pub := heldEnv(t)
	rep.down.Store(true)
	if _, send := m.Pubrel(pub, 5); send {
		t.Fatal("precondition: the PUBCOMP was not held with the replicas down")
	}
	again := newFake("pub", packet.V5, 100)
	if !connectV5(m, again, false, 600, nil) {
		t.Fatal("precondition: the session was not present on reconnect")
	}
	if _, send := m.Pubrel(again, 5); send {
		t.Fatal("the resent PUBREL was answered at once with the replicas down")
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	clock.advance(pubrelRetry)
	if got := pubcomps(again); !slices.Equal(got, []pubcomp{{5, packet.Success}}) {
		t.Fatalf("the new connection was sent PUBCOMPs %v, want exactly [5 Success]", got)
	}
	if got := pubcomps(pub); len(got) != 0 {
		t.Fatalf("the old connection was sent PUBCOMPs %v", got)
	}
}

// TestHeldPubcompOfADisconnectedClientIsDropped: the client disconnects
// while its PUBCOMP is held; once the release is durable nothing is sent to
// the closed connection, and no retry stays armed.
func TestHeldPubcompOfADisconnectedClientIsDropped(t *testing.T) {
	m, clock, rep, pub := heldEnv(t)
	rep.down.Store(true)
	if _, send := m.Pubrel(pub, 5); send {
		t.Fatal("precondition: the PUBCOMP was not held with the replicas down")
	}
	m.Disconnect(pub, false, packet.NormalDisconnection, false)
	armed := clock.pending() // the disconnect's own timers, and the retry
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if got := pubcomps(pub); len(got) != 0 {
		t.Fatalf("the disconnected client's connection was sent PUBCOMPs %v", got)
	}
	if n := clock.pending(); n != armed-1 {
		t.Fatalf("%d timers armed after the retry ran, want %d: the retry not re-armed", n, armed-1)
	}
}

// TestPubrelOnADetachedConnectionIsAnsweredAtOnce: a PUBREL arriving on a
// connection whose session was taken from this node has nothing here to wait
// for; it is answered at once, as before.
func TestPubrelOnADetachedConnectionIsAnsweredAtOnce(t *testing.T) {
	m, _, rep, pub := heldEnv(t)
	if _, ok := m.Take("default", "pub", packet.SessionTakenOver); !ok {
		t.Fatal("precondition: the session was not taken")
	}
	rep.down.Store(true)
	if code, send := m.Pubrel(pub, 5); code != packet.UnspecifiedError || !send {
		t.Fatalf("PUBREL on a detached connection answered %v, send %v; want UnspecifiedError, sent", code, send)
	}
}

// TestOnePubcompPerHold: a second PUBREL for an identifier whose PUBCOMP is
// held sends nothing more; one PUBCOMP follows the release.
func TestOnePubcompPerHold(t *testing.T) {
	m, clock, rep, pub := heldEnv(t)
	rep.down.Store(true)
	for i := 0; i < 2; i++ {
		if _, send := m.Pubrel(pub, 5); send {
			t.Fatalf("PUBREL %d was answered at once with the replicas down", i+1)
		}
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	clock.advance(pubrelRetry)
	if got := pubcomps(pub); !slices.Equal(got, []pubcomp{{5, packet.Success}}) {
		t.Fatalf("PUBCOMPs sent %v, want exactly [5 Success]", got)
	}
}

// TestHeldPubcompStopsForATakenSession: once the session is taken, a held
// PUBCOMP is not sent here and its retry stops; the new owner answers the
// client's resent PUBREL.
func TestHeldPubcompStopsForATakenSession(t *testing.T) {
	m, clock, rep, pub := heldEnv(t)
	armed := clock.pending()
	rep.down.Store(true)
	if _, send := m.Pubrel(pub, 5); send {
		t.Fatal("precondition: the PUBCOMP was not held with the replicas down")
	}
	if _, ok := m.Take("default", "pub", packet.SessionTakenOver); !ok {
		t.Fatal("precondition: the session was not taken")
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if got := pubcomps(pub); len(got) != 0 {
		t.Fatalf("PUBCOMPs %v sent for a session taken from this node", got)
	}
	if n := clock.pending(); n != armed {
		t.Fatalf("%d timers armed after the session was taken and the retry ran, want the %d armed before the hold", n, armed)
	}
}
