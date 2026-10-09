package session

import (
	"io"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// pubrec is one PUBREC a connection was sent.
type pubrec struct {
	id   uint16
	code packet.ReasonCode
}

func pubrecs(c *fakeConn) []pubrec {
	var out []pubrec
	for _, p := range c.packets() {
		if r, ok := p.(*packet.Pubrec); ok {
			out = append(out, pubrec{r.PacketID, r.ReasonCode})
		}
	}
	return out
}

// publishQoS2 sends a QoS 2 PUBLISH on t and returns the reason and whether
// the connection is to send the PUBREC now.
func publishQoS2(m *Manager, c *fakeConn, id uint16, dup bool, payload string) (packet.ReasonCode, bool) {
	return m.Publish(c, &packet.Publish{Topic: "t", QoS: 2, PacketID: id, Dup: dup, Payload: []byte(payload)})
}

// refused is the reason the switchable replication's failed barrier gives a
// publish.
const refused = packet.ImplementationSpecificError

// replicaEnv is a session manager over a real store whose tap applies every
// record to a second state, as a follower applies the frames it is sent,
// until withhold is set: from then on the records wait, as the frames a dying
// owner had not yet sent, until deliver applies them in order. Its
// replication's barrier and durability wait fail while down is set.
type replicaEnv struct {
	st       *store.Store
	m        *Manager
	clock    *fakeClock
	rep      *switchReplication
	replica  *store.State
	withhold atomic.Bool
	waiting  []store.Record
}

func newReplicaEnv(t *testing.T) *replicaEnv {
	t.Helper()
	e := &replicaEnv{replica: store.NewState(), rep: &switchReplication{}}
	tap := func(r store.Record) func() {
		if e.withhold.Load() {
			e.waiting = append(e.waiting, store.Record{Type: r.Type, Payload: slices.Clone(r.Payload)})
			return nil
		}
		e.apply(t, r)
		return nil
	}
	st, err := store.Open(t.TempDir(), store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Tap: tap})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	e.st = st
	e.m, e.clock = newManager(t, func(o *Options) { o.Journal = st })
	e.m.SetReplication(e.rep)
	return e
}

func (e *replicaEnv) apply(t *testing.T, r store.Record) {
	t.Helper()
	if _, err := e.replica.ApplyReport(r.Type, r.Payload); err != nil {
		t.Errorf("replica apply %s: %v", store.RecordName(r.Type), err)
	}
}

// deliver applies the withheld records in order and stops withholding.
func (e *replicaEnv) deliver(t *testing.T) {
	t.Helper()
	e.withhold.Store(false)
	for _, r := range e.waiting {
		e.apply(t, r)
	}
	e.waiting = nil
}

// takenOver installs the replica's copy of "pub" on a new manager where "sub"
// is connected and subscribed to "t" at QoS 1, as a takeover from that
// replica does after the owner died, and reconnects "pub" there.
func (e *replicaEnv) takenOver(t *testing.T) (m2 *Manager, sub, pub *fakeConn) {
	t.Helper()
	m2, _ = newManager(t, nil)
	sub = newFake("sub", packet.V5, 100)
	connectV5(m2, sub, true, 0, nil)
	subscribe(m2, sub, 1, "t")
	ss := e.replica.Find("default", "pub")
	if ss == nil {
		t.Fatal("precondition: the replica holds no copy of pub")
	}
	m2.Import(ss)
	pub = newFake("pub", packet.V5, 100)
	if present := connectV5(m2, pub, false, 3600, nil); !present {
		t.Fatal("precondition: pub's session is not present after the takeover")
	}
	return m2, sub, pub
}

// TestRefusedQoS2ReleaseReachesTheReplicaBeforeItsPubrec: a QoS 2 publish
// refused with an error PUBREC releases its identifier, and the client may
// then send a new message under it. So the PUBREC waits until the replica
// holds the release: a takeover from it then delivers the client's next
// message under the identifier rather than answering it as a duplicate and
// dropping it.
func TestRefusedQoS2ReleaseReachesTheReplicaBeforeItsPubrec(t *testing.T) {
	e := newReplicaEnv(t)
	pub := newFake("pub", packet.V5, 100)
	connectV5(e.m, pub, false, 3600, nil)
	e.rep.down.Store(true)
	e.rep.during = func() { e.withhold.Store(true) }
	code, send := publishQoS2(e.m, pub, 9, false, "refused")
	if !code.IsError() {
		t.Fatalf("precondition: the QoS 2 publish answered %v, want an error", code)
	}
	if ps := e.replica.Find("default", "pub"); ps == nil || !slices.Contains(ps.InboundQoS2, 9) {
		t.Fatalf("precondition: the replica's copy %+v does not have identifier 9 open", ps)
	}
	if send {
		t.Fatal("the error PUBREC was to go out at once with the release not on the replica")
	}
	e.clock.advance(pubrelRetry)
	if got := pubrecs(pub); len(got) != 0 {
		t.Fatalf("PUBRECs %v sent with the release not on the replica", got)
	}

	e.rep.during = nil
	e.deliver(t)
	e.rep.down.Store(false)
	e.clock.advance(pubrelRetry)
	if got := pubrecs(pub); !slices.Equal(got, []pubrec{{9, code}}) {
		t.Fatalf("once the replica held the release the PUBRECs sent are %v, want exactly [9 %v]", got, code)
	}
	m2, sub, pub2 := e.takenOver(t)
	if code, _ := publishQoS2(m2, pub2, 9, false, "next"); code.IsError() {
		t.Fatalf("the client's next QoS 2 publish under 9 answered %v after the takeover", code)
	}
	if n := countPayload(sub.publishes(), "next"); n != 1 {
		t.Fatalf("the client's next QoS 2 message under 9 was delivered %d times after the takeover, want once", n)
	}
}

// TestRefusedQoS2ReleaseIsSyncedBeforeItsPubrec: the release's sync fails, so
// the error PUBREC waits for the retry's sync; a power cut after it leaves the
// identifier closed, and the client's next QoS 2 message under it is
// delivered.
func TestRefusedQoS2ReleaseIsSyncedBeforeItsPubrec(t *testing.T) {
	dir := t.TempDir()
	d := faultdisk.New()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(dir, store.Options{Logger: quiet, Sync: d.Sync})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rep := &switchReplication{}
	m, clock := newManager(t, func(o *Options) { o.Journal = st })
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 3600, nil)
	rep.down.Store(true)
	rep.during = func() {
		rep.during = nil
		d.FailNext(1) // the next sync is the release's
	}
	code, send := publishQoS2(m, pub, 9, false, "refused")
	if !code.IsError() {
		t.Fatalf("precondition: the QoS 2 publish answered %v, want an error", code)
	}
	if send {
		t.Fatal("the error PUBREC was to go out at once over a release whose sync failed")
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if got := pubrecs(pub); !slices.Equal(got, []pubrec{{9, code}}) {
		t.Fatalf("after the retry the PUBRECs sent are %v, want exactly [9 %v]", got, code)
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
	if ps := rec.Find("default", "pub"); ps == nil || slices.Contains(ps.InboundQoS2, 9) {
		t.Fatalf("after a power cut that followed the error PUBREC, the recovered session %+v has identifier 9 open", ps)
	}
	m2, _ := newManager(t, func(o *Options) { o.Journal = again })
	m2.Load(rec)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m2, sub, true, 0, nil)
	subscribe(m2, sub, 1, "t")
	pub2 := newFake("pub", packet.V5, 100)
	connectV5(m2, pub2, false, 3600, nil)
	if code, _ := publishQoS2(m2, pub2, 9, false, "next"); code.IsError() {
		t.Fatalf("the client's next QoS 2 publish under 9 answered %v", code)
	}
	if n := countPayload(sub.publishes(), "next"); n != 1 {
		t.Fatalf("after a power cut, the client's next QoS 2 message under 9 was delivered %d times, want once", n)
	}
}

// heldPubrecEnv is a manager whose replication's barrier and durability wait
// fail while down is set, with a durable publisher "pub" whose QoS 2 publish
// under 9 was refused and whose error PUBREC is held, and a subscriber "sub"
// to t at QoS 1.
func heldPubrecEnv(t *testing.T) (*Manager, *fakeClock, *switchReplication, *fakeConn, *fakeConn) {
	t.Helper()
	rep := &switchReplication{}
	m, clock := newManager(t, nil)
	m.SetReplication(rep)
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, true, 0, nil)
	subscribe(m, sub, 1, "t")
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	rep.down.Store(true)
	if code, send := publishQoS2(m, pub, 9, false, "refused"); code != refused || send {
		t.Fatalf("precondition: the QoS 2 publish answered %v, send %v; want %v, held", code, send, refused)
	}
	return m, clock, rep, pub, sub
}

// unwrittenRelease is a rolling fixture with replication attached whose
// subscriber "sub" publishes at QoS 2 under 9 and is refused, the release's
// append failing (failures counts it and the disk calls after it), so the
// session is marked for a rewrite. No other rewrite is armed.
func unwrittenRelease(t *testing.T, failures int) (*rollingFixture, *switchReplication) {
	t.Helper()
	rep := &switchReplication{}
	f := newRollingFixture(t, nil)
	f.m.SetReplication(rep)
	sh := f.m.shardFor(key{"default", "sub"})
	sh.mu.Lock()
	s := sh.sessions[key{"default", "sub"}]
	sh.mu.Unlock()
	s.mu.Lock()
	s.rewriteArmed = true // no separate rewrite is armed by the failure below
	s.mu.Unlock()
	// The barrier fails; the release's own wait finds the replicas up, so
	// only the failed append can hold the PUBREC.
	rep.down.Store(true)
	waits := 0
	rep.during = func() {
		if waits++; waits == 1 {
			f.d.FailNext(failures) // the next disk call is the release's append
		} else {
			rep.down.Store(false)
		}
	}
	if code, send := f.m.Publish(f.sub, &packet.Publish{Topic: "q", QoS: 2, PacketID: 9, Payload: []byte("x")}); code != refused || send {
		t.Fatalf("the refused QoS 2 publish whose release failed to append answered %v, send %v; want %v, held", code, send, refused)
	}
	return f, rep
}

// TestRefusedQoS2PubrecWaitsForAnUnwrittenReleaseToBeRewritten: the release's
// append fails, so the journal lacks it; the error PUBREC waits for the
// retry's own rewrite of the session, and the recovered journal then has the
// identifier closed.
func TestRefusedQoS2PubrecWaitsForAnUnwrittenReleaseToBeRewritten(t *testing.T) {
	f, _ := unwrittenRelease(t, 1)
	f.clock.advance(pubrelRetry)
	got := pubrecs(f.sub)
	ss := recovered(t, f.dir, f.st).Find("default", "sub")
	if ss == nil {
		t.Fatal("the recovered journal has no session sub")
	}
	if slices.Contains(ss.InboundQoS2, 9) {
		t.Fatalf("PUBRECs %v sent with the recovered journal still holding identifier 9 open", got)
	}
	if !slices.Equal(got, []pubrec{{9, refused}}) {
		t.Fatalf("after the rewrite the PUBRECs sent are %v, want exactly [9 %v]", got, refused)
	}
}

// TestRefusedQoS2PubrecWaitsForARewriteThatFailsOnce: the retry's own rewrite
// fails too, so the retry tries again, and the error PUBREC follows the
// rewrite that succeeds.
func TestRefusedQoS2PubrecWaitsForARewriteThatFailsOnce(t *testing.T) {
	f, _ := unwrittenRelease(t, 2) // the release, then the first rewrite's first record
	f.clock.advance(pubrelRetry)
	if got := pubrecs(f.sub); len(got) != 0 {
		t.Fatalf("PUBRECs %v sent after a rewrite that failed", got)
	}
	f.clock.advance(pubrelRetry)
	got := pubrecs(f.sub)
	ss := recovered(t, f.dir, f.st).Find("default", "sub")
	if ss == nil || slices.Contains(ss.InboundQoS2, 9) {
		t.Fatalf("PUBRECs %v with the recovered session %+v not showing identifier 9 released", got, ss)
	}
	if !slices.Equal(got, []pubrec{{9, refused}}) {
		t.Fatalf("after the second rewrite the PUBRECs sent are %v, want exactly [9 %v]", got, refused)
	}
}

// TestHeldPubrecOfADisconnectedClientIsDropped: the client disconnects while
// its error PUBREC is held; once the release is durable nothing is sent to the
// closed connection, and no retry stays armed.
func TestHeldPubrecOfADisconnectedClientIsDropped(t *testing.T) {
	m, clock, rep, pub, _ := heldPubrecEnv(t)
	m.Disconnect(pub, false, packet.NormalDisconnection, false)
	armed := clock.pending() // the disconnect's own timers, and the retry
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if got := pubrecs(pub); len(got) != 0 {
		t.Fatalf("the disconnected client's connection was sent PUBRECs %v", got)
	}
	if n := clock.pending(); n != armed-1 {
		t.Fatalf("%d timers armed after the retry ran, want %d: the retry not re-armed", n, armed-1)
	}
}

// TestRetransmissionAfterAHeldPubrec: the client never received a PUBREC for
// its refused publish, so on its next connection it retransmits the PUBLISH
// with DUP set. That is a new attempt, whether the retry held for the old
// connection has run (dropping its hold) or not; it is dispatched again and
// answered on the new connection only, and the old connection gets nothing.
func TestRetransmissionAfterAHeldPubrec(t *testing.T) {
	for _, order := range []string{"after the retry", "before the retry"} {
		for _, outcome := range []string{"refused again", "accepted"} {
			t.Run(order+", "+outcome, func(t *testing.T) {
				m, clock, rep, pub, sub := heldPubrecEnv(t)
				again := newFake("pub", packet.V5, 100)
				if !connectV5(m, again, false, 600, nil) {
					t.Fatal("precondition: the session was not present on reconnect")
				}
				if order == "after the retry" {
					clock.advance(pubrelRetry)
				}
				before := countPayload(sub.publishes(), "refused")
				rep.down.Store(outcome == "refused again")
				code, send := publishQoS2(m, again, 9, true, "refused")
				if n := countPayload(sub.publishes(), "refused") - before; n != 1 {
					t.Fatalf("the retransmission was dispatched %d times, want once: a new attempt", n)
				}
				rep.down.Store(false)
				clock.advance(pubrelRetry)
				clock.advance(pubrelRetry)
				if got := pubrecs(pub); len(got) != 0 {
					t.Fatalf("the old connection was sent PUBRECs %v", got)
				}
				switch outcome {
				case "refused again":
					if code != refused || send {
						t.Fatalf("the retransmission refused again answered %v, send %v; want %v, held", code, send, refused)
					}
					if got := pubrecs(again); !slices.Equal(got, []pubrec{{9, refused}}) {
						t.Fatalf("the new connection was sent PUBRECs %v, want exactly [9 %v]", got, refused)
					}
				case "accepted":
					if code.IsError() || !send {
						t.Fatalf("the accepted retransmission answered %v, send %v; want success, sent", code, send)
					}
					if got := pubrecs(again); len(got) != 0 {
						t.Fatalf("after the accepted retransmission the new connection was sent error PUBRECs %v", got)
					}
				}
			})
		}
	}
}

// TestHeldPubrecNotSentToAConnectionReplacedDuringItsWait: the client
// reconnects while the retry waits for the replicas; the error PUBREC held for
// the old connection is not sent to it once the wait passes.
func TestHeldPubrecNotSentToAConnectionReplacedDuringItsWait(t *testing.T) {
	m, clock, rep, pub, _ := heldPubrecEnv(t)
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
	if got := append(pubrecs(pub), pubrecs(again)...); len(got) != 0 {
		t.Fatalf("PUBRECs %v sent after the connection was replaced during the wait", got)
	}
}

// TestANewOpeningClearsAHeldPubrec: a second PUBLISH under an identifier
// whose error PUBREC is held, on the same connection, is a new opening; once
// it is accepted, the stale error PUBREC is never sent after it.
func TestANewOpeningClearsAHeldPubrec(t *testing.T) {
	m, clock, rep, pub, _ := heldPubrecEnv(t)
	rep.down.Store(false)
	if code, send := publishQoS2(m, pub, 9, false, "again"); code.IsError() || !send {
		t.Fatalf("precondition: the second publish under 9 answered %v, send %v; want success, sent", code, send)
	}
	clock.advance(pubrelRetry)
	if got := pubrecs(pub); len(got) != 0 {
		t.Fatalf("after an accepted exchange under 9 the connection was sent error PUBRECs %v", got)
	}
}

// TestHeldPubrecStopsForATakenSession: once the session is taken, a held error
// PUBREC is not sent here and its retry stops.
func TestHeldPubrecStopsForATakenSession(t *testing.T) {
	rep := &switchReplication{}
	m, clock := newManager(t, nil)
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	armed := clock.pending()
	rep.down.Store(true)
	if _, send := publishQoS2(m, pub, 9, false, "refused"); send {
		t.Fatal("precondition: the error PUBREC was not held with the replicas down")
	}
	if _, ok := m.Take("default", "pub", packet.SessionTakenOver); !ok {
		t.Fatal("precondition: the session was not taken")
	}
	rep.down.Store(false)
	clock.advance(pubrelRetry)
	if got := pubrecs(pub); len(got) != 0 {
		t.Fatalf("PUBRECs %v sent for a session taken from this node", got)
	}
	if n := clock.pending(); n != armed {
		t.Fatalf("%d timers armed after the session was taken and the retry ran, want the %d armed before the hold", n, armed)
	}
}

// TestOnePubrecPerHold: once the retry has sent the held error PUBREC, later
// intervals send nothing more.
func TestOnePubrecPerHold(t *testing.T) {
	_, clock, rep, pub, _ := heldPubrecEnv(t)
	rep.down.Store(false)
	for i := 0; i < 3; i++ {
		clock.advance(pubrelRetry)
	}
	if got := pubrecs(pub); !slices.Equal(got, []pubrec{{9, refused}}) {
		t.Fatalf("PUBRECs sent %v, want exactly [9 %v]", got, refused)
	}
}

// TestRefusalOnADetachedConnectionIsAnsweredAtOnce: a QoS 2 publish on a
// connection whose session was taken from this node has nothing here to
// release; it is answered at once.
func TestRefusalOnADetachedConnectionIsAnsweredAtOnce(t *testing.T) {
	rep := &switchReplication{}
	m, _ := newManager(t, nil)
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	if _, ok := m.Take("default", "pub", packet.SessionTakenOver); !ok {
		t.Fatal("precondition: the session was not taken")
	}
	rep.down.Store(true)
	if code, send := publishQoS2(m, pub, 9, false, "x"); !code.IsError() || !send {
		t.Fatalf("a QoS 2 publish on a detached connection answered %v, send %v; want an error, sent", code, send)
	}
}

// TestOnlyARefusedQoS2PublishWaits: a QoS 2 publish that succeeds waits for
// nothing beyond its own barrier, and a refused QoS 1 publish is answered at
// once.
func TestOnlyARefusedQoS2PublishWaits(t *testing.T) {
	rep := &switchReplication{}
	m, _ := newManager(t, nil)
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	waits := 0
	rep.during = func() { waits++ }
	if code, send := publishQoS2(m, pub, 9, false, "ok"); code.IsError() || !send {
		t.Fatalf("a QoS 2 publish with the replicas up answered %v, send %v; want success, sent", code, send)
	}
	if waits != 1 {
		t.Fatalf("a successful QoS 2 publish waited on the replicas %d times, want once: its barrier", waits)
	}
	rep.during = nil
	rep.down.Store(true)
	if code, send := m.Publish(pub, &packet.Publish{Topic: "t", QoS: 1, PacketID: 10, Payload: []byte("q1")}); code != refused || !send {
		t.Fatalf("a refused QoS 1 publish answered %v, send %v; want %v, sent", code, send, refused)
	}
}

// TestAHeldPubrecIsCounted: each error PUBREC held for its release adds one to
// hoardmq_pubrec_held_total, and to no other hold's count.
func TestAHeldPubrecIsCounted(t *testing.T) {
	metrics := &seriesMetrics{}
	rep := &switchReplication{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics })
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	rep.down.Store(true)
	if _, send := publishQoS2(m, pub, 9, false, "refused"); send {
		t.Fatal("precondition: the error PUBREC was not held with the replicas down")
	}
	got := metrics.snapshot()
	if n := got["hoardmq_pubrec_held_total{}"]; n != 1 {
		t.Fatalf("hoardmq_pubrec_held_total is %v after one held PUBREC, want 1 (series %v)", n, got)
	}
	if n := got["hoardmq_pubcomp_held_total{}"]; n != 0 {
		t.Fatalf("hoardmq_pubcomp_held_total is %v after a held PUBREC, want 0", n)
	}
}
