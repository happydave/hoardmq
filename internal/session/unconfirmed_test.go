package session

import (
	"bytes"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

func inboundRecordOf9(accepted bool) func(store.Record) bool {
	return func(r store.Record) bool {
		return store.RecordName(r.Type) == "inbound-add" && bytes.Contains(r.Payload, []byte(`"packetId":9`)) &&
			bytes.Contains(r.Payload, []byte(`"accepted":true`)) == accepted
	}
}

// takenOverAt publishes "m" at QoS 2 under 9 from the persistent "pub" to the
// offline durable "sub", on a manager over a real store whose tap feeds a
// replica state up to and including the first record cut accepts and
// withholds the rest, as the frames an owner that died had not yet sent. A
// second manager imports both sessions from that replica, as a takeover from
// it does, and both clients reconnect there.
func takenOverAt(t *testing.T, cut func(store.Record) bool, mutate func(*Options)) (m2 *Manager, clock2 *fakeClock, sub, pub *fakeConn) {
	t.Helper()
	replica := store.NewState()
	withheld := false
	tap := func(r store.Record) func() {
		if withheld {
			return nil
		}
		if _, err := replica.ApplyReport(r.Type, r.Payload); err != nil {
			t.Errorf("replica apply %s: %v", store.RecordName(r.Type), err)
		}
		withheld = cut(r)
		return nil
	}
	st, err := store.Open(t.TempDir(), store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Tap: tap})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	s1 := newFake("sub", packet.V5, 100)
	connectV5(m, s1, false, 3600, nil)
	subscribe(m, s1, 2, "t")
	m.Disconnect(s1, false, packet.NormalDisconnection, false)
	p1 := newFake("pub", packet.V5, 100)
	connectV5(m, p1, false, 3600, nil)
	if code := publish(m, p1, "t", 2, 9, "m"); code.IsError() {
		t.Fatalf("precondition: the first attempt answered %v", code)
	}
	if !withheld {
		t.Fatal("precondition: the replica was never cut")
	}

	m2, clock2 = newManager(t, mutate)
	for _, id := range []string{"sub", "pub"} {
		ss := replica.Find("default", id)
		if ss == nil {
			t.Fatalf("precondition: the replica holds no copy of %s", id)
		}
		m2.Import(ss)
	}
	sub = newFake("sub", packet.V5, 100)
	pub = newFake("pub", packet.V5, 100)
	for _, c := range []*fakeConn{sub, pub} {
		if !connectV5(m2, c, false, 3600, nil) {
			t.Fatalf("precondition: %s's session is not present after the takeover", c.id)
		}
	}
	return m2, clock2, sub, pub
}

func inboundOpen(m *Manager, c *fakeConn, id uint16) bool {
	s := m.lookup(c)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.inboundQoS2[id]
	return ok
}

// TestRetransmissionUnderAnUnconfirmedIdentifierIsRefused: the
// replica holds the opening of 9 and nothing the publish did after it. The
// client's retransmission is refused rather than answered as a duplicate, the
// identifier is released, and the client's next publish under it reaches the
// subscriber.
func TestRetransmissionUnderAnUnconfirmedIdentifierIsRefused(t *testing.T) {
	metrics := &seriesMetrics{}
	m2, _, sub, pub := takenOverAt(t, inboundRecordOf9(false), func(o *Options) { o.Metrics = metrics })
	if n := countPayload(sub.publishes(), "m"); n != 0 {
		t.Fatalf("precondition: sub was given %d copies of m from the replica; the dispatch was not withheld", n)
	}
	if code, send := publishQoS2(m2, pub, 9, true, "m"); code != refused || !send {
		t.Fatalf("the retransmission under 9 answered %v, send %v; want %v, sent", code, send, refused)
	}
	if inboundOpen(m2, pub, 9) {
		t.Fatal("9 is still open after its retransmission was refused")
	}
	if n := metrics.snapshot()["hoardmq_qos2_unconfirmed_refused_total{level=5}"]; n != 1 {
		t.Fatalf("hoardmq_qos2_unconfirmed_refused_total{level=5} is %v after one refusal, want 1", n)
	}
	if code, _ := publishQoS2(m2, pub, 9, false, "m"); code.IsError() {
		t.Fatalf("the client's next publish under 9 answered %v", code)
	}
	if n := countPayload(sub.publishes(), "m"); n != 1 {
		t.Fatalf("sub was given m %d times, want once", n)
	}
}

// TestRetransmissionUnderAnAcceptedIdentifierIsAbsorbedAfterATakeover:
// the replica holds the publish's acceptance, so the retransmission is a
// duplicate of an accepted message and is not routed again.
func TestRetransmissionUnderAnAcceptedIdentifierIsAbsorbedAfterATakeover(t *testing.T) {
	m2, _, sub, pub := takenOverAt(t, inboundRecordOf9(true), nil)
	if n := countPayload(sub.publishes(), "m"); n != 1 {
		t.Fatalf("precondition: sub was given %d copies of m from the replica, want 1", n)
	}
	if code, send := publishQoS2(m2, pub, 9, true, "m"); code != packet.Success || !send {
		t.Fatalf("the retransmission under 9 answered %v, send %v; want success, sent", code, send)
	}
	if n := countPayload(sub.publishes(), "m"); n != 1 {
		t.Fatalf("sub was given m %d times, want once", n)
	}
}

// TestPubrelUnderAnUnconfirmedIdentifierCompletes: a client that did get
// its PUBREC sends PUBREL, not PUBLISH, and the exchange completes.
func TestPubrelUnderAnUnconfirmedIdentifierCompletes(t *testing.T) {
	m2, _, _, pub := takenOverAt(t, inboundRecordOf9(false), nil)
	if code, send := m2.Pubrel(pub, 9); code != packet.Success || !send {
		t.Fatalf("PUBREL under 9 answered %v, send %v; want success, sent", code, send)
	}
	if inboundOpen(m2, pub, 9) {
		t.Fatal("9 is still open after its PUBREL")
	}
}

// TestRefusalOfAnUnconfirmedRetransmissionWaitsForItsRelease: the
// refusal releases the identifier, so its error PUBREC waits until the
// release is on the replicas, as every refused QoS 2 publish's does.
func TestRefusalOfAnUnconfirmedRetransmissionWaitsForItsRelease(t *testing.T) {
	m2, clock2, _, pub := takenOverAt(t, inboundRecordOf9(false), nil)
	rep := &switchReplication{}
	rep.down.Store(true)
	m2.SetReplication(rep)
	if code, send := publishQoS2(m2, pub, 9, true, "m"); code != refused || send {
		t.Fatalf("the retransmission under 9 answered %v, send %v; want %v, held", code, send, refused)
	}
	rep.down.Store(false)
	clock2.advance(pubrelRetry)
	if got := pubrecs(pub); !slices.Equal(got, []pubrec{{9, refused}}) {
		t.Fatalf("PUBRECs sent %v, want [9 %v] once the release is on the replicas", got, refused)
	}
}

func restartWith(t *testing.T, dir string) (*Manager, *store.Store) {
	t.Helper()
	st, err := store.Open(dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	state, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	m.Load(state)
	return m, st
}

// TestAcceptanceSurvivesARestart: an accepted publish's retransmission
// after a restart is absorbed as a duplicate.
func TestAcceptanceSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	sub := newFake("sub", packet.V5, 100)
	connectV5(m, sub, false, 3600, nil)
	subscribe(m, sub, 2, "t")
	m.Disconnect(sub, false, packet.NormalDisconnection, false)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 3600, nil)
	if code := publish(m, pub, "t", 2, 9, "m"); code.IsError() {
		t.Fatalf("precondition: the publish answered %v", code)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	m2, _ := restartWith(t, dir)
	sub2 := newFake("sub", packet.V5, 100)
	pub2 := newFake("pub", packet.V5, 100)
	for _, c := range []*fakeConn{sub2, pub2} {
		if !connectV5(m2, c, false, 3600, nil) {
			t.Fatalf("precondition: %s's session was not recovered", c.id)
		}
	}
	if code, _ := publishQoS2(m2, pub2, 9, true, "m"); code != packet.Success {
		t.Fatalf("the retransmission under 9 after a restart answered %v, want success", code)
	}
	if n := countPayload(sub2.publishes(), "m"); n != 1 {
		t.Fatalf("sub was given m %d times, want once", n)
	}
}

// TestOpeningWithoutAcceptanceRecoversUnconfirmed: a journal holding the
// opening of 9 and no acceptance, as a power cut can leave it, recovers 9 as
// unconfirmed, and the retransmission under it is refused.
func TestOpeningWithoutAcceptanceRecoversUnconfirmed(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	k := store.Key{Namespace: "default", ClientID: "pub"}
	for _, err := range []error{
		st.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: "pub", Version: byte(packet.V5), ExpiryInterval: 3600}),
		st.InboundQoS2(k, 9, true),
		st.Sync(),
		st.Close(),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	m2, _ := restartWith(t, dir)
	pub := newFake("pub", packet.V5, 100)
	if !connectV5(m2, pub, false, 3600, nil) {
		t.Fatal("precondition: pub's session was not recovered")
	}
	if code, _ := publishQoS2(m2, pub, 9, true, "m"); code != refused {
		t.Fatalf("the retransmission under a recovered opening answered %v, want %v", code, refused)
	}
}

// TestAWholeWriteKeepsTheAcceptance: an imported session is written
// whole, and the replicas it feeds keep its accepted identifiers accepted.
func TestAWholeWriteKeepsTheAcceptance(t *testing.T) {
	e := newReplicaEnv(t)
	e.m.Import(&store.SessionState{
		SessionMeta:     store.SessionMeta{Namespace: "default", ClientID: "pub", Version: byte(packet.V5), ExpiryInterval: 3600},
		Subscriptions:   []store.SubscriptionRecord{},
		InboundQoS2:     []uint16{9, 12},
		InboundAccepted: []uint16{9},
		Entries:         []store.Entry{},
	})
	got := e.replica.Find("default", "pub")
	if got == nil {
		t.Fatal("precondition: the whole write did not reach the replica")
	}
	if !slices.Equal(got.InboundQoS2, []uint16{9, 12}) && !slices.Equal(got.InboundQoS2, []uint16{12, 9}) {
		t.Fatalf("precondition: the replica holds %v open, want 9 and 12", got.InboundQoS2)
	}
	if !slices.Equal(got.InboundAccepted, []uint16{9}) {
		t.Fatalf("the replica holds %v accepted after the whole write, want [9]", got.InboundAccepted)
	}
}

// TestRetransmissionDuringTheFirstAttemptAnswersWithItsVerdict: the
// client reconnects to this node while its first attempt's barrier runs and
// retransmits. The retransmission is answered with the first attempt's
// verdict, once it is reached, and a refused attempt's identifier is released
// although its own connection has gone.
func TestRetransmissionDuringTheFirstAttemptAnswersWithItsVerdict(t *testing.T) {
	for _, c := range []struct {
		name string
		down bool
	}{{"refused", true}, {"accepted", false}} {
		t.Run(c.name, func(t *testing.T) {
			rep := &switchReplication{}
			m, _ := newManager(t, nil)
			m.SetReplication(rep)
			pub := newFake("pub", packet.V5, 100)
			connectV5(m, pub, false, 600, nil)
			again := newFake("pub", packet.V5, 100)
			answered := make(chan packet.ReasonCode, 1)
			rep.during = func() {
				rep.during = nil
				if !connectV5(m, again, false, 600, nil) {
					t.Error("precondition: the session was not present on reconnect")
				}
				go func() {
					code, _ := publishQoS2(m, again, 9, true, "m")
					answered <- code
				}()
				select {
				case code := <-answered:
					t.Errorf("the retransmission was answered %v while the first attempt's barrier ran", code)
				case <-time.After(200 * time.Millisecond):
				}
				rep.down.Store(c.down)
			}
			first, send := publishQoS2(m, pub, 9, false, "m")
			if rep.during != nil {
				t.Fatal("precondition: the first attempt did not wait on the barrier")
			}
			if c.down != first.IsError() {
				t.Fatalf("precondition: the first attempt answered %v with the replicas down %v", first, c.down)
			}
			if !send {
				t.Error("the first attempt's answer, on a connection that has gone, was held")
			}
			select {
			case code := <-answered:
				if code != first {
					t.Fatalf("the retransmission answered %v, want the first attempt's %v", code, first)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the retransmission was never answered")
			}
			if c.down && inboundOpen(m, again, 9) {
				t.Fatal("9 is still open after the attempt holding it was refused")
			}
		})
	}
}

// TestATakenSessionCarriesItsAcceptance: the state handed to a takeover
// lists an accepted identifier as accepted, so the new owner absorbs its
// retransmission.
func TestATakenSessionCarriesItsAcceptance(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	if code, _ := publishQoS2(m, pub, 9, false, "m"); code.IsError() {
		t.Fatalf("precondition: the publish answered %v", code)
	}
	ss, ok := m.Take("default", "pub", packet.SessionTakenOver)
	if !ok {
		t.Fatal("precondition: the session was not taken")
	}
	if !slices.Equal(ss.InboundQoS2, []uint16{9}) || !slices.Equal(ss.InboundAccepted, []uint16{9}) {
		t.Fatalf("the taken state holds %v open and %v accepted, want 9 in both", ss.InboundQoS2, ss.InboundAccepted)
	}
}

// TestARefusalReleasesOnlyItsOwnAttempt: a refusal whose attempt no
// longer holds the identifier leaves the attempt that does alone.
func TestARefusalReleasesOnlyItsOwnAttempt(t *testing.T) {
	m, _ := newManager(t, nil)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	if code, _ := publishQoS2(m, pub, 9, false, "m"); code.IsError() {
		t.Fatalf("precondition: the publish answered %v", code)
	}
	m.releaseRefused(m.lookup(pub), pub, 9, &inboundAttempt{}, refused)
	if !inboundOpen(m, pub, 9) {
		t.Fatal("a refusal of an earlier attempt released the identifier a later attempt holds")
	}
}

// TestARefusedAttemptIsReleasedAfterItsConnectionIsReplaced: the client
// reconnects to this node while its attempt's barrier runs and has not yet
// retransmitted; the refused attempt still releases its identifier.
func TestARefusedAttemptIsReleasedAfterItsConnectionIsReplaced(t *testing.T) {
	rep := &switchReplication{}
	m, _ := newManager(t, nil)
	m.SetReplication(rep)
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, false, 600, nil)
	again := newFake("pub", packet.V5, 100)
	rep.during = func() {
		rep.during = nil
		if !connectV5(m, again, false, 600, nil) {
			t.Error("precondition: the session was not present on reconnect")
		}
		rep.down.Store(true)
	}
	if code, _ := publishQoS2(m, pub, 9, false, "m"); code != refused {
		t.Fatalf("precondition: the attempt answered %v, want %v", code, refused)
	}
	if inboundOpen(m, again, 9) {
		t.Fatal("9 is still open after the attempt holding it was refused")
	}
}

// TestNoAcceptanceForAnIdentifierReleasedDuringItsBarrier: a PUBREL
// that releases the identifier while its attempt's barrier runs leaves no
// acceptance to follow the release on the replica, where it would open the
// identifier again.
func TestNoAcceptanceForAnIdentifierReleasedDuringItsBarrier(t *testing.T) {
	e := newReplicaEnv(t)
	pub := newFake("pub", packet.V5, 100)
	connectV5(e.m, pub, false, 600, nil)
	again := newFake("pub", packet.V5, 100)
	e.rep.during = func() {
		e.rep.during = nil
		connectV5(e.m, again, false, 600, nil)
		if code, _ := e.m.Pubrel(again, 9); code != packet.Success {
			t.Errorf("precondition: the PUBREL answered %v", code)
		}
	}
	if code, _ := publishQoS2(e.m, pub, 9, false, "m"); code.IsError() {
		t.Fatalf("precondition: the attempt answered %v", code)
	}
	if e.rep.during != nil {
		t.Fatal("precondition: the attempt did not wait on the barrier")
	}
	if ss := e.replica.Find("default", "pub"); ss == nil || slices.Contains(ss.InboundQoS2, 9) {
		t.Fatalf("the replica holds 9 open after its release (%+v)", ss)
	}
}
