package session

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// queueEnv is a session manager over a real store whose tap applies every
// record to a second state, as a follower applies the records it is sent: a
// persistent session "sub" subscribed to "t", offline, and a publisher.
type queueEnv struct {
	dir       string
	st        *store.Store
	m         *Manager
	clock     *fakeClock
	pub       *fakeConn
	replica   *store.State
	unapplied []string
}

func newQueueEnv(t *testing.T, subQoS byte) *queueEnv {
	t.Helper()
	e := &queueEnv{dir: t.TempDir(), replica: store.NewState()}
	tap := func(r store.Record) func() {
		un, err := e.replica.ApplyReport(r.Type, r.Payload)
		if err != nil {
			t.Errorf("replica apply %s: %v", store.RecordName(r.Type), err)
		}
		if un != "" {
			e.unapplied = append(e.unapplied, store.RecordName(r.Type)+": "+string(un))
		}
		return nil
	}
	st, err := store.Open(e.dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Tap: tap})
	if err != nil {
		t.Fatal(err)
	}
	e.st = st
	t.Cleanup(func() { _ = st.Close() })
	e.m, e.clock = newManager(t, func(o *Options) { o.Journal = st })
	e.pub = newFake("pub", packet.V5, 100)
	connectV5(e.m, e.pub, true, 0, nil)
	sub := newFake("sub", packet.V5, 100)
	connectV5(e.m, sub, true, 3600, nil)
	subscribe(e.m, sub, subQoS, "t")
	e.m.Disconnect(sub, false, packet.NormalDisconnection, false)
	return e
}

// restart closes the store without a checkpoint, as a crash leaves it,
// reopens it, and loads what it recovers into a new manager.
func (e *queueEnv) restart(t *testing.T) (*Manager, *store.State) {
	t.Helper()
	if err := e.st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(e.dir, store.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rec, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if ss := rec.Find("default", "sub"); ss == nil || len(ss.Subscriptions) != 1 {
		t.Fatalf("precondition: the session and its subscription are not recovered: %+v", ss)
	}
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	m.Load(rec)
	return m, rec
}

// qos0Queued is a queue env at QoS 0 with one QoS 0 message ("q0") queued for
// the offline session and then drained to it on a reconnect.
func qos0Queued(t *testing.T) (*queueEnv, *fakeConn) {
	t.Helper()
	e := newQueueEnv(t, 0)
	publish(e.m, e.pub, "t", 0, 0, "q0")
	if rec, ok := e.m.Record("sub"); !ok || rec.Queued != 1 {
		t.Fatalf("precondition: the QoS 0 message is not queued for the offline session: %+v", rec)
	}
	if got := entries(e.replica.Find("default", "sub")); len(got) != 1 {
		t.Fatalf("precondition: the replica holds %q after the enqueue, want [q0]", got)
	}
	sub := newFake("sub", packet.V5, 100)
	connectV5(e.m, sub, false, 3600, nil)
	if n := countPayload(sub.publishes(), "q0"); n != 1 {
		t.Fatalf("precondition: the reconnected session was sent the queued message %d times, want 1", n)
	}
	if rec, _ := e.m.Record("sub"); rec.Queued != 0 {
		t.Fatalf("precondition: the queue still holds %d after the drain", rec.Queued)
	}
	return e, sub
}

func entries(ss *store.SessionState) []string {
	if ss == nil {
		return nil
	}
	var out []string
	for _, e := range ss.Entries {
		out = append(out, string(e.Message.Payload))
	}
	return out
}

func countPayload(pubs []*packet.Publish, payload string) int {
	n := 0
	for _, p := range pubs {
		if string(p.Payload) == payload {
			n++
		}
	}
	return n
}

// TestQoS0SentFromTheQueueIsNotRecovered: a QoS 0 message queued for an
// offline session and sent from the queue when it reconnects is gone from
// the log's state and from a replica's, so a restart that recovers the log,
// or a takeover that installs the replica's copy, does not send it again.
// QoS 0 is at most once (WI 1831).
func TestQoS0SentFromTheQueueIsNotRecovered(t *testing.T) {
	t.Run("the replica's state", func(t *testing.T) {
		e, _ := qos0Queued(t)
		if got := entries(e.replica.Find("default", "sub")); len(got) != 0 {
			t.Fatalf("the replica still holds %q after the message left the queue", got)
		}
		if len(e.unapplied) != 0 {
			t.Fatalf("the replica could not apply %q", e.unapplied)
		}
	})
	t.Run("a restart from the log", func(t *testing.T) {
		e, sub := qos0Queued(t)
		e.m.Disconnect(sub, false, packet.NormalDisconnection, false)
		m2, rec := e.restart(t)
		again := newFake("sub", packet.V5, 100)
		if present := connectV5(m2, again, false, 3600, nil); !present {
			t.Fatal("precondition: the recovered session is not present")
		}
		if n := countPayload(again.publishes(), "q0"); n != 0 {
			t.Fatalf("the QoS 0 message was sent again after the restart (%d times); recovered entries %q", n, entries(rec.Find("default", "sub")))
		}
	})
	t.Run("a takeover of the replica's copy", func(t *testing.T) {
		e, _ := qos0Queued(t)
		ss := e.replica.Find("default", "sub")
		if ss == nil || len(ss.Subscriptions) != 1 {
			t.Fatalf("precondition: the replica does not hold the session: %+v", ss)
		}
		m2, _ := newManager(t, nil)
		m2.Import(ss)
		again := newFake("sub", packet.V5, 100)
		if present := connectV5(m2, again, false, 3600, nil); !present {
			t.Fatal("precondition: the imported session is not present")
		}
		if n := countPayload(again.publishes(), "q0"); n != 0 {
			t.Fatalf("the QoS 0 message was sent again after the takeover (%d times); replica entries %q", n, entries(ss))
		}
	})
}

// TestExpiredQoS0LeavesTheLogState: a queued QoS 0 message whose expiry
// passed while its session was offline is dropped, not sent, when the session
// reconnects, and its entry leaves the log's state and the replica's.
func TestExpiredQoS0LeavesTheLogState(t *testing.T) {
	e := newQueueEnv(t, 0)
	expiry := uint32(10)
	e.m.Publish(e.pub, &packet.Publish{Topic: "t", Payload: []byte("stale"), Properties: packet.Properties{MessageExpiryInterval: &expiry}})
	if got := entries(e.replica.Find("default", "sub")); len(got) != 1 {
		t.Fatalf("precondition: the replica holds %q after the enqueue, want [stale]", got)
	}
	e.clock.advance(time.Minute)
	sub := newFake("sub", packet.V5, 100)
	connectV5(e.m, sub, false, 3600, nil)
	if n := countPayload(sub.publishes(), "stale"); n != 0 {
		t.Fatalf("precondition: the expired message was sent %d times", n)
	}
	if rec, _ := e.m.Record("sub"); rec.Queued != 0 || rec.Expired != 1 {
		t.Fatalf("precondition: the expired message was not dropped from the queue: %+v", rec)
	}
	if got := entries(e.replica.Find("default", "sub")); len(got) != 0 {
		t.Fatalf("the replica still holds %q after the expired message left the queue", got)
	}
	e.m.Disconnect(sub, false, packet.NormalDisconnection, false)
	if _, rec := e.restart(t); len(entries(rec.Find("default", "sub"))) != 0 {
		t.Fatalf("the log still holds %q after the expired message left the queue", entries(rec.Find("default", "sub")))
	}
}

// TestQoS0StillQueuedIsKept: a QoS 0 message queued behind a QoS 1 message
// the reconnected session has no send quota for has not left the queue, so
// the log and the replica keep both, and after a restart each is sent once.
func TestQoS0StillQueuedIsKept(t *testing.T) {
	e := newQueueEnv(t, 1)
	publish(e.m, e.pub, "t", 1, 1, "m1")
	publish(e.m, e.pub, "t", 0, 0, "q0")
	stalled := newFake("sub", packet.V5, 0)
	connectV5(e.m, stalled, false, 3600, nil)
	if len(stalled.publishes()) != 0 {
		t.Fatalf("precondition: the session with no send quota was sent %d messages", len(stalled.publishes()))
	}
	if rec, _ := e.m.Record("sub"); rec.Queued != 2 {
		t.Fatalf("precondition: the queue holds %d, want both messages", rec.Queued)
	}
	if got := entries(e.replica.Find("default", "sub")); len(got) != 2 || got[0] != "m1" || got[1] != "q0" {
		t.Fatalf("the replica holds %q; want [m1 q0], neither sent yet", got)
	}
	e.m.Disconnect(stalled, false, packet.NormalDisconnection, false)
	m2, rec := e.restart(t)
	if got := entries(rec.Find("default", "sub")); len(got) != 2 || got[0] != "m1" || got[1] != "q0" {
		t.Fatalf("the log holds %q; want [m1 q0], neither sent yet", got)
	}
	again := newFake("sub", packet.V5, 100)
	connectV5(m2, again, false, 3600, nil)
	if a, b := countPayload(again.publishes(), "m1"), countPayload(again.publishes(), "q0"); a != 1 || b != 1 {
		t.Fatalf("after the restart m1 was sent %d times and q0 %d; want each once", a, b)
	}
}
