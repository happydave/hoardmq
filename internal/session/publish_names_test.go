package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// scopeJournal records the key of every write, for comparing what a call
// wrote with what its durability wait named.
type scopeJournal struct {
	mu      sync.Mutex
	written map[store.Scope]bool
}

func (j *scopeJournal) note(s store.Scope) {
	j.mu.Lock()
	if j.written == nil {
		j.written = map[store.Scope]bool{}
	}
	j.written[s.Bare()] = true
	j.mu.Unlock()
}

func (j *scopeJournal) take() map[store.Scope]bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	w := j.written
	j.written = nil
	return w
}

func (j *scopeJournal) key(k store.Key) error { j.note(k.Scope()); return nil }

func (j *scopeJournal) SessionUpsert(m store.SessionMeta) error {
	j.note(store.Scope{Namespace: m.Namespace, ClientID: m.ClientID})
	return nil
}
func (j *scopeJournal) SessionDelete(k store.Key) error                         { return j.key(k) }
func (j *scopeJournal) Subscribe(k store.Key, _ store.SubscriptionRecord) error { return j.key(k) }
func (j *scopeJournal) Unsubscribe(k store.Key, _ string) error                 { return j.key(k) }
func (j *scopeJournal) InboundQoS2(k store.Key, _ uint16, _ bool) error         { return j.key(k) }
func (j *scopeJournal) InboundAccepted(k store.Key, _ uint16) error             { return j.key(k) }
func (j *scopeJournal) Enqueue(k store.Key, _ store.Entry) error                { return j.key(k) }
func (j *scopeJournal) Sent(k store.Key, _ uint64, _ uint16) error              { return j.key(k) }
func (j *scopeJournal) InflightState(k store.Key, _ uint16, _ byte) error       { return j.key(k) }
func (j *scopeJournal) Complete(k store.Key, _ uint16) error                    { return j.key(k) }
func (j *scopeJournal) Dropped(k store.Key, _ uint64) error                     { return j.key(k) }
func (j *scopeJournal) Retain(m *message.Message) error {
	j.note(store.Scope{Namespace: m.Namespace, Topic: m.Topic})
	return nil
}
func (j *scopeJournal) RetainDelete(ns, topic string) error {
	j.note(store.Scope{Namespace: ns, Topic: topic})
	return nil
}
func (j *scopeJournal) Sync() error { return nil }

// namingReplication checks, at each durability wait, that every record the
// journal took since the last check is named by the wait.
type namingReplication struct {
	t       *testing.T
	j       *scopeJournal
	waits   int
	written int // records the checked waits covered, for the non-vacuity check
}

func (r *namingReplication) check(what string, keys []store.Scope) {
	r.t.Helper()
	r.waits++
	named := map[store.Scope]bool{}
	for _, k := range keys {
		named[k.Bare()] = true
	}
	w := r.j.take()
	r.written += len(w)
	for s := range w {
		if !named[s] {
			r.t.Errorf("%s wrote a record for %+v and did not name it; named %v", what, s, keys)
		}
	}
}

func (r *namingReplication) Barrier(_ context.Context, keys []store.Scope, _ []message.ForwardRef) error {
	r.check("a publish's barrier", keys)
	return nil
}
func (r *namingReplication) Durable(_ context.Context, keys []store.Scope) error {
	r.check("a durability wait", keys)
	return nil
}
func (r *namingReplication) StoreRetained(*message.Message) bool { return true }
func (r *namingReplication) QueryRetained(string, string, time.Time) []*message.Message {
	return nil
}

// TestEveryWriteIsNamedByItsWait: a barrier waits only on the targets of the
// keys it names (WI 1631), so a record a call writes and does not name is
// acknowledged unexamined. For each path that writes before a durability wait,
// every key written is among those named. Unlike the enumerated checks above,
// this compares the journal's own writes with the names. A shared-group
// delivery is not here: this package's router has no groups, and the one that
// does imports this package.
func TestEveryWriteIsNamedByItsWait(t *testing.T) {
	setup := func(t *testing.T) (*Manager, *scopeJournal, *namingReplication, *fakeConn) {
		t.Helper()
		j := &scopeJournal{}
		m, _ := newManager(t, func(o *Options) { o.Journal = j })
		rep := &namingReplication{t: t, j: j}
		m.SetReplication(rep)
		pub := newFake("pub", packet.V5, 100)
		connectV5(m, pub, false, 3600, nil)
		for _, id := range []string{"sub-a", "sub-b"} {
			c := newFake(id, packet.V5, 100)
			connectV5(m, c, false, 3600, nil)
			subscribe(m, c, 1, "t/x")
		}
		offline := newFake("sub-offline", packet.V5, 100)
		connectV5(m, offline, false, 3600, nil)
		subscribe(m, offline, 2, "t/x")
		m.Disconnect(offline, true, packet.NormalDisconnection, false)
		j.take()
		rep.waits, rep.written = 0, 0
		return m, j, rep, pub
	}
	covered := func(t *testing.T, rep *namingReplication) {
		t.Helper()
		if rep.waits != 1 || rep.written == 0 {
			t.Fatalf("precondition: %d waits covering %d records; the check needs one wait over something written", rep.waits, rep.written)
		}
	}

	t.Run("QoS 1 publish", func(t *testing.T) {
		m, _, rep, pub := setup(t)
		code, _ := m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 1, Payload: []byte("x")})
		if code != packet.Success {
			t.Fatalf("publish: %v", code)
		}
		covered(t, rep)
	})
	t.Run("QoS 2 publish", func(t *testing.T) {
		m, _, rep, pub := setup(t)
		if code, _ := m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 2, PacketID: 2, Payload: []byte("x")}); code != packet.Success {
			t.Fatalf("publish: %v", code)
		}
		covered(t, rep)
	})
	t.Run("retained QoS 1 publish", func(t *testing.T) {
		m, _, rep, pub := setup(t)
		if code, _ := m.Publish(pub, &packet.Publish{Topic: "t/x", QoS: 1, PacketID: 3, Retain: true, Payload: []byte("x")}); code != packet.Success {
			t.Fatalf("publish: %v", code)
		}
		covered(t, rep)
	})
	t.Run("forwarded delivery", func(t *testing.T) {
		m, _, rep, _ := setup(t)
		_, into, _ := m.InjectForwarded(&message.Message{Namespace: "default", Topic: "t/x", QoS: 1, Payload: []byte("x")})
		_ = rep.Durable(context.Background(), into)
		covered(t, rep)
	})
}
