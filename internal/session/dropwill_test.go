package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// willClient connects id with a Will on will/<id> and a session expiry, and
// resumes it.
func willClient(m *Manager, id string, delay uint32) *fakeConn {
	c := newFake(id, packet.V5, 10)
	pkt := &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true, Will: &packet.Will{Topic: "will/" + id, Payload: []byte(id)}}
	if delay > 0 {
		pkt.Will.Properties.WillDelayInterval = &delay
	}
	exp := uint32(600)
	pkt.Properties.SessionExpiryInterval = &exp
	m.Connect(c, pkt, 3)
	m.Resume(c)
	return c
}

func willTopics(w *fakeConn) []string {
	var out []string
	for _, p := range w.publishes() {
		out = append(out, p.Topic)
	}
	return out
}

// TestInvalidateWithoutTheWill: will false ends a connected session and an
// offline one with an armed Will and publishes neither Will; the same calls
// with will true, beside them, publish theirs.
func TestInvalidateWithoutTheWill(t *testing.T) {
	m, _ := newManager(t, nil)
	watcher := newFake("w", packet.V5, 10)
	connectV5(m, watcher, true, 0, nil)
	subscribe(m, watcher, 0, "will/online-kept", "will/online-dropped", "will/armed-kept", "will/armed-dropped")
	willClient(m, "online-kept", 0)
	willClient(m, "online-dropped", 0)
	for _, id := range []string{"armed-kept", "armed-dropped"} {
		c := willClient(m, id, 300)
		m.Disconnect(c, false, packet.UnspecifiedError, true)
	}
	if got := willTopics(watcher); len(got) != 0 {
		t.Fatalf("precondition: a Will went out before any invalidation: %v", got)
	}
	for _, c := range []struct {
		id   string
		will bool
	}{{"online-dropped", false}, {"armed-dropped", false}, {"online-kept", true}, {"armed-kept", true}} {
		res := m.Invalidate("default", c.id, packet.AdministrativeAction, nil, c.will)
		if !res.Found || res.WillPublished != c.will {
			t.Fatalf("%s: %+v, want the session found and WillPublished %v", c.id, res, c.will)
		}
		if _, held := m.Record(c.id); held {
			t.Fatalf("%s: the session is still held", c.id)
		}
	}
	got := willTopics(watcher)
	if len(got) != 2 || got[0] != "will/online-kept" || got[1] != "will/armed-kept" {
		t.Fatalf("Wills published %v, want will/online-kept and will/armed-kept only", got)
	}
}

// TestDropWill: the Will goes only while the given connection is the
// session's; closing that connection then publishes nothing, and a stale
// connection's call leaves the Will to be published.
func TestDropWill(t *testing.T) {
	m, _ := newManager(t, nil)
	watcher := newFake("w", packet.V5, 10)
	connectV5(m, watcher, true, 0, nil)
	subscribe(m, watcher, 0, "will/kept", "will/dropped")
	kept := willClient(m, "kept", 0)
	dropped := willClient(m, "dropped", 0)
	if m.DropWill("default", "kept", newFake("kept", packet.V5, 10)) {
		t.Fatal("a connection that is not the session's dropped its Will")
	}
	if !m.DropWill("default", "dropped", dropped) {
		t.Fatal("the session's own connection did not drop its Will")
	}
	m.Disconnect(kept, false, packet.AdministrativeAction, true)
	m.Disconnect(dropped, false, packet.AdministrativeAction, true)
	if got := willTopics(watcher); len(got) != 1 || got[0] != "will/kept" {
		t.Fatalf("Wills published %v, want will/kept only", got)
	}
}
