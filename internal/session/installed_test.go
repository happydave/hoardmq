package session

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// TestHeldAtInstall: a routed delivery is skipped only for the exact message
// the session was installed holding; anything else, or a message with no
// identity, is delivered.
func TestHeldAtInstall(t *testing.T) {
	at := time.Unix(1_700_000_000, 123)
	held := &message.Message{Publisher: message.Publisher{Node: "n2"}, Seq: 41, Arrived: at}
	id, _ := held.Identity()
	installed := map[message.ID]struct{}{id: {}}
	for _, tc := range []struct {
		name      string
		installed map[message.ID]struct{}
		msg       *message.Message
		want      bool
	}{
		{"the message the session was installed holding", installed, &message.Message{Publisher: message.Publisher{Node: "n2"}, Seq: 41, Arrived: at}, true},
		{"the same publish decoded from the wire", installed, decoded(t, held), true},
		{"another sequence from the same node", installed, &message.Message{Publisher: message.Publisher{Node: "n2"}, Seq: 42, Arrived: at}, false},
		{"the same sequence from another node", installed, &message.Message{Publisher: message.Publisher{Node: "n3"}, Seq: 41, Arrived: at}, false},
		{"the same sequence from another incarnation of the node", installed, &message.Message{Publisher: message.Publisher{Node: "n2"}, Seq: 41, Arrived: at.Add(time.Nanosecond)}, false},
		{"a message never sequenced", installed, &message.Message{Publisher: message.Publisher{Node: "n2"}, Arrived: at}, false},
		{"a message with no arrival time", installed, &message.Message{Publisher: message.Publisher{Node: "n2"}, Seq: 41}, false},
		{"a session installed holding nothing", nil, held, false},
	} {
		if got := heldAtInstall(tc.installed, tc.msg); got != tc.want {
			t.Errorf("%s: heldAtInstall = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// decoded is msg after a round trip through the encoding a forward and a
// session state use.
func decoded(t *testing.T, msg *message.Message) *message.Message {
	t.Helper()
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var out message.Message
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

// installedState is one offline session subscribed to t/x holding one queued
// message published on n2.
func installedState(msg *message.Message) *store.SessionState {
	return &store.SessionState{
		SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: "x", ExpiryInterval: 600, Version: byte(packet.V5), Epoch: 7},
		Subscriptions: []store.SubscriptionRecord{{Filter: "t/x", QoS: 1}},
		Entries:       []store.Entry{{Seq: 1, QoS: 1, State: store.StateQueued, PacketID: 6, Message: msg}},
	}
}

// TestInstalledSessionIsNotGivenAMessageItHeldAgain: the previous owner
// routed a publish into a session and forwarded the same publish here for
// this node's own subscribers; the session was handed over holding it, and
// the forward arrives after the install. The session keeps one copy, and the
// forward still counts as having reached it (WI 1521).
func TestInstalledSessionIsNotGivenAMessageItHeldAgain(t *testing.T) {
	m, _ := newManager(t, nil)
	msg := &message.Message{Topic: "t/x", QoS: 1, Payload: []byte("m41"), Publisher: message.Publisher{ClientID: "pub", Node: "n2"}, Seq: 41, Arrived: time.Unix(1_700_000_000, 5)}
	m.Import(installedState(decoded(t, msg)))

	late := decoded(t, msg)
	n, into, _ := m.InjectForwarded(late)
	if n != 1 || len(into) != 1 || into[0].ClientID != "x" {
		t.Fatalf("the late forward reached %d sessions, naming %v; want it counted as reaching x", n, into)
	}
	if rec, _ := m.Record("x"); rec.Queued != 1 {
		t.Fatalf("x holds %d queued entries after the late forward; want the one it was installed with", rec.Queued)
	}
	if got := m.DuplicatesSkipped(); got != 1 {
		t.Fatalf("duplicates skipped %d, want 1", got)
	}

	next := decoded(t, msg)
	next.Seq, next.Payload = 42, []byte("m42")
	_, _, _ = m.InjectForwarded(next)
	if rec, _ := m.Record("x"); rec.Queued != 2 {
		t.Fatalf("x holds %d queued entries after a new message; want 2", rec.Queued)
	}
	if got := m.DuplicatesSkipped(); got != 1 {
		t.Fatalf("duplicates skipped %d after a new message, want still 1", got)
	}
}

// TestRecoveredSessionRemembersNothing: a session loaded from this node's own
// log at start was not handed over, so a routed message is delivered to it
// whatever its log held.
func TestRecoveredSessionRemembersNothing(t *testing.T) {
	m, _ := newManager(t, nil)
	msg := &message.Message{Topic: "t/x", QoS: 1, Payload: []byte("m41"), Publisher: message.Publisher{ClientID: "pub", Node: "n2"}, Seq: 41, Arrived: time.Unix(1_700_000_000, 5)}
	m.Load(&store.State{Sessions: []*store.SessionState{installedState(decoded(t, msg))}})
	_, _, _ = m.InjectForwarded(decoded(t, msg))
	if rec, _ := m.Record("x"); rec.Queued != 2 {
		t.Fatalf("x holds %d queued entries; want 2: recovery records nothing to skip", rec.Queued)
	}
	if got := m.DuplicatesSkipped(); got != 0 {
		t.Fatalf("duplicates skipped %d, want 0", got)
	}
}

// TestRetainedResentOnSubscribeIsNotSkipped: a retained message the session
// was installed holding is sent again when the client subscribes, as a
// subscribe asks; only routed deliveries are checked.
func TestRetainedResentOnSubscribeIsNotSkipped(t *testing.T) {
	m, _ := newManager(t, nil)
	msg := &message.Message{Topic: "t/x", QoS: 1, Retain: true, Payload: []byte("r"), Publisher: message.Publisher{ClientID: "pub", Node: "n2"}, Seq: 41, Arrived: time.Unix(1_700_000_000, 5)}
	m.Import(installedState(decoded(t, msg)))
	m.InjectRetained(decoded(t, msg))
	c := newFake("x", packet.V5, 100)
	if present := connectV5(m, c, false, 600, nil); !present {
		t.Fatal("precondition not reached: the installed session was not resumed")
	}
	subscribe(m, c, 1, "t/x")
	m.Subscribed(c)
	got := 0
	c.mu.Lock()
	for _, p := range c.sent {
		if pub, ok := p.(*packet.Publish); ok && string(pub.Payload) == "r" {
			got++
		}
	}
	c.mu.Unlock()
	if got != 2 {
		t.Fatalf("the retained message reached the client %d times; want the queued copy and the one the subscribe asked for", got)
	}
}
