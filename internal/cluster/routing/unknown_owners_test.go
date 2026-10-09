package routing

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/message"
)

// stubTransport drives the plane's peer and route-sync events without a
// network, so the unknown-owner window can be entered and left exactly.
type stubTransport struct {
	mu       sync.Mutex
	peers    []transport.PeerInfo
	handlers map[transport.Class]transport.Handler
	obs      []transport.Observer
	dataObs  []transport.Observer
	frames   []sentFrame
}

func newStubTransport(peers ...string) *stubTransport {
	s := &stubTransport{handlers: map[transport.Class]transport.Handler{}}
	for _, name := range peers {
		s.peers = append(s.peers, transport.PeerInfo{Name: name, State: transport.Down})
	}
	return s
}

func (s *stubTransport) Send(_ context.Context, peer string, c transport.Class, payload []byte) error {
	s.mu.Lock()
	s.frames = append(s.frames, sentFrame{peer: peer, class: c, payload: payload})
	s.mu.Unlock()
	return nil
}

// sentFrame is one frame the plane handed the stub.
type sentFrame struct {
	peer    string
	class   transport.Class
	payload []byte
}

// sentFrames is every frame sent so far.
func (s *stubTransport) sentFrames() []sentFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sentFrame(nil), s.frames...)
}

func (s *stubTransport) Handle(c transport.Class, h transport.Handler) {
	s.mu.Lock()
	s.handlers[c] = h
	s.mu.Unlock()
}

func (s *stubTransport) Observe(o transport.Observer) {
	s.mu.Lock()
	s.obs = append(s.obs, o)
	s.mu.Unlock()
}

func (s *stubTransport) ObserveData(o transport.Observer) {
	s.mu.Lock()
	s.dataObs = append(s.dataObs, o)
	s.mu.Unlock()
}

func (s *stubTransport) Peers() []transport.PeerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]transport.PeerInfo(nil), s.peers...)
}

// setState moves a peer and notifies the control observers, as the real
// transport does on a change.
func (s *stubTransport) setState(name string, st transport.State) {
	s.mu.Lock()
	for i := range s.peers {
		if s.peers[i].Name == name {
			s.peers[i].State = st
		}
	}
	obs := append([]transport.Observer(nil), s.obs...)
	s.mu.Unlock()
	for _, o := range obs {
		o(name, st)
	}
}

// deliverFullSync hands the plane a peer's complete own-entry set, the
// message that makes that peer's routes known.
func (s *stubTransport) deliverFullSync(t *testing.T, peer string, entries []Entry) {
	t.Helper()
	s.mu.Lock()
	h := s.handlers[transport.RouteUpdate]
	s.mu.Unlock()
	if h == nil {
		t.Fatal("no route-update handler registered")
	}
	body, err := json.Marshal(fullSync{Snapshot: 1, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	h(peer, append([]byte{routeFull}, body...))
}

// stubPlane builds a plane on a stub transport. owned is the replicated
// lease view; nil means none is wired, as in a plane built without one.
func stubPlane(t *testing.T, node string, peers []string, members []string, owned func(string) (int, bool)) (*Plane, *stubTransport) {
	t.Helper()
	tr := newStubTransport(peers...)
	o := Options{Node: node, Transport: tr, SyncInterval: time.Hour, Logger: quiet, OwnedSessions: owned}
	if members != nil {
		o.Members = func() []string { return members }
	}
	p := New(o)
	p.SetSink(&fakeSink{})
	p.Start()
	t.Cleanup(p.Stop)
	return p, tr
}

func qos1(topic string) *message.Message {
	return &message.Message{Namespace: "default", Topic: topic, QoS: 1, Payload: []byte("x"), Seq: 1, Arrived: time.Now()}
}

// TestPublishRefusedWhileAnOwnersRoutesAreUnknown is the defect of WI 1351:
// a node that restarted beside a peer it cannot reach holds nothing for that
// peer, so HiddenMatch has nothing to match and the publish was acknowledged.
// It is the negative control for the rule — on the tree before the change it
// fails, because Forward returns nil.
func TestPublishRefusedWhileAnOwnersRoutesAreUnknown(t *testing.T) {
	owned := func(node string) (int, bool) {
		if node == "n1" {
			return 1, true // n1 still owns a session somewhere
		}
		return 0, true
	}
	p, tr := stubPlane(t, "n2", []string{"n1", "n3"}, []string{"n1", "n2", "n3"}, owned)

	// n3 comes up and syncs; n1 is unreachable and never does.
	tr.setState("n3", transport.Up)
	tr.deliverFullSync(t, "n3", []Entry{{Namespace: "default", Filter: "n3/only", Owner: "n3", Version: 1, Count: 1}})

	if _, err := p.Forward(qos1("anything/at/all")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a QoS 1 publish must not be acknowledged while n1's routes are unknown; got %v", err)
	}
	// The table genuinely holds nothing for n1, which is why HiddenMatch
	// cannot be the mechanism: this is the state the rule exists for.
	if p.table.HiddenMatch("default", "anything/at/all") {
		t.Fatal("the table holds entries for n1; this test no longer exercises the unknown-owner case")
	}
}

// TestUnknownOwnerWindowClosesOnItsSync: the refusal ends when the owner's
// routes arrive, and the hidden-owner refusal that already worked is
// untouched by the change.
func TestUnknownOwnerWindowClosesOnItsSync(t *testing.T) {
	p, tr := stubPlane(t, "n2", []string{"n1", "n3"}, []string{"n1", "n2", "n3"},
		func(string) (int, bool) { return 1, true })
	tr.setState("n3", transport.Up)
	tr.deliverFullSync(t, "n3", nil)
	if _, err := p.Forward(qos1("t/1")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("expected a refusal while n1 is unknown, got %v", err)
	}

	tr.setState("n1", transport.Up)
	tr.deliverFullSync(t, "n1", []Entry{{Namespace: "default", Filter: "t/1", Owner: "n1", Version: 1, Count: 1}})
	if _, err := p.Forward(qos1("t/other")); err != nil {
		t.Fatalf("every owner has synced; a publish nobody matches must be accepted: %v", err)
	}

	// n1's entries are now known, so the pre-existing rule takes over: with
	// n1 down its matching route is hidden and the publish is still refused.
	tr.setState("n1", transport.Down)
	if _, err := p.Forward(qos1("t/1")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("a hidden owner's matching route must still refuse: %v", err)
	}
	if _, err := p.Forward(qos1("t/unmatched")); err != nil {
		t.Fatalf("a topic no owner matches must be accepted once every owner is known: %v", err)
	}
}

// TestUnknownOwnerWindowClosesWhenItHoldsNothing: the second exit, for an
// owner nothing was taken from (no EmptiedAt wired: emptied index zero). Its
// routes cannot matter, so the refusal ends without it ever being reachable.
// An owner a reassignment emptied is unserved_test.go's.
func TestUnknownOwnerWindowClosesWhenItHoldsNothing(t *testing.T) {
	var sessions int64 = 1
	var mu sync.Mutex
	owned := func(string) (int, bool) {
		mu.Lock()
		defer mu.Unlock()
		return int(sessions), true
	}
	p, tr := stubPlane(t, "n2", []string{"n1"}, []string{"n1", "n2"}, owned)
	tr.setState("n1", transport.Connecting) // a dial that never completes
	if _, err := p.Forward(qos1("t/1")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("expected a refusal while n1 owns a session and has not synced, got %v", err)
	}
	mu.Lock()
	sessions = 0
	mu.Unlock()
	p.refreshOwnerKnowledge()
	if _, err := p.Forward(qos1("t/1")); err != nil {
		t.Fatalf("n1 owns nothing, so its routes cannot matter: %v", err)
	}
}

// TestUnknownOwnerDoesNotAffectQoS0: QoS 0 carries no acknowledgement, so
// there is nothing to be wrong about.
func TestUnknownOwnerDoesNotAffectQoS0(t *testing.T) {
	p, _ := stubPlane(t, "n2", []string{"n1"}, []string{"n1", "n2"},
		func(string) (int, bool) { return 1, true })
	m := qos1("t/1")
	m.QoS = 0
	if _, err := p.Forward(m); err != nil {
		t.Fatalf("QoS 0 must be unaffected: %v", err)
	}
	if _, err := p.Forward(qos1("t/1")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatal("QoS 1 must still be refused in the same state")
	}
}

// TestNoPeersNeverRefuses: a node with nobody to be ignorant about must not
// refuse, and must not still be sitting on the safe default New installs.
func TestNoPeersNeverRefuses(t *testing.T) {
	p, _ := stubPlane(t, "solo", nil, []string{"solo"}, func(string) (int, bool) { return 0, true })
	if _, err := p.Forward(qos1("t/1")); err != nil {
		t.Fatalf("a node with no peers has nothing unknown: %v", err)
	}
}

// TestLeaseViewUnavailableCountsAsUnknown: a node whose replicated lease
// state has not been replayed cannot prove a peer holds nothing.
func TestLeaseViewUnavailableCountsAsUnknown(t *testing.T) {
	p, _ := stubPlane(t, "n2", []string{"n1"}, []string{"n1", "n2"},
		func(string) (int, bool) { return 0, false })
	if _, err := p.Forward(qos1("t/1")); !errors.Is(err, ErrRoutesUnavailable) {
		t.Fatalf("an unreadable lease view proves nothing; expected a refusal, got %v", err)
	}
}

// TestForwardReportsItsForwards: Forward returns one per remote node it sent
// the message to, and zero for a topic no remote node matches (WI 1487).
func TestForwardReportsItsForwards(t *testing.T) {
	p, tr := stubPlane(t, "n2", []string{"n1", "n3"}, []string{"n1", "n2", "n3"}, func(string) (int, bool) { return 1, true })
	for _, peer := range []string{"n1", "n3"} {
		tr.setState(peer, transport.Up)
	}
	tr.deliverFullSync(t, "n1", []Entry{{Namespace: "default", Filter: "t/1", Owner: "n1", Version: 1, Count: 1}})
	tr.deliverFullSync(t, "n3", []Entry{{Namespace: "default", Filter: "t/+", Owner: "n3", Version: 1, Count: 1}})
	if sent, err := p.Forward(qos1("t/1")); err != nil || sent != 2 {
		t.Fatalf("t/1 is matched on n1 and n3: sent %d, err %v", sent, err)
	}
	if sent, err := p.Forward(qos1("t/2")); err != nil || sent != 1 {
		t.Fatalf("t/2 is matched on n3 only: sent %d, err %v", sent, err)
	}
	if sent, err := p.Forward(qos1("other")); err != nil || sent != 0 {
		t.Fatalf("nothing matches other: sent %d, err %v", sent, err)
	}
}

// TestOwnerKnowledgeEpisodes (WI 1571): the span of unknown owners is
// recorded from the plane's start, ends when the last owner is known, and a
// new one opens when an owner becomes unknown again.
func TestOwnerKnowledgeEpisodes(t *testing.T) {
	var sessions int64 = 1
	var mu sync.Mutex
	owned := func(string) (int, bool) {
		mu.Lock()
		defer mu.Unlock()
		return int(sessions), true
	}
	setOwned := func(n int64) {
		mu.Lock()
		sessions = n
		mu.Unlock()
	}
	p, tr := stubPlane(t, "n2", []string{"n1"}, []string{"n1", "n2"}, owned)
	tr.setState("n1", transport.Connecting)
	if eps, open := p.OwnerKnowledge(); !open || len(eps) != 1 || eps[0].Start.IsZero() {
		t.Fatalf("a plane starting with n1 unknown: %+v open=%v", eps, open)
	}
	setOwned(0)
	p.refreshOwnerKnowledge()
	eps, open := p.OwnerKnowledge()
	if open || len(eps) != 1 || eps[0].End.Before(eps[0].Start) || eps[0].End.IsZero() {
		t.Fatalf("n1 known to hold nothing: %+v open=%v", eps, open)
	}
	setOwned(1)
	p.refreshOwnerKnowledge()
	if eps, open := p.OwnerKnowledge(); !open || len(eps) != 2 {
		t.Fatalf("n1 holding a session again, unsynced: %+v open=%v", eps, open)
	}
	for range MaxEpisodes {
		setOwned(0)
		p.refreshOwnerKnowledge()
		setOwned(1)
		p.refreshOwnerKnowledge()
	}
	if eps, _ := p.OwnerKnowledge(); len(eps) != MaxEpisodes {
		t.Fatalf("episodes kept: %d, want %d", len(eps), MaxEpisodes)
	}
}
