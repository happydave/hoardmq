package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// awaitAck registers a takeover pull on a node by hand, so a test can play
// the requester without an acquire.
func awaitAck(n *tnode, req uint64) chan takeoverAck {
	ch := make(chan takeoverAck, 1)
	n.svc.mu.Lock()
	n.svc.pulls[req] = ch
	n.svc.mu.Unlock()
	return ch
}

func recvAck(t *testing.T, who string, ch chan takeoverAck) takeoverAck {
	t.Helper()
	select {
	case a := <-ch:
		return a
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never received an answer to its takeover request", who)
		return takeoverAck{}
	}
}

// TestTakeoverServesTheHighestTokenOnly: two nodes each believe they hold x's
// lease and both ask its previous owner for the session. They are
// constructed directly — no clock is skewed. The one holding the later grant
// gets the state; the other is told it was superseded; the session leaves the
// previous owner once.
func TestTakeoverServesTheHighestTokenOnly(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b, cc := c.nodes["n1"], c.nodes["n2"], c.nodes["n3"]
	res, err := acquire(t, a, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	a.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: res.Epoch}, Subscriptions: []store.SubscriptionRecord{{Filter: "t/x", QoS: 1}}})
	lower, higher := res.Epoch+1, res.Epoch+3
	k := key{"default", "x"}
	toB, toC := awaitAck(b, 901), awaitAck(cc, 902)
	a.svc.serveTakeover(k, []any{
		pendingTakeover{req: takeoverRequest{Req: 901, Namespace: k.ns, ClientID: k.id, Epoch: lower}, peer: "n2"},
		pendingTakeover{req: takeoverRequest{Req: 902, Namespace: k.ns, ClientID: k.id, Epoch: higher}, peer: "n3"},
	})
	if got := recvAck(t, "n3", toC); got.Superseded || got.State == nil || got.State.Epoch != higher || len(got.State.Subscriptions) != 1 {
		t.Fatalf("the holder of the later grant: %+v, want the session stamped %d", got, higher)
	}
	if got := recvAck(t, "n2", toB); !got.Superseded || got.State != nil {
		t.Fatalf("the holder of the earlier grant: %+v, want superseded and no state", got)
	}
	if a.sessions.has("default", "x") {
		t.Fatal("the previous owner still holds the session it handed over")
	}

	// A stale requester arriving alone, after the later grant has been seen:
	// nothing is given up for it.
	a.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: higher}})
	late := awaitAck(b, 903)
	a.svc.serveTakeover(k, []any{pendingTakeover{req: takeoverRequest{Req: 903, Namespace: k.ns, ClientID: k.id, Epoch: lower}, peer: "n2"}})
	if got := recvAck(t, "n2", late); !got.Superseded || got.State != nil {
		t.Fatalf("a lone request below the highest token seen: %+v, want superseded", got)
	}
	if !a.sessions.has("default", "x") {
		t.Fatal("the session was taken on a superseded request's account")
	}
}

// TestSupersededTakeoverRefusesTheConnect: the requester's side. Its acquire
// was granted, and the previous owner answers that a later grant exists; the
// client is refused rather than admitted on an empty session.
func TestSupersededTakeoverRefusesTheConnect(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	res, err := acquire(t, a, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	a.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: res.Epoch}})
	// n1 has heard of a grant far above anything n2 is about to be given.
	a.svc.Tokens().Note("default", "x", res.Epoch+1_000_000)
	_, err = acquire(t, b, "x", nil, 300, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("acquire answered superseded: %v, want the CONNECT refused", err)
	}
	if !a.sessions.has("default", "x") {
		t.Fatal("the previous owner gave the session up to a superseded requester")
	}
}

type opSink struct {
	mu sync.Mutex
	n  map[string]float64
}

func (s *opSink) Add(name string, labels map[string]string, delta float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == nil {
		s.n = map[string]float64{}
	}
	s.n[name+" "+labels["op"]] += delta
}
func (s *opSink) Set(string, map[string]string, float64) {}

func (s *opSink) get(key string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n[key]
}

// TestProposalsAreCountedByKind: what the log is asked to carry, by what it
// is for. A same-owner renewal is told apart from a grant.
func TestProposalsAreCountedByKind(t *testing.T) {
	sink := &opSink{}
	s, err := New(Options{Node: "me", Core: &fakeCore{role: "leader"}, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet, Metrics: sink})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	_, _ = s.proposeVia(context.Background(), []Op{
		{Kind: OpAcquire, ClientID: "a", Node: "me", Now: now},
		{Kind: OpAcquire, ClientID: "a", Node: "me", Epoch: 7, Now: now},
		{Kind: OpAcquire, ClientID: "b", Node: "me", Epoch: 9, Now: now},
		{Kind: OpDisconnected, ClientID: "a", Node: "me", Epoch: 7, Now: now},
		{Kind: OpAlive, Node: "me", Now: now},
	}, transport.LeaseNotification)
	for key, want := range map[string]float64{"acquire": 1, "renew": 2, "disconnected": 1, "alive": 1, "release": 0} {
		if got := sink.get("hoardmq_lease_proposals_total " + key); got != want {
			t.Errorf("proposals{op=%q} = %v, want %v", key, got, want)
		}
	}
}

// belowLevelLeader is a leader whose table says n9 owns a lease and has not
// renewed for an hour, at cluster level 2 with a raise to 3 waiting.
func belowLevelLeader(t *testing.T) *Service {
	t.Helper()
	core := &fakeCore{role: "leader", members: []consensus.Member{{Name: "me", Role: consensus.Voter}, {Name: "n9", Role: consensus.Voter}}}
	s, err := New(Options{Node: "me", Core: core, Machine: consensus.NewMachine(), Sessions: newFakeSessions(), TTL: 2 * time.Second, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	// Every member advertises level 3, so a raise from 2 is there to be made.
	if _, err := s.table.Apply(5, Encode([]Op{
		{Kind: OpAlive, Node: "me", Now: old, Level: 3},
		{Kind: OpAlive, Node: "n9", Now: old, Level: 3},
		{Kind: OpAcquire, Namespace: "default", ClientID: "c", Node: "n9", Now: old},
		{Kind: OpSetLevel, Level: 2, Now: old},
	})); err != nil {
		t.Fatal(err)
	}
	unheardFor(s, "n9", time.Hour)
	return s
}

func pendingOf(s *Service, kind OpKind) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.pending {
		if r.op.Kind == kind {
			n++
		}
	}
	return n
}

// TestNodeBelowTheLevelNeitherAdoptsNorRaises: it serves no client, so what it
// adopted nobody could reach; and it does not understand the level it would
// be raising past.
func TestNodeBelowTheLevelNeitherAdoptsNorRaises(t *testing.T) {
	s := belowLevelLeader(t)
	s.SetBuildLevel(1)
	s.adopt()
	s.raiseLevel()
	if n := pendingOf(s, OpReassign) + pendingOf(s, OpSetLevel); n != 0 {
		t.Fatalf("a leader below the cluster's level proposed %d operations", n)
	}
	s.SetBuildLevel(3)
	s.adopt()
	s.raiseLevel()
	if n, m := pendingOf(s, OpReassign), pendingOf(s, OpSetLevel); n != 1 || m != 1 {
		t.Fatalf("a leader that understands the level proposed %d reassignments and %d raises, want one of each", n, m)
	}
}

// TestRecoveryQueuesTheAdoptionsAReplayedReassignmentImplies: replay runs no
// change hook, so a reassignment applied during recovery leaves this node
// owning sessions it has not fetched. When recovery ends each is queued.
func TestRecoveryQueuesTheAdoptionsAReplayedReassignmentImplies(t *testing.T) {
	sessions := newFakeSessions()
	s, err := New(Options{Node: "me", Core: &fakeCore{role: "follower", leader: "other"}, Machine: consensus.NewMachine(), Sessions: sessions, TTL: 2 * time.Second, Logger: quiet,
		Fallback: func(context.Context, string, string, string) Fetched {
			return Fetched{Answered: true}
		}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.table.SetRecovering(true)
	if _, err := s.table.Apply(8, Encode([]Op{
		{Kind: OpAcquire, Namespace: "default", ClientID: "fetched", Node: "n9", Now: now},
		{Kind: OpAcquire, Namespace: "default", ClientID: "unfetched", Node: "n9", Now: now},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.table.Apply(9, Encode([]Op{{Kind: OpReassign, Node: "n9", To: "me", Now: now.Add(time.Hour)}})); err != nil {
		t.Fatal(err)
	}
	s.table.SetRecovering(false)
	if s.adopting.unfinished() != 0 {
		t.Fatal("replay queued an adoption; the test's premise is that it cannot")
	}
	sessions.put("default", "fetched", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "fetched"}})
	s.recovered()
	if got := s.adopting.unfinished(); got != 1 || !s.adopting.unfinishedFor(key{"default", "unfetched"}) {
		t.Fatalf("after recovery %d adoptions are queued (unfetched queued: %v); want the one session this node owns and does not hold", got, s.adopting.unfinishedFor(key{"default", "unfetched"}))
	}
	if got := s.tokens.Seen("default", "unfetched"); got != 9 {
		t.Fatalf("the registry after recovery reads %d for a lease replayed at 9", got)
	}
}

// TestDroppedWorkEndsItsAdoption: a key the queue gives up on after its
// second panic is an adoption nobody will finish. It must stop counting as
// unfinished, and whoever waits on it must be released.
func TestDroppedWorkEndsItsAdoption(t *testing.T) {
	set := newAdoptionSet()
	k := key{"default", "cursed"}
	set.begin(k, adoptJob{epoch: 4})
	ad, _ := set.claim(k)
	q := newWorkQueue("adopt", 1, quiet, func(key, []any) { panic("always") })
	q.onDrop = set.abandon
	q.start()
	defer q.stopAndWait()
	q.enqueue(k, adoptJob{epoch: 4})
	select {
	case <-ad.done:
	case <-time.After(5 * time.Second):
		t.Fatal("whoever waits on the adoption was never released")
	}
	if got := set.unfinished(); got != 0 {
		t.Fatalf("%d adoptions unfinished after the queue dropped the key", got)
	}
}
