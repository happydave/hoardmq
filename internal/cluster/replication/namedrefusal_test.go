package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

// TestRejectionNamesItsFrame: a follower's rejection carries the key of the
// frame it refused, and an acknowledgement without one decodes as naming
// nothing, which is what an older follower sends.
func TestRejectionNamesItsFrame(t *testing.T) {
	cases := []struct {
		name string
		fr   frame
		want store.Scope
	}{
		{"session", frame{Kind: kindAppend, Seq: 7, Namespace: "default", ClientID: "c1", Topic: "t/1", Epoch: 42}, store.Scope{Namespace: "default", ClientID: "c1", Epoch: 42}},
		{"retained", frame{Kind: kindAppend, Seq: 8, Namespace: "default", Topic: "sensors/a", Epoch: 3}, store.Scope{Namespace: "default", Topic: "sensors/a"}},
		{"no identity", frame{Kind: kindResyncMark, Seq: 9}, store.Scope{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body, err := json.Marshal(rejection(c.fr, "a reason"))
			if err != nil {
				t.Fatal(err)
			}
			var a ack
			if err := json.Unmarshal(body, &a); err != nil {
				t.Fatal(err)
			}
			if !a.Rejected || a.Seq != c.fr.Seq || a.Reason != "a reason" {
				t.Fatalf("decoded %+v from %s", a, body)
			}
			if got := a.named(); got != c.want {
				t.Errorf("named %+v, want %+v (wire %s)", got, c.want, body)
			}
		})
	}
	t.Run("an older follower's rejection names nothing", func(t *testing.T) {
		var a ack
		if err := json.Unmarshal([]byte(`{"seq":5,"rejected":true,"reason":"epoch 1 is behind lease epoch 2"}`), &a); err != nil {
			t.Fatal(err)
		}
		if !a.named().Zero() {
			t.Errorf("named %+v, want nothing", a.named())
		}
	})
}

// TestScopeRefusal is the decision: the window's key, the follower's, or
// neither. A name that disagrees with the window blocks rather than wins.
func TestScopeRefusal(t *testing.T) {
	k := store.Scope{Namespace: "default", ClientID: "k", Epoch: 5}
	kLater := store.Scope{Namespace: "default", ClientID: "k", Epoch: 9}
	other := store.Scope{Namespace: "default", ClientID: "other", Epoch: 5}
	cases := []struct {
		name         string
		windowed     store.Scope
		inWindow     bool
		named        store.Scope
		want         store.Scope
		wantFallback string
	}{
		{"window only", k, true, store.Scope{}, k, ""},
		{"window and the same name", k, true, k, k, ""},
		{"the same key at a later epoch", k, true, kLater, kLater, ""},
		{"window and another name", k, true, other, store.Scope{}, fallbackMismatch},
		{"named outside the window", store.Scope{}, false, k, k, ""},
		{"neither", store.Scope{}, false, store.Scope{}, store.Scope{}, fallbackNoKey},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, fb := scopeRefusal(c.windowed, c.inWindow, c.named)
			if got != c.want || fb != c.wantFallback {
				t.Errorf("scopeRefusal = %+v, %q; want %+v, %q", got, fb, c.want, c.wantFallback)
			}
		})
	}
}

// deepStream pushes a frame for k and then enough frames for other clients
// that the attribution window no longer holds k's sequence.
func deepStream(t *testing.T, k string) *outbound {
	t.Helper()
	p := &outbound{name: "peer"}
	p.cond = sync.NewCond(&p.mu)
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: k, Epoch: 1})
	for i := 0; i < maxPushedTracked; i++ {
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: fmt.Sprintf("o%d", i), Epoch: 1})
	}
	if _, in := p.windowedLocked(1); in {
		t.Fatal("precondition not reached: the window still holds the first frame")
	}
	return p
}

// TestRefusalOutsideTheWindowIsScopedByItsName is WI 1802's defect at the
// size of one stream: a refusal the window has lost blocked every barrier to
// the peer. Named by the follower, it is held against its own key only.
func TestRefusalOutsideTheWindowIsScopedByItsName(t *testing.T) {
	k := store.Scope{Namespace: "default", ClientID: "k"}
	unrelated := store.Scope{Namespace: "default", ClientID: "o7"}

	t.Run("named", func(t *testing.T) {
		p := deepStream(t, "k")
		if p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{Namespace: "default", ClientID: "k", Epoch: 1}) {
			t.Error("a refusal the follower named must not ask for a resync")
		}
		if p.streamBlocked || p.hole {
			t.Errorf("a named refusal must not block the stream (blocked %v, hole %v)", p.streamBlocked, p.hole)
		}
		if n := p.fallbackNoKey.Load() + p.fallbackMismatch.Load(); n != 0 {
			t.Errorf("fallbacks = %d, want 0", n)
		}
		if err := p.refusal([]store.Scope{k}); barrierReason(err) != "not-held" {
			t.Errorf("a barrier naming the refused key: %v, want not-held", err)
		}
		if err := p.refusal([]store.Scope{unrelated}); err != nil {
			t.Errorf("a barrier naming another key must not be refused: %v", err)
		}
	})

	t.Run("unnamed, from an older follower", func(t *testing.T) {
		p := deepStream(t, "k")
		if !p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{}) {
			t.Error("an unscopable refusal must ask for a resync")
		}
		if !p.streamBlocked {
			t.Error("an unscopable refusal must block the stream, as before")
		}
		if got := p.fallbackNoKey.Load(); got != 1 {
			t.Errorf("fallbackNoKey = %d, want 1", got)
		}
	})

	t.Run("named, but before the key's last whole write", func(t *testing.T) {
		p := deepStream(t, "k")
		p.push(frame{Kind: kindSnapshot, Namespace: "default", ClientID: "k", Epoch: 1})
		p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{Namespace: "default", ClientID: "k", Epoch: 1})
		if err := p.refusal([]store.Scope{k}); err != nil {
			t.Errorf("a refusal of the copy a whole write replaces must not be recorded: %v", err)
		}
		if p.streamBlocked {
			t.Error("the stream must not be blocked")
		}
	})
}

// TestNamedRefusalKeepsADeepStreamUnblocked is the same over two replicators
// and the real transport. The follower's worker is held in its lease lookup for
// the refused session while the owner pushes past the window, so the refusal
// always reaches the owner after its attribution has gone: a wrong
// implementation cannot pass by the answer arriving early.
func TestNamedRefusalKeepsADeepStreamUnblocked(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	k := clientIn(2, 0, "k")
	n1.leases.set("default", k, "n1", 1)
	n2.leases.set("default", k, "n2", 2) // n2 has seen the lease move: n1's record is stale there
	release := make(chan struct{})
	var once sync.Once
	held := make(chan struct{})
	n2.leases.onLookup = func(_, id string) {
		if id == k {
			once.Do(func() { close(held) })
			<-release
		}
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	p := n1.rep.outboundFor("n2")

	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: k, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	kSeq := p.sent
	p.mu.Unlock()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: the follower never looked the refused session up")
	}
	for i := 0; i < maxPushedTracked+4; i++ {
		if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: fmt.Sprintf("o%d", i), Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	}
	p.mu.Lock()
	first := uint64(0)
	if len(p.pushed) > 0 {
		first = p.pushed[0].seq
	}
	p.mu.Unlock()
	if first <= kSeq {
		t.Fatalf("precondition not reached: the window still holds the refused frame %d (oldest %d)", kSeq, first)
	}
	close(release)
	h.waitFor("the owner's stream to be acknowledged past the refusal", 20*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.ackSeq >= p.sent
	})

	// A block is cleared by the resync it asks for, so the counters, not the
	// stream's state at the end, are what show whether one happened.
	p.mu.Lock()
	blocked := p.streamBlocked || p.hole
	p.mu.Unlock()
	fallbacks := p.fallbackNoKey.Load() + p.fallbackMismatch.Load()
	if blocks := p.trace.stats(0).Blocks; blocked || fallbacks != 0 || blocks != 0 {
		t.Errorf("the stream was blocked whole by a refusal its follower named (fallbacks %d, blocks %d, blocked now %v)",
			fallbacks, blocks, blocked)
	}
	other := store.Scope{Namespace: "default", ClientID: "o3"}
	if err := n1.rep.Barrier(context.Background(), []store.Scope{other}); err != nil {
		t.Errorf("a barrier naming another session must pass: %v", err)
	}
	err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: k}})
	if !errors.Is(err, ErrReplicaUnavailable) || barrierReason(err) != "not-held" {
		t.Errorf("a barrier naming the refused session: %v, want not-held", err)
	}
	if n2.rep.ReplicaState().Find("default", k) != nil {
		t.Error("the refused record was applied; the test proves nothing")
	}
}

// TestFallbackClassification: every refusal the sender cannot scope is
// sampled with why it could not.
func TestFallbackClassification(t *testing.T) {
	fresh := func() *outbound {
		p := &outbound{name: "peer"}
		p.cond = sync.NewCond(&p.mu)
		return p
	}
	outcome := func(p *outbound) string {
		t.Helper()
		st := p.trace.stats(0)
		if len(st.Fallbacks) != 1 {
			t.Fatalf("fallback samples %+v, want one", st.Fallbacks)
		}
		return st.Fallbacks[0].Outcome
	}
	t.Run("window", func(t *testing.T) {
		p := deepStream(t, "k")
		p.failedAt(1, "r", store.Scope{})
		if got := outcome(p); got != rejWindow {
			t.Errorf("outcome %q, want %q", got, rejWindow)
		}
	})
	t.Run("broken", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "k"})
		p.broken()
		p.ackSeq = 0 // a break acknowledges what it drops; the refusal was already in flight
		p.failedAt(1, "r", store.Scope{})
		if got := outcome(p); got != rejBroken {
			t.Errorf("outcome %q, want %q", got, rejBroken)
		}
	})
	t.Run("acked", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "k"})
		p.acked(1)
		p.failedAt(1, "r", store.Scope{})
		if got := outcome(p); got != rejAcked {
			t.Errorf("outcome %q, want %q", got, rejAcked)
		}
	})
	t.Run("no identity", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindResyncMark})
		p.failedAt(1, "r", store.Scope{})
		if got := outcome(p); got != rejNoIdentity {
			t.Errorf("outcome %q, want %q", got, rejNoIdentity)
		}
	})
	t.Run("mismatch", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "k"})
		if !p.failedAt(1, "r", store.Scope{Namespace: "default", ClientID: "other"}) || !p.streamBlocked {
			t.Error("a name that disagrees with the window must block the stream")
		}
		if got := outcome(p); got != rejMismatch {
			t.Errorf("outcome %q, want %q", got, rejMismatch)
		}
		if got := p.fallbackMismatch.Load(); got != 1 {
			t.Errorf("fallbackMismatch = %d, want 1", got)
		}
	})
	t.Run("at cap", func(t *testing.T) {
		p := fresh()
		for i := 0; i <= maxRefusedTracked; i++ {
			p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: fmt.Sprintf("c%d", i)})
			p.failedAt(uint64(i+1), "r", store.Scope{})
		}
		if got := p.trace.stats(0).Rejections[rejAtCap]; got != 1 {
			t.Errorf("at-cap refusals %d, want 1", got)
		}
	})
}

// TestRejectClass names each reason a follower sends.
func TestRejectClass(t *testing.T) {
	for reason, want := range map[string]string{
		gapReason + ": expected 3, got 5":                                "gap",
		unappliedReason + ": no-session subscribe":                       "unapplied",
		levelReason + " 3 is above this build's 2":                       "feature-level",
		"epoch 4 is behind 9, the highest grant seen for the client":     "behind-seen",
		"epoch 4 is behind lease epoch 9":                                "behind-lease",
		"epoch 4 is owned by n3, not n2":                                 "owned-elsewhere",
		"no epoch, and n2 is not the owner":                              "no-epoch",
		"map version 3 is behind 4":                                      "map-version",
		"followers did not acknowledge: replication: a follower is down": "retained-unacknowledged",
		"snapshot without state":                                         "malformed",
		"store: disk full":                                               "apply",
	} {
		if got := rejectClass(reason); got != want {
			t.Errorf("rejectClass(%q) = %q, want %q", reason, got, want)
		}
	}
}

// TestUnappliedRefusalOutsideTheWindowIsRepaired: a follower whose copy has
// lost a session refuses the owner's next record for it as unapplied. When the
// window has lost that record's sequence, the key the follower named is what
// lets the owner repair that one session rather than resync the stream.
func TestUnappliedRefusalOutsideTheWindowIsRepaired(t *testing.T) {
	h, sinks := withCountersAnd(t, []string{"n1", "n2"}, 2, 2, func(*Options) {})
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	waitSettled(h, 1)
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	k := clientIn(2, 0, "u")
	h.writeSession("n1", k, 1)
	if err := n1.barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	feedSession(n1, k, store.Entry{Seq: 1, Message: &message.Message{Namespace: "default", Topic: "a/b", Payload: []byte("queued")}, QoS: 1})
	if _, err := n2.rep.follower.apply(store.DeleteRecord("default", k)); err != nil {
		t.Fatal(err)
	}
	resyncs := func() float64 {
		return sinks["n1"].get("hoardmq_replication_resyncs_total", map[string]string{"peer": "n2"})
	}
	resyncsBefore := resyncs()

	release := make(chan struct{})
	var once sync.Once
	held := make(chan struct{})
	n2.leases.onLookup = func(_, id string) {
		if id == k {
			once.Do(func() { close(held) })
			<-release
		}
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	p := n1.rep.outboundFor("n2")
	if err := n1.own.Enqueue(store.Key{Namespace: "default", ClientID: k, Epoch: 1}, store.Entry{Seq: 2, Message: &message.Message{Namespace: "default", Topic: "a/b"}, QoS: 1}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	kSeq := p.sent
	p.mu.Unlock()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition not reached: the follower never looked the session up")
	}
	for i := 0; i < maxPushedTracked+4; i++ {
		if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: fmt.Sprintf("v%d", i), Epoch: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, in := p.windowedLocked(kSeq); in {
		t.Fatal("precondition not reached: the window still holds the refused record")
	}
	close(release)
	h.waitFor("the follower to be repaired", 20*time.Second, func() bool {
		return repairs(sinks, "n1", "snapshot") >= 1 || resyncs() > resyncsBefore
	})
	if got := repairs(sinks, "n1", "snapshot"); got != 1 {
		t.Errorf("repairs{snapshot} = %v, want 1: the named session was not repaired", got)
	}
	if got := resyncs(); got != resyncsBefore {
		t.Errorf("resyncs %v → %v: an unapplied refusal the follower named resynced the stream", resyncsBefore, got)
	}
	p.mu.Lock()
	blocked := p.streamBlocked
	p.mu.Unlock()
	if blocked {
		t.Error("the stream was blocked whole")
	}
	h.waitFor("the repaired copy on the follower", 10*time.Second, func() bool {
		return n2.rep.ReplicaState().Find("default", k) != nil
	})
}
