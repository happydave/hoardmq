package replication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/store"
)

// TestRefusalIsScopedToItsSession is WI 1510's central pair. A peer's refusal
// must fail a barrier that names the refused session — the escape, which a
// later accepted frame used to lift — and must not fail one that names only
// other sessions, which is the wedge this work item removes.
func TestRefusalIsScopedToItsSession(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	id := clientIn(2, 0) // leader n1, follower n2
	h.nodes["n1"].leases.set("default", id, "n1", 1)
	h.nodes["n2"].leases.set("default", id, "n2", 2)
	n1 := h.nodes["n1"]
	p := n1.rep.outboundFor("n2")
	refused := store.Scope{Namespace: "default", ClientID: id}

	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: id, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("the refusal to be recorded against its session", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		_, ok := p.refused[refused]
		return ok
	})

	// The wedge, before anything else is pushed. With the refusal still the
	// last frame on the stream, a barrier for an unrelated session used to
	// fail — that is the defect WI 1506 found, and it is only visible here,
	// because any later push escapes the stream-wide mark and hides it.
	t.Run("an unrelated session is not wedged by a refusal on the last frame", func(t *testing.T) {
		unrelated := store.Scope{Namespace: "default", ClientID: clientIn(2, 0, "z")}
		if err := n1.rep.Barrier(context.Background(), []store.Scope{unrelated}); err != nil {
			t.Fatalf("a refusal must not wedge an unrelated session: %v", err)
		}
	})

	// A later accepted frame for an unrelated session. Before this work item
	// it lifted the refusal for every barrier; it must not now.
	other := clientIn(2, 0, "y")
	otherScope := store.Scope{Namespace: "default", ClientID: other}
	if err := n1.own.SessionUpsert(store.SessionMeta{Namespace: "default", ClientID: other, Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	h.waitFor("the later frame to be acknowledged", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.ackSeq >= p.sent
	})

	t.Run("fails a barrier naming the refused session", func(t *testing.T) {
		err := n1.rep.Barrier(context.Background(), []store.Scope{refused})
		if !errors.Is(err, ErrReplicaUnavailable) {
			t.Fatalf("barrier over a refused record should fail: %v", err)
		}
		// The reason is pinned: a timeout would also be a failure, and a
		// control that passes for the wrong reason is not coverage.
		if got := barrierReason(err); got != "not-held" {
			t.Errorf("reason %q, want not-held (%v)", got, err)
		}
	})

	t.Run("passes a barrier naming only other sessions", func(t *testing.T) {
		if err := n1.rep.Barrier(context.Background(), []store.Scope{otherScope}); err != nil {
			t.Fatalf("an unrelated session's barrier must not be blocked: %v", err)
		}
		p.mu.Lock()
		_, still := p.refused[refused]
		p.mu.Unlock()
		if !still {
			t.Error("the pass forgot the refusal instead of scoping around it")
		}
		if got := p.escapes.Load(); got != 0 {
			t.Errorf("escapes = %d, want 0: a barrier passed naming a refused key", got)
		}
	})

	t.Run("the refused record is genuinely absent from the follower", func(t *testing.T) {
		if h.nodes["n2"].rep.ReplicaState().Find("default", id) != nil {
			t.Fatal("the refused record was applied; the test proves nothing")
		}
	})
}

// TestBarrierNamingNothingBlocks: a durability wait that names no records has
// examined nothing, so it cannot honestly report that anything is held.
func TestBarrierNamingNothingBlocks(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	if err := h.nodes["n1"].rep.Barrier(context.Background(), nil); !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("a wait naming nothing must fail, got %v", err)
	}
}

// TestBarrierNamingANothingKeyBlocks: a key naming neither a client nor a
// topic has no targets, so a barrier that waited on its targets would wait on
// nothing for it. It fails as unnamed, alone or beside a real key.
func TestBarrierNamingANothingKeyBlocks(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2"}, 2, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 2))
	n1 := h.nodes["n1"]
	h.writeSession("n1", "real", 1)
	if err := n1.rep.Barrier(context.Background(), []store.Scope{{Namespace: "default", ClientID: "real"}}); err != nil {
		t.Fatalf("precondition: a barrier over a real key: %v", err)
	}
	for name, keys := range map[string][]store.Scope{
		"alone":         {{Namespace: "default"}},
		"beside a real": {{Namespace: "default", ClientID: "real"}, {Namespace: "default"}},
	} {
		err := n1.rep.Barrier(context.Background(), keys)
		if !errors.Is(err, ErrReplicaUnavailable) || !strings.Contains(err.Error(), "named no records") {
			t.Errorf("%s: a barrier naming a key with no identity: %v, want it to fail as unnamed", name, err)
		}
	}
}

// TestRefusalScoping covers the parts that need no cluster: which identity a
// frame is scoped by, and the fallback when it has none.
func TestRefusalScoping(t *testing.T) {
	fresh := func() *outbound {
		p := &outbound{name: "peer"}
		p.cond = sync.NewCond(&p.mu)
		return p
	}

	t.Run("a session frame is scoped by client id", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c1", Topic: "t/1"})
		p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{})
		want := store.Scope{Namespace: "default", ClientID: "c1"}
		if _, ok := p.refusedAmong([]store.Scope{want}); !ok {
			t.Errorf("a session frame carrying a topic must scope by client id, refused=%v", p.refused)
		}
		if p.streamBlocked {
			t.Error("a scopable refusal must not block the stream")
		}
	})

	t.Run("a retained frame is scoped by topic", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindAppend, Namespace: "default", Topic: "sensors/a"})
		p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{})
		want := store.Scope{Namespace: "default", Topic: "sensors/a"}
		if _, ok := p.refusedAmong([]store.Scope{want}); !ok {
			t.Errorf("a retained frame must scope by topic, refused=%v", p.refused)
		}
		if p.streamBlocked {
			t.Error("a retained refusal must not fall back to blocking the stream")
		}
		if got := p.fallbackNoKey.Load(); got != 0 {
			t.Errorf("fallbackNoKey = %d, want 0", got)
		}
	})

	t.Run("a frame with no identity falls back to the whole stream, repairably", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindResyncMark})
		if !p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{}) {
			t.Fatal("an unscopable refusal must ask for the resync that clears it")
		}
		if !p.streamBlocked {
			t.Error("an unscopable refusal must block the stream")
		}
		if got := p.fallbackNoKey.Load(); got != 1 {
			t.Errorf("fallbackNoKey = %d, want 1 — the fallback must be counted", got)
		}
		// Without a hole the block is permanent: startResync refuses to run,
		// and whole() is the only thing that clears streamBlocked.
		if !p.hole {
			t.Fatal("the fallback must mark the stream holed, or nothing can ever repair it")
		}
		if _, ok, _ := p.startResync(time.Now(), 0); !ok {
			t.Fatal("a blocked stream must be resyncable")
		}
		if !p.whole(p.holeGen, time.Now()) {
			t.Fatal("whole reported a competing break")
		}
		if p.streamBlocked || p.hole {
			t.Error("a completed resync must clear the fallback")
		}
	})

	t.Run("the refused set has a cap, and reaching it falls back", func(t *testing.T) {
		p := fresh()
		for i := 0; i <= maxRefusedTracked; i++ {
			p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: fmt.Sprintf("c%d", i)})
			p.failedAt(uint64(i+1), "epoch 1 is behind lease epoch 2", store.Scope{})
		}
		if got := p.fallbackAtCap.Load(); got == 0 {
			t.Error("reaching the cap must fall back and count it")
		}
		if !p.streamBlocked {
			t.Error("reaching the cap must block the stream")
		}
		if !p.hole {
			t.Error("the cap fallback must be repairable too")
		}
	})

	t.Run("an absent attribution is not keyed to the previous refusal", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c1"})
		p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{})
		// A sequence that was never tracked. Scanning and finding nothing must
		// be detected, not left holding c1's key from the refusal above.
		p.failedAt(999, "epoch 1 is behind lease epoch 2", store.Scope{})
		if _, ok := p.refusedAmong([]store.Scope{{Namespace: "default", ClientID: "c2"}}); ok {
			t.Error("an unattributable refusal was keyed to a session it did not belong to")
		}
		if !p.streamBlocked {
			t.Error("an unattributable refusal must fall back to blocking the stream")
		}
	})

	t.Run("a resync clears the refusals it replaced", func(t *testing.T) {
		p := fresh()
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c1"})
		p.failedAt(1, "epoch 1 is behind lease epoch 2", store.Scope{})
		p.push(frame{Kind: kindResyncMark})
		p.failedAt(2, "epoch 1 is behind lease epoch 2", store.Scope{})
		gen := p.holeGen
		p.hole = true
		if !p.whole(gen, time.Now()) {
			t.Fatal("whole reported a competing break")
		}
		if len(p.refused) != 0 || p.streamBlocked {
			t.Errorf("a completed resync must clear both, refused=%v blocked=%v", p.refused, p.streamBlocked)
		}
	})
}
