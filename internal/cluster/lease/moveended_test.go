package lease

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// movesEnded records what MoveEnded was told, per node.
type movesEnded struct {
	mu    sync.Mutex
	calls map[string][]string
}

func (r *movesEnded) options(o *Options) {
	node := o.Node
	o.NextPublishSeq = func() uint64 { return 77 }
	o.MoveEnded = func(ns, id, owner string, mark uint64) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.calls == nil {
			r.calls = map[string][]string{}
		}
		b, _ := json.Marshal([]any{ns, id, owner, mark})
		r.calls[node] = append(r.calls[node], string(b))
	}
}

func (r *movesEnded) on(node string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls[node]...)
}

// recordingTakeoverCluster is takeoverCluster with every node's MoveEnded
// recorded and its publish sequence reading 77.
func recordingTakeoverCluster(t *testing.T, rec *movesEnded) (a, b *tnode) {
	t.Helper()
	fixtureOptions = func(o *Options) {
		o.AdoptionDelay, o.AdoptRetryDelay = time.Hour, time.Hour
		rec.options(o)
	}
	fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) { return nil, true }
	t.Cleanup(func() { fixtureOptions, fixtureFallback = nil, nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	return c.nodes["n1"], c.nodes["n2"]
}

// TestConfirmationEndsTheGiversMove: the giver drops its seal on the
// confirmation and then tells the routing plane which node holds the client
// and the publish sequence that node reported.
func TestConfirmationEndsTheGiversMove(t *testing.T) {
	var rec movesEnded
	a, b := recordingTakeoverCluster(t, &rec)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, false)
	if err != nil || res.State == nil || res.PrevOwner != "n2" {
		t.Fatalf("the takeover: %+v, %v", res, err)
	}
	st := res.State
	st.Epoch = res.Epoch
	a.sessions.Import(st)
	waitFor(t, "the seal to end on the confirmation", 5*time.Second, func() bool { return b.sessions.sealedCount() == 0 })
	waitFor(t, "the giver's move to end", 5*time.Second, func() bool { return len(rec.on("n2")) >= 1 })
	if got := rec.on("n2"); len(got) != 1 || got[0] != `["default","x","n1",77]` {
		t.Fatalf("the giver was told %v; want the new owner n1 with its mark 77, once", got)
	}
	if got := rec.on("n1"); len(got) != 0 {
		t.Fatalf("the new owner ended a move of its own: %v", got)
	}
}

// TestConfirmationThatEvictsEndsTheMove: a session never sealed (no pull
// came) is evicted by the confirmation, and the move that eviction starts is
// ended with it.
func TestConfirmationThatEvictsEndsTheMove(t *testing.T) {
	var rec movesEnded
	a, b := recordingTakeoverCluster(t, &rec)
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	b.sessions.put("default", "x", sessionFor("x"))
	res, err := acquire(t, a, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	a.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: res.Epoch}})
	waitFor(t, "the previous owner's session to be reclaimed", 5*time.Second, func() bool { return !b.sessions.has("default", "x") })
	waitFor(t, "the giver's move to end", 5*time.Second, func() bool { return len(rec.on("n2")) >= 1 })
	if got := rec.on("n2"); len(got) != 1 || got[0] != `["default","x","n1",77]` {
		t.Fatalf("the giver was told %v; want the new owner n1 with its mark 77, once", got)
	}
}

// TestIgnoredConfirmationEndsNoMove: a confirmation below a re-sealed copy's
// token changes nothing, so it ends no move; the highest queued one that
// drops the copy ends it with its own sender and mark.
func TestIgnoredConfirmationEndsNoMove(t *testing.T) {
	var rec movesEnded
	fixtureOptions = rec.options
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	n.sessions.mu.Lock()
	n.sessions.sealed[n.sessions.key("default", "x")] = &fakeSealed{st: sessionFor("x"), token: 50}
	n.sessions.mu.Unlock()
	n.svc.serveConfirmed(key{"default", "x"}, []any{confirmIn{token: 40, peer: "n9", fromSeq: 5}})
	if got := rec.on("n1"); len(got) != 0 {
		t.Fatalf("an ignored confirmation ended a move: %v", got)
	}
	n.svc.serveConfirmed(key{"default", "x"}, []any{confirmIn{token: 40, peer: "n9", fromSeq: 5}, confirmIn{token: 60, peer: "n8", fromSeq: 9}})
	if got := rec.on("n1"); len(got) != 1 || got[0] != `["default","x","n8",9]` {
		t.Fatalf("told %v; want n8 with mark 9, once", got)
	}
	n.svc.serveConfirmed(key{"default", "y"}, []any{confirmIn{token: 60, peer: "n8", fromSeq: 9}})
	if got := rec.on("n1"); len(got) != 1 {
		t.Fatalf("a confirmation for a client with nothing here ended a move: %v", got)
	}
}

// TestConfirmationFromAnOlderBuildCarriesNoMark: a confirmation without the
// member decodes with mark 0, which proves nothing.
func TestConfirmationFromAnOlderBuildCarriesNoMark(t *testing.T) {
	var c confirmation
	if err := json.Unmarshal([]byte(`{"ns":"default","id":"x","token":5}`), &c); err != nil || c.FromSeq != 0 || c.Token != 5 {
		t.Fatalf("decoded %+v, %v", c, err)
	}
	b, _ := json.Marshal(confirmation{Namespace: "default", ClientID: "x", Token: 5})
	if string(b) != `{"ns":"default","id":"x","token":5}` {
		t.Fatalf("a confirmation with no mark encodes %s; an older build must see what it sees today", b)
	}
}
