package replication

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// TestFetchReach: a fetch asks a peer that is Up, counts one back on its
// control connection as not yet answered, and leaves the rest to the caller as
// down.
func TestFetchReach(t *testing.T) {
	for _, c := range []struct {
		name string
		p    transport.PeerInfo
		want fetchPeerReach
	}{
		{"up", transport.PeerInfo{State: transport.Up, ControlUp: true}, reachAsk},
		{"control only, connecting", transport.PeerInfo{State: transport.Connecting, ControlUp: true}, reachReturning},
		{"control only, down", transport.PeerInfo{State: transport.Down, ControlUp: true}, reachReturning},
		{"no connection", transport.PeerInfo{State: transport.Down}, reachDown},
		{"connecting, no control", transport.PeerInfo{State: transport.Connecting}, reachDown},
		// Read between a control link's end and the peer marked down.
		{"up, control just gone", transport.PeerInfo{State: transport.Up}, reachAsk},
	} {
		if got := fetchReach(c.p); got != c.want {
			t.Errorf("%s: fetchReach = %d, want %d", c.name, got, c.want)
		}
	}
}

// returnOnControlOnly cuts a and b apart, holds their data connection and
// lets them meet again: each then reads the other back on its control
// connection and not Up.
func returnOnControlOnly(h *harness, a, b string) {
	h.t.Helper()
	ta, tb := h.nodes[a].tr, h.nodes[b].tr
	ta.Isolate(b, true)
	tb.Isolate(a, true)
	h.waitFor(a+" to see "+b+" down", 5*time.Second, func() bool { return !h.nodes[a].rep.up(b) && !h.nodes[a].rep.peerInfo(b).ControlUp })
	ta.HoldData(b, true)
	tb.HoldData(a, true)
	ta.Isolate(b, false)
	tb.Isolate(a, false)
	h.waitFor(a+" to see "+b+" back on its control connection", 5*time.Second, func() bool {
		p := h.nodes[a].rep.peerInfo(b)
		return p.ControlUp && p.State != transport.Up
	})
}

// releaseData lets a and b's data connection up again and waits for it.
func releaseData(h *harness, a, b string) {
	h.t.Helper()
	h.nodes[a].tr.HoldData(b, false)
	h.nodes[b].tr.HoldData(a, false)
	h.waitFor(a+" to see "+b+" Up", 10*time.Second, func() bool { return h.nodes[a].rep.up(b) })
}

// TestFetchDoesNotReadAReturningSourceAsDown: X's source n3 holds X with two
// messages and is back on its control connection only; n4 keeps a copy with
// one. The fetch can ask neither n3 nor take n4's copy over it, so it reads
// unanswered with nobody down. Once n3 is Up, its copy is the answer.
func TestFetchDoesNotReadAReturningSourceAsDown(t *testing.T) {
	h, id := keptCopyFixture(t)
	holdCopy(h, "n3", id, queued(1, "queued"), queued(2, "later"))
	returnOnControlOnly(h, "n2", "n3")
	n2 := h.nodes["n2"]
	ss, answered, down := n2.rep.Fetch(context.Background(), "default", id, "n1")
	if ss != nil || answered || len(down) != 0 {
		t.Fatalf("the fetch returned a copy (%v), answered %v, down %v; want no copy, unanswered and nobody down while the source is back on its control connection",
			ss != nil, answered, down)
	}
	if got := n2.rep.FetchStats().NoneReturning; got != 1 {
		t.Fatalf("NoneReturning = %d, want 1", got)
	}
	releaseData(h, "n2", "n3")
	ss, answered, _ = n2.rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || !answered || len(ss.Entries) != 2 {
		t.Fatalf("with n3 Up the fetch returned %v (answered %v), want n3's copy with two entries", ss, answered)
	}
}

// TestFetchDoesNotFindNothingWhileAFormerHolderIsReturning: X's source n3
// answers that it holds nothing, and n4, which keeps the only copy, is back
// on its control connection only. The fetch must not read that as the
// session held nowhere.
func TestFetchDoesNotFindNothingWhileAFormerHolderIsReturning(t *testing.T) {
	h, id := keptCopyFixture(t)
	returnOnControlOnly(h, "n2", "n4")
	n2 := h.nodes["n2"]
	ss, answered, _ := n2.rep.Fetch(context.Background(), "default", id, "n1")
	if ss != nil || answered {
		t.Fatalf("the fetch returned a copy (%v) or read as answered (%v) while n4, which keeps a copy, was back on its control connection only", ss != nil, answered)
	}
	if got := n2.rep.FetchStats().NoneReturning; got != 1 {
		t.Fatalf("NoneReturning = %d, want 1", got)
	}
	releaseData(h, "n2", "n4")
	ss, answered, _ = n2.rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || !answered || len(ss.Entries) != 1 {
		t.Fatalf("with n4 Up the fetch returned %v (answered %v), want n4's kept copy", ss, answered)
	}
}
