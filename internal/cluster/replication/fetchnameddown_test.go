package replication

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
)

// TestPeerNamedDown: a fetch names a peer it cannot reach as down only under
// a map whose basis no longer holds the previous owner, and never the owner
// itself or a peer back on its control connection.
func TestPeerNamedDown(t *testing.T) {
	without, with := []string{"n2", "n3", "n4"}, []string{"n1", "n2", "n3", "n4"}
	for _, c := range []struct {
		name  string
		prev  string
		basis []string
		peer  string
		reach fetchPeerReach
		want  bool
	}{
		{"owner out of the basis, peer down", "n1", without, "n3", reachDown, true},
		{"owner in the basis, peer down", "n1", with, "n3", reachDown, false},
		{"the owner itself, down", "n1", without, "n1", reachDown, false},
		{"owner out of the basis, peer returning", "n1", without, "n3", reachReturning, false},
		{"owner out of the basis, peer up", "n1", without, "n3", reachAsk, false},
		{"no previous owner", "", without, "n3", reachDown, false},
	} {
		if got := namedDown(c.prev, c.basis, c.peer, c.reach); got != c.want {
			t.Errorf("%s: namedDown = %v, want %v", c.name, got, c.want)
		}
	}
}

// downPeerFixture: n1 owned X and is gone. X's shard is held by n2 and n4, so
// X's one source for n1 is n2, which fetches; n3 holds X's copy with three
// entries and is cut off from n2; n4 holds nothing. basis is the map's.
func downPeerFixture(t *testing.T, basis []string) (h *harness, id string) {
	t.Helper()
	h = newHarnessWith(t, []string{"n1", "n2", "n3", "n4"}, 1, 2, func(o *Options) { o.AckTimeout = time.Second })
	m := shardmap.Map{Version: 1, Basis: basis, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n2", Followers: []string{"n4"}}}}
	h.applyMap(m)
	waitSettled(h, 1)
	id = clientIn(1, 0, "x")
	if got := m.Targets(0, "n1"); len(got) != 1 || got[0] != "n2" {
		t.Fatalf("precondition: X's sources for n1 are %v, want [n2]", got)
	}
	h.setLease("default", id, "n1", 1)
	holdCopy(h, "n3", id, queued(1, "m1"), queued(2, "m2"), queued(3, "m3"))
	h.nodes["n1"].rep.Stop()
	h.nodes["n1"].tr.Stop()
	h.nodes["n2"].tr.Isolate("n3", true)
	h.nodes["n3"].tr.Isolate("n2", true)
	n2 := h.nodes["n2"].rep
	h.waitFor("n2 to read n1 and n3 down", 5*time.Second, func() bool {
		return fetchReach(n2.peerInfo("n1")) == reachDown && fetchReach(n2.peerInfo("n3")) == reachDown
	})
	return h, id
}

// TestFetchNamesADownPeerUnderAMapWithoutThePreviousOwner: under a map that
// no longer holds n1, X's source is n2 itself, and n3, which holds the copy
// n1 fed, is neither a source nor reachable. The fetch finds nothing and
// names n3 down, so the caller waits for it; once n3 is reachable again its
// copy is the answer.
func TestFetchNamesADownPeerUnderAMapWithoutThePreviousOwner(t *testing.T) {
	h, id := downPeerFixture(t, []string{"n2", "n3", "n4"})
	n2 := h.nodes["n2"].rep
	ss, answered, down := n2.Fetch(context.Background(), "default", id, "n1")
	if ss != nil || !answered || !slices.Equal(down, []string{"n3"}) {
		t.Fatalf("the fetch returned a copy %v, answered %v, down %v; want none, answered, and [n3] down", ss != nil, answered, down)
	}
	if st := n2.FetchStats(); st.NonePeerDown != 1 || st.NoneSourceDown != 0 {
		t.Fatalf("NonePeerDown %d, NoneSourceDown %d; want 1 and 0", st.NonePeerDown, st.NoneSourceDown)
	}
	h.nodes["n2"].tr.Isolate("n3", false)
	h.nodes["n3"].tr.Isolate("n2", false)
	h.waitFor("n2 to see n3 Up", 10*time.Second, func() bool { return n2.up("n3") })
	ss, answered, _ = n2.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || !answered || len(ss.Entries) != 3 {
		t.Fatalf("with n3 Up the fetch returned %v (answered %v), want n3's copy with three entries", ss, answered)
	}
}

// TestFetchNamesNoOtherPeerUnderAMapHoldingThePreviousOwner: the same nodes,
// but the map still holds n1, so X's sources are n1's targets and a down peer
// that is not one of them is not named.
func TestFetchNamesNoOtherPeerUnderAMapHoldingThePreviousOwner(t *testing.T) {
	h, id := downPeerFixture(t, []string{"n1", "n2", "n3", "n4"})
	n2 := h.nodes["n2"].rep
	ss, answered, down := n2.Fetch(context.Background(), "default", id, "n1")
	if ss != nil || !answered || len(down) != 0 {
		t.Fatalf("the fetch returned a copy %v, answered %v, down %v; want none, answered and nobody down", ss != nil, answered, down)
	}
	if st := n2.FetchStats(); st.NonePeerDown != 0 {
		t.Fatalf("NonePeerDown %d, want 0", st.NonePeerDown)
	}
}

// TestFetchNamesTheDownSourcesFirst: under a map without n1 whose X source,
// n4, is down as well as n3, the fetch names the source first, so the
// caller waits on it before a peer that may only keep a copy.
func TestFetchNamesTheDownSourcesFirst(t *testing.T) {
	h, id := downPeerFixture(t, []string{"n2", "n3", "n4"})
	h.applyMap(shardmap.Map{Version: 2, Basis: []string{"n2", "n3", "n4"}, Replicas: 2, Shards: []shardmap.Shard{{Leader: "n4", Followers: []string{"n2"}}}})
	n2 := h.nodes["n2"].rep
	h.waitFor("n2 to reconcile map 2", 5*time.Second, func() bool { return n2.Reconciled() == 2 })
	h.nodes["n2"].tr.Isolate("n4", true)
	h.nodes["n4"].tr.Isolate("n2", true)
	h.waitFor("n2 to read n4 down", 5*time.Second, func() bool { return fetchReach(n2.peerInfo("n4")) == reachDown })
	ss, answered, down := n2.Fetch(context.Background(), "default", id, "n1")
	if ss != nil || !answered || !slices.Equal(down, []string{"n4", "n3"}) {
		t.Fatalf("the fetch returned a copy %v, answered %v, down %v; want none, answered, and [n4 n3] down", ss != nil, answered, down)
	}
}

// TestFetchTakesAKeptCopyWhileAPeerIsNamedDown: n4 keeps a copy of X and
// answers while n3 is named down. Every source answered, so the kept copy is
// the fetch's answer.
func TestFetchTakesAKeptCopyWhileAPeerIsNamedDown(t *testing.T) {
	h, id := downPeerFixture(t, []string{"n2", "n3", "n4"})
	holdCopy(h, "n4", id, queued(1, "m1"))
	ss, answered, _ := h.nodes["n2"].rep.Fetch(context.Background(), "default", id, "n1")
	if ss == nil || !answered || len(ss.Entries) != 1 {
		t.Fatalf("the fetch returned %v (answered %v), want n4's kept copy with one entry", ss, answered)
	}
}
