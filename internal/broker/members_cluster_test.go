package broker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// adminTrio starts three nodes with the admin API on each.
func adminTrio(t *testing.T) map[string]*fixture {
	t.Helper()
	dir := t.TempDir()
	nodes := startTrio(t, func(name string, d *config.Document) {
		withAdmin(t, dir)(d)
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
	})
	waitMap(t, nodes, 3)
	return nodes
}

// voterNames is a node's voters, sorted by the core.
func voterNames(f *fixture) string {
	var out []string
	for _, m := range f.srv.Consensus().Members() {
		if m.Role == consensus.Voter {
			out = append(out, m.Name)
		}
	}
	return strings.Join(out, ",")
}

// others are the node names but those given, in order.
func others(names []string, but ...string) []string {
	var out []string
	for _, n := range names {
		skip := false
		for _, b := range but {
			skip = skip || n == b
		}
		if !skip {
			out = append(out, n)
		}
	}
	return out
}

// TestRemoveAStoppedMember: a stopped member, declared dead and owning no
// lease, is removed by a call to a follower, which forwards it to the leader;
// both record it; the live nodes list two voters. Restarted on its store, the
// removed node never learns it: its log ends before the removal and no member
// sends it anything, so it stays not ready (recovering), and its campaigns
// leave the leader's term alone.
func TestRemoveAStoppedMember(t *testing.T) {
	nodes := adminTrio(t)
	names := []string{"n1", "n2", "n3"}
	lead := leaderOf(nodes)
	rest := others(names, lead)
	caller, target := rest[0], rest[1]
	doc := *nodes[target].srv.config()
	nodes[target].stop()
	waitUntil(t, target+" declared dead and owning no lease", 30*time.Second, func() bool {
		tb := nodes[lead].srv.Leases().Table()
		return !tb.Live(target) && tb.CountOwnedBy(target) == 0
	})

	code, data, h := apiH(t, nodes[caller], "DELETE", "/api/v1/cluster/members/"+target, adminOps, "")
	if code != 200 || h.Get(admin.NodeHeader) != lead {
		t.Fatalf("removal through %s: %d from %q: %s", caller, code, h.Get(admin.NodeHeader), data)
	}
	if got := decode[admin.MemberRemoval](t, data); got.Node != target || len(got.Members) != 2 {
		t.Fatalf("the answer: %+v", got)
	}
	want := strings.Join(others(names, target), ",")
	for _, n := range []string{lead, caller} {
		waitUntil(t, n+" to list two voters", 10*time.Second, func() bool { return voterNames(nodes[n]) == want })
	}
	if !auditHas(t, nodes[caller], "token:ops", "cluster.member.remove", target, "ok") || !auditHas(t, nodes[lead], "token:ops", "cluster.member.remove", target, "ok") {
		t.Fatalf("the removal is not audited on both nodes")
	}

	srv, err := NewWithHooks(doc, nil, fixtureHooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	back := &fixture{t: t, srv: srv, ca: nodes[target].ca, dir: nodes[target].dir, client: nodes[target].client}
	t.Cleanup(back.stop)
	term := nodes[lead].srv.Consensus().Status().Term
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		if ready, reason := srv.Readiness(); ready || reason != "recovering" {
			t.Fatalf("the removed node restarted from before its removal, %d s on: ready=%v %q", i+1, ready, reason)
		}
	}
	if got := nodes[lead].srv.Consensus().Status().Term; got != term {
		t.Fatalf("the leader's term moved from %d to %d while the removed node campaigned", term, got)
	}
	if voterNames(nodes[lead]) != want {
		t.Fatalf("the restarted removed node is a voter again: %s", voterNames(nodes[lead]))
	}
	if _, err := os.Stat(doc.Node.DataDir); err != nil {
		t.Fatalf("the removed node's data: %v", err)
	}
}

// TestRemovalRefusals: an unknown node, a live member not drained, one that
// owns a lease, a removal that would lose quorum, and a reader's call are
// refused, and the membership is unchanged.
func TestRemovalRefusals(t *testing.T) {
	nodes := adminTrio(t)
	names := []string{"n1", "n2", "n3"}
	lead := leaderOf(nodes)
	rest := others(names, lead)
	a, b := rest[0], rest[1]
	refused := func(target, reason string, status int) {
		t.Helper()
		code, data := api(t, nodes[lead], "DELETE", "/api/v1/cluster/members/"+target, adminOps, "")
		if code != status || !strings.Contains(string(data), reason) {
			t.Fatalf("removing %s: %d %s, want %d %s", target, code, data, status, reason)
		}
	}
	refused("n9", "not-a-member", 404)
	refused(a, "not-drained", 409)

	exp := uint32(600)
	c, ack := nodes[a].connect("tcp", packet.V5, &packet.Connect{ClientID: "rm1", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect %s", ack.ReasonCode)
	}
	defer func() { _ = c.c.Close() }()
	waitUntil(t, "the leader to see rm1 on "+a, 10*time.Second, func() bool {
		l, ok := nodes[lead].srv.Leases().Table().Lookup("default", "rm1")
		return ok && l.Owner == a
	})
	refused(a, "owns-leases", 409)

	if code, _ := api(t, nodes[lead], "DELETE", "/api/v1/cluster/members/"+a, adminReader, ""); code != 403 {
		t.Fatalf("a reader's removal: %d", code)
	}

	nodes[b].stop()
	waitUntil(t, "the leader to read "+b+" unreachable", 30*time.Second, func() bool { return !nodes[lead].srv.Leases().PeerReachable(b) })
	refused(a, "quorum", 409)
	if got := voterNames(nodes[lead]); got != "n1,n2,n3" {
		t.Fatalf("voters after the refusals: %s", got)
	}
}

// TestRemoveTheLeader: a drained leader asked over HTTP to remove itself
// hands leadership over, and the call goes to the new leader, which removes
// it; the old leader reports `removed`.
func TestRemoveTheLeader(t *testing.T) {
	nodes := adminTrio(t)
	names := []string{"n1", "n2", "n3"}
	lead := leaderOf(nodes)
	rest := others(names, lead)
	srv := nodes[lead].srv
	srv.Leases().PauseRenewals(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := srv.Leases().Handoff(ctx, rest[0]); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, lead+" marked leaving", 10*time.Second, func() bool { return srv.Leases().Table().Leaving(lead) })

	code, data, h := apiH(t, nodes[lead], "DELETE", "/api/v1/cluster/members/"+lead, adminOps, "")
	newLead := h.Get(admin.NodeHeader)
	if code != 200 || newLead == "" || newLead == lead {
		t.Fatalf("the leader's own removal: %d from %q: %s", code, newLead, data)
	}
	want := strings.Join(rest, ",")
	for _, n := range rest {
		waitUntil(t, n+" to list two voters", 10*time.Second, func() bool { return voterNames(nodes[n]) == want })
	}
	waitUntil(t, "the old leader to read removed", 10*time.Second, func() bool {
		_, reason := srv.Readiness()
		return reason == "removed"
	})
}
