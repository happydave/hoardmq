package broker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/module/testmod"
	"github.com/happydave/hoardmq/internal/packet"
)

// clusterCalls lists the cluster events the test module recorded.
func clusterCalls() []string {
	var out []string
	for _, c := range testmod.Shared.Recorded() {
		if strings.HasPrefix(c, "cluster ") {
			out = append(out, c)
		}
	}
	return out
}

func waitForCall(t *testing.T, want string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if index(testmod.Shared.Recorded(), want) >= 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%q not recorded; cluster events:\n%s", want, strings.Join(clusterCalls(), "\n"))
}

// TestClusterEventsOnACluster: on three nodes with the test module, a
// takeover is reported once, by the new owner; a drain by the draining node;
// and a member's removal by each node left. Each event carries only its own
// fields.
func TestClusterEventsOnACluster(t *testing.T) {
	testmod.Register()
	testmod.Shared.Reset()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		if d.Modules == nil {
			d.Modules = map[string]json.RawMessage{}
		}
		d.Modules[string(testmod.ID)] = json.RawMessage(`{}`)
		d.Cluster.Drain.RatePerSecond = 1000
		d.Cluster.Drain.TimeoutMs = 10000
		d.Cluster.Readiness.StableMs = 0
	})
	deadline := time.Now().Add(15 * time.Second)
	for {
		voters := 0
		for _, m := range nodes["n1"].srv.Consensus().Members() {
			if m.Role == consensus.Voter {
				voters++
			}
		}
		if voters == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("three voters")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Each node's membership watcher takes its baseline at its first tick,
	// and a change made before then is in the baseline, not reported.
	time.Sleep(2 * memberPoll)

	exp := uint32(300)
	x1, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n1: %s", ack.ReasonCode)
	}
	_, ack = nodes["n2"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n2: %s", ack.ReasonCode)
	}
	if d, ok := x1.read().(*packet.Disconnect); !ok || d.ReasonCode != packet.SessionTakenOver {
		t.Fatalf("n1 connection: %+v", d)
	}
	waitForCall(t, "cluster at=n2 lease.moved member= ns=default id=x from=n1 to=n2 timed=true", 5*time.Second)

	nodes["n3"].srv.Drain()
	waitForCall(t, "cluster at=n3 drain.started member=n3 ns= id= from= to= timed=true", 5*time.Second)
	select {
	case <-nodes["n3"].srv.Drained():
	case <-time.After(20 * time.Second):
		t.Fatal("n3's drain did not complete")
	}
	removeMember(t, nodes, "n3")
	for _, at := range []string{"n1", "n2"} {
		waitForCall(t, "cluster at="+at+" member.left member=n3 ns= id= from= to= timed=true", 5*time.Second)
	}

	for _, c := range clusterCalls() {
		switch {
		case strings.Contains(c, " lease.moved "):
			if strings.Contains(c, "at=n1 lease.moved member= ns=default id=x ") {
				t.Errorf("the previous owner reported the move: %s", c)
			}
			if !strings.Contains(c, " member= ") {
				t.Errorf("a lease move carries a member: %s", c)
			}
		case strings.Contains(c, " member.joined "), strings.Contains(c, " member.left "), strings.Contains(c, " drain.started "):
			if !strings.HasSuffix(c, " ns= id= from= to= timed=true") {
				t.Errorf("a membership or drain event carries a lease's fields: %s", c)
			}
		default:
			t.Errorf("an event of no known kind: %s", c)
		}
	}
	for name, f := range nodes {
		if n := f.srv.Registry().Hooks().ClusterDropped()[testmod.ID]; n != 0 {
			t.Errorf("%s dropped %d cluster events", name, n)
		}
	}
}

// removeMember removes target through the leader, handing leadership away
// first when target leads.
func removeMember(t *testing.T, nodes map[string]*fixture, target string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		leader, ok := nodes["n1"].srv.Consensus().Leader()
		if ok {
			if f := nodes[leader]; f != nil {
				_, err := f.srv.removeMember(context.Background(), target)
				if err == nil {
					return
				}
				var ae *admin.Error
				if !errors.As(err, &ae) || time.Now().After(deadline) {
					t.Fatalf("removing %s at %s: %v", target, leader, err)
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no leader removed %s", target)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestUnsubscribeFamilyEndToEnd: the test module's interceptor sees each
// filter of an UNSUBSCRIBE, and its Drop refuses that one at 5.0 while the
// rest unsubscribe.
func TestUnsubscribeFamilyEndToEnd(t *testing.T) {
	f := withTestModule(t, nil)
	testmod.Shared.Set(func(c *testmod.Control) { c.RefuseUnsubscribe = "b" })
	cl, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "u", CleanStart: true}, false)
	cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "a"}, {Filter: "b"}}})
	cl.read()
	cl.send(&packet.Unsubscribe{PacketID: 2, Filters: []string{"a", "b"}})
	u, ok := cl.read().(*packet.Unsuback)
	if !ok || len(u.ReasonCodes) != 2 || u.ReasonCodes[0] != packet.Success || u.ReasonCodes[1] != packet.NotAuthorized {
		t.Fatalf("UNSUBACK %+v, want [0x00 0x87]", u)
	}
	calls := testmod.Shared.Recorded()
	if index(calls, "unsubscribe a") < 0 || index(calls, "unsubscribe b") < 0 || index(calls, "authorize unsubscribe b") >= 0 {
		t.Fatalf("want both filters intercepted and b never authorized; got:\n%s", strings.Join(calls, "\n"))
	}
}

// TestMemberChanges: the joins, then the departures, each in name order; the
// same view raises nothing.
func TestMemberChanges(t *testing.T) {
	before := memberSet([]string{"a", "b", "c"})
	after := memberSet([]string{"a", "d", "c", "e"})
	var got []string
	for _, ev := range memberChanges(before, after) {
		got = append(got, ev.Kind.String()+" "+ev.Member)
	}
	if want := "member.joined d, member.joined e, member.left b"; strings.Join(got, ", ") != want {
		t.Fatalf("changes %q, want %q", strings.Join(got, ", "), want)
	}
	if ch := memberChanges(after, after); len(ch) != 0 {
		t.Fatalf("an unchanged view raised %v", ch)
	}
	if v := memberSet([]string{"a", ""}); len(v) != 1 || !v["a"] {
		t.Fatalf("a member without a name is in the view: %v", v)
	}
	if v := memberSet([]string{""}); v != nil {
		t.Fatalf("a view of no named member is %v, want none", v)
	}
}
