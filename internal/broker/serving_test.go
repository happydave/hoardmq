package broker

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestServingDecision (WI 1571): a node not ready is not serving
// whatever it knows of routes; a ready node that lacks an owner's routes is
// degraded; a ready node that has them serves.
func TestServingDecision(t *testing.T) {
	cases := []struct {
		ready       bool
		reason      string
		ownersKnown bool
		want        bool
		wantReason  string
	}{
		{false, "consensus", false, false, "not-ready: consensus"},
		{false, "draining", true, false, "not-ready: draining"},
		{true, "", false, false, degradedRoutesUnknown},
		{true, "", true, true, ""},
	}
	for _, c := range cases {
		got, reason := serving(c.ready, c.reason, c.ownersKnown)
		if got != c.want || reason != c.wantReason {
			t.Errorf("ready %v (%q), owners known %v: serving %v %q, want %v %q", c.ready, c.reason, c.ownersKnown, got, reason, c.want, c.wantReason)
		}
	}
}

// TestDegradedNodeStaysReady (WI 1571): n3 restarted while n1's route
// syncs to it are dropped does not know the routes of n1, which owns a
// session: it is ready, /servingz says degraded and the gauge is 1; once
// the syncs get through it serves and the gauge is 0.
func TestDegradedNodeStaysReady(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		metricsOn(d)
		d.Cluster.Routing.SyncMs = 1000
	})
	exp := uint32(600)
	if _, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "owned", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false); ack.ReasonCode != packet.Success {
		t.Fatalf("connect on n1: %s", ack.ReasonCode)
	}
	nodes["n1"].srv.Cluster().SetFault("n3", &transport.Fault{Drop: 1, Classes: []transport.Class{transport.RouteUpdate}, Rand: rand.New(rand.NewSource(1))})

	doc := *nodes["n3"].srv.config()
	old := nodes["n3"]
	old.stop()
	srv, err := NewWithHooks(doc, nil, fixtureHooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	n3 := &fixture{t: t, srv: srv, ca: old.ca, dir: old.dir, client: old.client}
	t.Cleanup(n3.stop)

	waitUntil(t, "the restarted n3 ready", 20*time.Second, func() bool {
		ready, _ := srv.Readiness()
		return ready
	})
	if code, body := scrape(t, n3, "/readyz"); code != 200 {
		t.Fatalf("readyz on a degraded node: %d %s", code, body)
	}
	if code, body := scrape(t, n3, "/servingz"); code != 503 || strings.TrimSpace(body) != "degraded: routes-unknown" {
		t.Fatalf("servingz without n1's routes: %d %q", code, body)
	}
	if v := metric(n3, "hoardmq_node_degraded", map[string]string{"reason": "routes-unknown"}); v != 1 {
		t.Fatalf("degraded gauge %v, want 1", v)
	}

	nodes["n1"].srv.Cluster().SetFault("n3", nil)
	waitUntil(t, "n3 serving once n1's sync arrives", 10*time.Second, func() bool {
		code, _ := scrape(t, n3, "/servingz")
		return code == 200
	})
	if v := metric(n3, "hoardmq_node_degraded", map[string]string{"reason": "routes-unknown"}); v != 0 {
		t.Fatalf("degraded gauge %v once serving, want 0", v)
	}
	if v := metric(n3, "hoardmq_node_degraded_seconds", nil); v != 0 {
		t.Fatalf("degraded seconds %v once serving, want 0", v)
	}
	if code, _ := scrape(t, n3, "/readyz"); code != 200 {
		t.Fatalf("readyz once serving: %d", code)
	}
}
