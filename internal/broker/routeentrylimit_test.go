package broker

import (
	"slices"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/routing"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// A clustered node refuses a filter whose route entry would not fit a
// cluster frame, 0x97 at 5.0 and 0x80 at 3.1.1, and takes one that would;
// the other nodes could never be told of the refused one. A single node,
// which sends no route entry, takes it.
func TestSubscribeRefusesAFilterNoFrameCarries(t *testing.T) {
	const limit = 65536
	fits := func(f string) bool { return routing.EntryFits("default", f, "n1", limit) }
	n := len("a/")
	for fits("a/" + strings.Repeat("x", n)) {
		n += 512
	}
	for !fits("a/" + strings.Repeat("x", n)) {
		n--
	}
	under, over := "a/"+strings.Repeat("x", n), "a/"+strings.Repeat("x", n+1)

	subscribe := func(cl *client, id uint16, filters ...string) []packet.ReasonCode {
		t.Helper()
		s := &packet.Subscribe{PacketID: id}
		for _, f := range filters {
			s.Subscriptions = append(s.Subscriptions, packet.Subscription{Filter: f, QoS: 1})
		}
		cl.send(s)
		ack, ok := cl.read().(*packet.Suback)
		if !ok || ack.PacketID != id {
			t.Fatalf("SUBACK %d: %+v", id, ack)
		}
		return ack.ReasonCodes
	}

	nodes := startCluster(t, []string{"n1"}, func(_ string, d *config.Document) {
		metricsOn(d)
		d.Cluster.MaxFrameBytes = limit
	})
	f := nodes["n1"]
	v5, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "five", CleanStart: true, KeepAlive: 30}, false)
	if got := subscribe(v5, 1, under, over); !slices.Equal(got, []packet.ReasonCode{1, packet.QuotaExceeded}) {
		t.Fatalf("5.0 SUBACK %v, want [1 0x97]", got)
	}
	v4, _ := f.connect("tcp", packet.V4, &packet.Connect{ClientID: "four", CleanStart: true, KeepAlive: 30}, false)
	if got := subscribe(v4, 1, over); !slices.Equal(got, []packet.ReasonCode{0x80}) {
		t.Fatalf("3.1.1 SUBACK %v, want [0x80]", got)
	}
	_, body := scrape(t, f, "/metrics")
	if v, ok := metricValue(t, body, `hoardmq_subscription_refusals_total{reason="route-entry-too-large"}`); !ok || v != 2 {
		t.Fatalf("refusals on the scrape %v %v, want 2", v, ok)
	}

	single := start(t, func(d *config.Document) { d.Cluster.MaxFrameBytes = limit })
	one, _ := single.connect("tcp", packet.V5, &packet.Connect{ClientID: "one", CleanStart: true, KeepAlive: 30}, false)
	if got := subscribe(one, 1, over); !slices.Equal(got, []packet.ReasonCode{1}) {
		t.Fatalf("a single node's SUBACK %v, want [1]", got)
	}
}
