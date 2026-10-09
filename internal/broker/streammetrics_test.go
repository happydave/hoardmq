package broker

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// metricValue reads one series from a scrape, by its exact label set.
func metricValue(t *testing.T, body, series string) (float64, bool) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		name, value, ok := strings.Cut(line, " ")
		if !ok || name != series {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		return v, true
	}
	return 0, false
}

// TestMetricsShowABlockedStreamAndWhatClosedConnections: the two series an
// operator has no substitute for — whether this node's replication to a peer
// is carrying barriers *now*, and what closed its clients' connections
// (WI 1523). The first answers WI 1519's question of the product: a node that
// refuses QoS 1 publishes after a peer goes is one whose stream to that peer
// is blocked, and the age says how long it has been.
func TestMetricsShowABlockedStreamAndWhatClosedConnections(t *testing.T) {
	nodes := startTrio(t, func(name string, d *config.Document) {
		d.Cluster.Shards = 8
		if name == "n1" {
			d.Observability.Address = "127.0.0.1:0"
			d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
		}
	})
	waitMap(t, nodes, 3)
	exp := uint32(600)
	// Enough durable sessions that n1 replicates to both of its peers, so a
	// stream to either exists to be blocked.
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("dur-%d", i)
		cl, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s: %s", id, ack.ReasonCode)
		}
		cl.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "m/" + id, QoS: 1}}})
		cl.read()
	}
	// The same client identifier reconnects on another node: that node takes
	// the session away, and n1 closes the connection it had — the close cause
	// an operator would look for.
	taken, ack := nodes["n3"].connect("tcp", packet.V5, &packet.Connect{ClientID: "dur-0", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("takeover connect: %s", ack.ReasonCode)
	}
	_ = taken

	// The old connection closes as part of the takeover, and the takeover
	// detaches it before it takes the state — so this reads as `detach`, not
	// as `take`. Which of the four it is, is the point of the series.
	var body string
	takeDeadline := time.Now().Add(5 * time.Second)
	for {
		_, body = scrape(t, nodes["n1"], "/metrics")
		if v, ok := metricValue(t, body, `hoardmq_connections_closed_total{cause="detach"}`); ok && v >= 1 {
			break
		}
		if time.Now().After(takeDeadline) {
			v, ok := metricValue(t, body, `hoardmq_connections_closed_total{cause="detach"}`)
			t.Fatalf("closes by cause: detach = %v (present %v); want at least 1 after a takeover", v, ok)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, cause := range []string{"take", "evict", "evict-import"} {
		if _, ok := metricValue(t, body, fmt.Sprintf(`hoardmq_connections_closed_total{cause=%q}`, cause)); !ok {
			t.Fatalf("closes by cause: %s is absent; an operator reads the four together", cause)
		}
	}
	// Which peers n1 replicates to depends on where the shard map put these
	// sessions; the series exist for the streams that exist, and the test
	// takes one of them rather than assuming a name.
	victim := ""
	for _, peer := range []string{"n2", "n3"} {
		if v, ok := metricValue(t, body, fmt.Sprintf(`hoardmq_replication_stream_blocked{peer=%q,reason="none"}`, peer)); ok {
			if v != 0 {
				t.Fatalf("stream to %s before any peer goes: %v; want 0", peer, v)
			}
			victim = peer
		}
	}
	if victim == "" {
		t.Fatal("no replication stream from n1 to either peer: the sessions did not reach a target")
	}

	// n2 goes. Its stream carries no barrier until it returns and resyncs, so
	// a publish that needs it would be refused — which is what the gauge is
	// for.
	nodes[victim].stop()
	deadline := time.Now().Add(15 * time.Second)
	var first float64
	for time.Now().Before(deadline) {
		_, body = scrape(t, nodes["n1"], "/metrics")
		if v, ok := metricValue(t, body, fmt.Sprintf(`hoardmq_replication_stream_blocked{peer=%q,reason="holed"}`, victim)); ok && v == 1 {
			first, _ = metricValue(t, body, fmt.Sprintf(`hoardmq_replication_stream_blocked_seconds{peer=%q}`, victim))
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if v, ok := metricValue(t, body, fmt.Sprintf(`hoardmq_replication_stream_blocked{peer=%q,reason="holed"}`, victim)); !ok || v != 1 {
		t.Fatalf("stream to %s after it went: %v (present %v); want 1 holed", victim, v, ok)
	}
	// The age is the half an operator reads to tell "now" from "once".
	time.Sleep(400 * time.Millisecond)
	_, body = scrape(t, nodes["n1"], "/metrics")
	second, ok := metricValue(t, body, fmt.Sprintf(`hoardmq_replication_stream_blocked_seconds{peer=%q}`, victim))
	if !ok || second <= first {
		t.Fatalf("the blocked stream's age did not grow: %v then %v (present %v)", first, second, ok)
	}
}
