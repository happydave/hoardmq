package sim

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestAckCountsSubscribersOnOtherNodes: a publish whose only subscriber is on
// another node reached a subscriber, so its PUBACK and PUBREC carry Success;
// a publish nobody subscribes to anywhere still carries No matching
// subscribers (WI 1487).
func TestAckCountsSubscribersOnOtherNodes(t *testing.T) {
	c := New(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sub := NewClient("sub-remote", []string{c.Node("n2").TCP}, 600, 1)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Subscribe(ctx, "ack/t", 2); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nodes, _ := c.Node("n1").Server().Routes().Table().Match("default", "ack/t"); len(nodes) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	rc := dialRaw(t, c.Node("n1").TCP, "pub-remote", 45*time.Second)

	// An acknowledgement of 0x83 says the broker could not confirm delivery in
	// time, which says nothing about the reason code under test, so confirmed
	// retries such a publish as a new message within this budget (WI 1487).
	budget := time.Now().Add(20 * time.Second)
	var confirmed []uint64
	acked := func(qos byte, topic string) packet.ReasonCode {
		t.Helper()
		code, seq := rc.confirmed(qos, topic, budget, func() string { return "failure counters: " + failureCounters(c) })
		if topic == "ack/t" {
			confirmed = append(confirmed, seq)
		}
		return code
	}
	if code := acked(1, "ack/t"); code != packet.Success {
		t.Fatalf("QoS 1 to a subscriber on n2: %s, want Success", code)
	}
	if code := acked(2, "ack/t"); code != packet.Success {
		t.Fatalf("QoS 2 to a subscriber on n2: %s, want Success", code)
	}
	if code := acked(1, "nobody/t"); code != packet.NoMatchingSubscribers {
		t.Fatalf("QoS 1 to no subscriber anywhere: %s, want No matching subscribers", code)
	}

	// The confirmed messages to ack/t reached the subscriber: the
	// acknowledgements above describe deliveries that happened.
	seen := map[uint64]bool{}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(seen) < len(confirmed) {
		for _, d := range sub.TakeDeliveries() {
			for _, s := range confirmed {
				if d.Seq == s {
					seen[s] = true
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(seen) != len(confirmed) {
		t.Fatalf("the subscriber on n2 received %d of the %d confirmed messages %v", len(seen), len(confirmed), confirmed)
	}
}

// failureCounters renders every node's publish-failure counters, so a test
// that could not get a publish confirmed says which path refused it.
func failureCounters(c *Cluster) string {
	var parts []string
	for _, n := range c.Running() {
		srv := n.Server()
		if srv == nil {
			continue
		}
		snap, ok := srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
		if !ok {
			continue
		}
		for _, m := range snap.Snapshot() {
			if strings.Contains(m.Name, "failures") || strings.Contains(m.Name, "barrier") || strings.HasPrefix(m.Name, "hoardmq_replication_") {
				parts = append(parts, fmt.Sprintf("%s %s%v=%v", n.Name, m.Name, m.Labels, m.Value))
			}
		}
		if srv.Cluster() != nil {
			for _, pi := range srv.Cluster().Peers() {
				parts = append(parts, fmt.Sprintf("%s peer %s state=%s data=%s readidle=%.0fms dataidle=%.0fms out=%d urgent=%d dataout=%d dataurgent=%d downfor=%.0fms",
					n.Name, pi.Name, pi.State, pi.DataState, pi.ReadIdleMs, pi.DataReadIdleMs, pi.OutQueued, pi.UrgentQueued, pi.DataOutQueued, pi.DataUrgentQueued, pi.DownForMs))
			}
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}
