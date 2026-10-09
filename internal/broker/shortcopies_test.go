package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestShortCopiesGauge (WI 1632): with three copies on three nodes n1's
// persistent sessions and led shards are fully copied; with n3 gone and the
// map moved they have two copies and the gauges count them; with n3 back
// they read zero again.
func TestShortCopiesGauge(t *testing.T) {
	nodes := startTrio(t, func(_ string, d *config.Document) {
		d.Cluster.Shards = 8
		d.Cluster.Replicas = 3
	})
	waitMap(t, nodes, 3)
	n1 := nodes["n1"]
	exp := uint32(600)
	const persistent = 4
	for i := range persistent {
		_, ack := n1.connect("tcp", packet.V5, &packet.Connect{ClientID: fmt.Sprint("p", i), CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect p%d: %s", i, ack.ReasonCode)
		}
	}
	if _, ack := n1.connect("tcp", packet.V5, &packet.Connect{ClientID: "brief", CleanStart: true}, false); ack.ReasonCode != packet.Success {
		t.Fatalf("connect brief: %s", ack.ReasonCode)
	}
	read := func() (sessions, led float64) {
		return metric(n1, "hoardmq_replication_sessions_short", nil), metric(n1, "hoardmq_replication_retained_shards_short", nil)
	}
	if s, l := read(); s != 0 || l != 0 {
		t.Fatalf("three copies on three nodes: %v sessions and %v led shards short", s, l)
	}

	doc := *nodes["n3"].srv.config()
	nodes["n3"].srv.Crash()
	nodes["n3"].stopped = true
	waitMap(t, nodes, 2)
	if s, l := read(); s != persistent || l != 4 {
		t.Fatalf("n3 gone: %v sessions and %v led shards short, want %d and 4", s, l, persistent)
	}

	srv, err := NewWithHooks(doc, nil, fixtureHooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	n3 := &fixture{t: t, srv: srv}
	t.Cleanup(n3.stop)
	nodes["n3"] = n3
	waitMap(t, nodes, 3)
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, l := read()
		if s == 0 && l == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("n3 back: %v sessions and %v led shards still short", s, l)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
