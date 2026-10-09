package broker

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestForwardedAppendFailureRefusesThePublish is WI 1891's defect across
// nodes: a QoS 1 publish on n1 is forwarded to n2, where the offline durable
// subscriber's queued record fails to append (its roll's fsync fails on n2).
// n2 refuses that forward, so the publish is refused on n1; the copy n2 held
// only in memory is withdrawn, and the publisher's retry is the one copy the
// subscriber receives.
func TestForwardedAppendFailureRefusesThePublish(t *testing.T) {
	var n2State atomic.Pointer[string]
	d := faultdisk.New()
	d.Match = func(path string) bool {
		p := n2State.Load()
		return p != nil && strings.HasPrefix(path, *p)
	}
	withDisk(t, d)
	nodes := startTrio(t, func(name string, doc *config.Document) {
		if name == "n2" {
			doc.Node.WALSegmentBytes = 1 << 20
			metricsOn(doc)
		}
	})
	state := nodes["n2"].dir + "/data/logs/state/"
	n2State.Store(&state)

	exp := uint32(600)
	durable := &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}
	sub, _ := nodes["n2"].connect("tcp", packet.V5, durable, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "j/#", QoS: 1}}})
	sub.read()
	_ = sub.c.Close()
	waitOffline(t, nodes["n2"], "durable")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := nodes["n1"].srv.Routes().Table().Match("default", "j/1"); len(n) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n, _ := nodes["n1"].srv.Routes().Table().Match("default", "j/1"); len(n) != 1 || n[0] != "n2" {
		t.Fatalf("precondition: n1 routes j/1 to %v, want n2", n)
	}

	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publish := func(id uint16, payload []byte) packet.ReasonCode {
		t.Helper()
		pub.send(&packet.Publish{Topic: "j/1", QoS: 1, PacketID: id, Payload: payload})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}
	fillToRoll(t, publish, 1)
	if _, ok := scrapeValue(mustScrape(t, nodes["n2"]), `hoardmq_forward_failures_total{reason="not-journaled"}`); ok {
		t.Fatal("precondition: n2 refused a forward before the fault")
	}
	d.FailNext(1)
	if code := publish(3, []byte("rolls")); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose forwarded delivery's record failed to append on n2 answered %v, want 0x83", code)
	}
	body := mustScrape(t, nodes["n2"])
	requireSeries(t, body, `hoardmq_journal_fsync_failures_total{store="node"}`, func(v float64) bool { return v == 1 }, "1: the roll's")
	requireSeries(t, body, `hoardmq_forward_failures_total{reason="not-journaled"}`, func(v float64) bool { return v == 1 }, "1")
	if code := publish(4, []byte("rolls")); code != packet.Success {
		t.Fatalf("the retry answered %v, want Success once n2's journal is repaired", code)
	}
	rec, _ := nodes["n2"].srv.Sessions().Record("durable")
	if rec.Queued != 3 {
		t.Fatalf("the subscriber holds %d queued messages on n2, want the two fillers and the retry: the refused copy was kept", rec.Queued)
	}
	durable.CleanStart = false
	again, ack := nodes["n2"].connect("tcp", packet.V5, durable, false)
	if !ack.SessionPresent {
		t.Fatal("precondition: the durable session was not present on reconnect")
	}
	copies := 0
	for i := 0; i < rec.Queued; i++ {
		p, ok := again.read().(*packet.Publish)
		if !ok {
			t.Fatalf("delivery %d was not a PUBLISH", i)
		}
		if string(p.Payload) == "rolls" {
			copies++
		}
		again.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	}
	if copies != 1 {
		t.Fatalf("the subscriber received %d copies of the message, want 1", copies)
	}
}
