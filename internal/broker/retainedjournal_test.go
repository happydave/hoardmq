package broker

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// TestLeaderRefusesARetainedWriteItsJournalDidNotTake: a retained publish on
// n1 to a topic whose shard n2 leads, forwarded to n2, where the record fails
// to append (its roll's fsync fails) or its sync fails. The
// publish is refused on n1; once n2's journal is repaired, the topic's next
// publish is accepted rather than refused until a resync. An admin delete on
// n1 of that topic, refused by n2 the same way, answers "durable": false.
func TestLeaderRefusesARetainedWriteItsJournalDidNotTake(t *testing.T) {
	var n2State atomic.Pointer[string]
	d := faultdisk.New()
	d.Match = func(path string) bool {
		p := n2State.Load()
		return p != nil && strings.HasPrefix(path, *p)
	}
	withDisk(t, d)
	adminOn := withAdmin(t, t.TempDir())
	nodes := startTrio(t, func(name string, doc *config.Document) {
		doc.Cluster.Shards = 8
		doc.Cluster.Replicas = 2
		if name == "n1" {
			adminOn(doc)
		}
		if name == "n2" {
			doc.Node.WALSegmentBytes = 1 << 20
			metricsOn(doc)
		}
	})
	m := waitMap(t, nodes, 3)
	state := nodes["n2"].dir + "/data/logs/state/"
	n2State.Store(&state)
	led := topicLedBy(m, "r", "n2", 8)
	own := topicLedBy(m, "s", "n1", 8)

	pub, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publish := func(id uint16, topic string, payload []byte) packet.ReasonCode {
		t.Helper()
		pub.send(&packet.Publish{Topic: topic, QoS: 1, Retain: true, PacketID: id, Payload: payload})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}
	big := bytes.Repeat([]byte("q"), 600<<10)
	for id := uint16(1); id <= 2; id++ {
		if code := publish(id, led, big); code.IsError() {
			t.Fatalf("filling n2's segment: publish %d answered %v", id, code)
		}
	}

	// The append rolls, and the roll's fsync fails.
	d.FailNext(1)
	if code := publish(3, led, []byte("a")); code != packet.ImplementationSpecificError {
		t.Fatalf("a retained publish whose record failed to append on its leader answered %v, want 0x83", code)
	}
	requireSeries(t, mustScrape(t, nodes["n2"]), `hoardmq_journal_failures_total{op="append"}`, func(v float64) bool { return v == 1 }, "1: the retained record's")
	if code := publish(4, led, []byte("b")); code.IsError() {
		t.Fatalf("the topic's next publish, once n2's journal is repaired, answered %v", code)
	}

	// The append does not roll, and the sync covering it fails.
	d.FailNext(1)
	if code := publish(5, led, []byte("c")); code != packet.ImplementationSpecificError {
		t.Fatalf("a retained publish whose record's sync failed on its leader answered %v, want 0x83", code)
	}
	requireSeries(t, mustScrape(t, nodes["n2"]), `hoardmq_journal_failures_total{op="sync"}`, func(v float64) bool { return v >= 1 }, ">= 1")
	if code := publish(6, led, []byte("d")); code.IsError() {
		t.Fatalf("the topic's next publish after the failed sync answered %v", code)
	}

	if code := publish(7, own, []byte("e")); code.IsError() {
		t.Fatalf("a retained publish to a topic n1 leads answered %v", code)
	}

	// The delete is handed to n2, whose sync of its record fails.
	d.FailNext(1)
	code, data := api(t, nodes["n1"], "DELETE", "/api/v1/retained?topic="+led, adminOps, "")
	if code != 200 || decode[admin.Durability](t, data).Durable {
		t.Fatalf("an admin delete its leader's journal did not take: %d %s, want 200 durable false", code, data)
	}
	code, data = api(t, nodes["n1"], "DELETE", "/api/v1/retained?topic="+led, adminOps, "")
	if code != 200 || !decode[admin.Durability](t, data).Durable {
		t.Fatalf("the next admin delete of the topic: %d %s, want 200 durable true", code, data)
	}
}

// TestAdminRetainedDeleteWhoseAppendFailedIsNotDurable: a retained delete
// stored on this node whose record fails to append (its roll's fsync fails)
// answers "durable": false.
func TestAdminRetainedDeleteWhoseAppendFailedIsNotDurable(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/logs/state/")
	withDisk(t, d)
	adminOn := withAdmin(t, t.TempDir())
	f := start(t, func(doc *config.Document) {
		adminOn(doc)
		doc.Node.WALSegmentBytes = 1 << 20
	})
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	big := bytes.Repeat([]byte("q"), 600<<10)
	for id := uint16(1); id <= 2; id++ {
		pub.send(&packet.Publish{Topic: "r/" + string(rune('0'+id)), QoS: 1, Retain: true, PacketID: id, Payload: big})
		if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode.IsError() {
			t.Fatalf("filling the segment: publish %d: %+v", id, a)
		}
	}
	d.FailNext(1)
	code, data := api(t, f, "DELETE", "/api/v1/retained?topic=r/1", adminOps, "")
	if code != 200 || decode[admin.Durability](t, data).Durable {
		t.Fatalf("a retained delete whose record failed to append: %d %s, want 200 durable false", code, data)
	}
	code, data = api(t, f, "DELETE", "/api/v1/retained?topic=r/2", adminOps, "")
	if code != 200 || !decode[admin.Durability](t, data).Durable {
		t.Fatalf("a retained delete with no fault: %d %s, want 200 durable true", code, data)
	}
}
