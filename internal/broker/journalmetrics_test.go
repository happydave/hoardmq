package broker

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// onPath limits the fault disk to the files under one directory of a node's
// data, so the raft log and the other store keep syncing.
func onPath(part string) func(string) bool {
	return func(path string) bool { return strings.Contains(path, part) }
}

func withDisk(t *testing.T, d *faultdisk.Disk) {
	t.Helper()
	fixtureHooks = Hooks{Sync: d.Sync}
	t.Cleanup(func() { fixtureHooks = Hooks{} })
}

func metricsOn(d *config.Document) {
	d.Observability.Address = "127.0.0.1:0"
	d.Modules["metrics.prometheus"] = json.RawMessage(`{}`)
}

// TestJournalFailuresReachTheScrape (criterion 3; WI 1817): the node's own
// journal failing under a session, by its fsync and by the roll an append
// makes, is refused to the client and read back from /metrics: the store's
// failed fsyncs and repair, and the session's failed operations by kind.
func TestJournalFailuresReachTheScrape(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/logs/state/")
	withDisk(t, d)
	f := start(t, func(doc *config.Document) {
		metricsOn(doc)
		doc.Node.WALSegmentBytes = 1 << 20
	})
	exp := uint32(600)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "j/#", QoS: 1}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publish := func(id uint16, payload []byte) packet.ReasonCode {
		t.Helper()
		pub.send(&packet.Publish{Topic: "j/1", QoS: 1, PacketID: id, Payload: payload})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}
	if code := publish(1, []byte("ok")); code.IsError() {
		t.Fatalf("precondition: a publish before the fault answered %v", code)
	}
	p := sub.read().(*packet.Publish)
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})

	body := mustScrape(t, f)
	for _, s := range []string{`hoardmq_journal_fsync_failures_total{store="node"}`, `hoardmq_journal_repairs_total{store="node"}`, `hoardmq_journal_repair_failures_total{store="node"}`} {
		requireSeries(t, body, s, func(v float64) bool { return v == 0 }, "0 before the fault")
	}
	requireSeries(t, body, `hoardmq_journal_fsyncs_total{store="node"}`, func(v float64) bool { return v > 0 }, "> 0")
	requireSeries(t, body, `hoardmq_journal_index{store="node"}`, func(v float64) bool { return v > 0 }, "> 0")
	requireSeries(t, body, `hoardmq_journal_checkpoint_age_seconds{store="node"}`, func(v float64) bool { return v >= 0 && v < 60 }, "a recent checkpoint")
	if _, ok := scrapeValue(body, `hoardmq_journal_failures_total{op="sync"}`); ok {
		t.Error("a session journal failure counted before the fault")
	}

	// (a) The group's fsync fails under the publish: refused, and counted.
	d.FailNext(1)
	if code := publish(2, []byte("refused")); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose journal fsync failed answered %v, want 0x83", code)
	}
	p = sub.read().(*packet.Publish)
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	body = mustScrape(t, f)
	requireSeries(t, body, `hoardmq_journal_fsync_failures_total{store="node"}`, func(v float64) bool { return v == 1 }, "1")
	requireSeries(t, body, `hoardmq_journal_failures_total{op="sync"}`, func(v float64) bool { return v >= 1 }, ">= 1")
	if code := publish(3, []byte("after")); code.IsError() {
		t.Fatalf("the publish after the failure answered %v; the repair should release it", code)
	}
	p = sub.read().(*packet.Publish)
	sub.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
	body = mustScrape(t, f)
	requireSeries(t, body, `hoardmq_journal_repairs_total{store="node"}`, func(v float64) bool { return v == 1 }, "1")
	requireSeries(t, body, `hoardmq_journal_checkpoint_index{store="node"}`, func(v float64) bool { return v > 0 }, "the repair's index")

	// (b) The append's roll fails: the subscriber's record is not journaled,
	// and the session's append failure is counted.
	_ = sub.c.Close()
	waitOffline(t, f, "durable")
	fillToRoll(t, publish, 10)
	if _, ok := scrapeValue(mustScrape(t, f), `hoardmq_journal_failures_total{op="append"}`); ok {
		t.Fatal("precondition: an append failure counted before the roll's fault")
	}
	d.FailNext(1)
	code := publish(13, []byte("rolls"))
	body = mustScrape(t, f)
	requireSeries(t, body, `hoardmq_journal_failures_total{op="append"}`, func(v float64) bool { return v >= 1 }, ">= 1")
	requireSeries(t, body, `hoardmq_journal_fsync_failures_total{store="node"}`, func(v float64) bool { return v == 2 }, "2")
	if code != packet.ImplementationSpecificError {
		t.Errorf("a publish whose subscriber's queued record failed to append answered %v, want 0x83", code)
	}
}

func waitOffline(t *testing.T, f *fixture, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := f.srv.Sessions().Record(id); ok && !rec.Online {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("precondition: %s's session did not go offline", id)
}

// fillToRoll publishes two records of about 800 KiB in the journal's JSON
// into a node journal whose segments roll at 1 MiB: the first append finds the
// segment under 1 MiB and the second leaves it past, so the next append rolls
// it, and the roll's fsync of the old segment is the next fsync the disk sees.
func fillToRoll(t *testing.T, publish func(uint16, []byte) packet.ReasonCode, first uint16) {
	t.Helper()
	big := bytes.Repeat([]byte("q"), 600<<10)
	for id := first; id < first+2; id++ {
		if code := publish(id, big); code.IsError() {
			t.Fatalf("filling the segment: publish %d answered %v", id, code)
		}
	}
}

// TestSubscriberAppendFailureRefusesThePublish is WI 1891's defect: a QoS 1
// publish into an offline durable subscriber's queue whose record fails to
// append (its roll's fsync fails) is refused, the copy that exists only in
// memory is withdrawn, and the publisher's retry is the one copy the
// subscriber receives.
func TestSubscriberAppendFailureRefusesThePublish(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/logs/state/")
	withDisk(t, d)
	f := start(t, func(doc *config.Document) { doc.Node.WALSegmentBytes = 1 << 20 })
	exp := uint32(600)
	durable := &packet.Connect{ClientID: "durable", Properties: packet.Properties{SessionExpiryInterval: &exp}}
	durable.CleanStart = true
	sub, _ := f.connect("tcp", packet.V5, durable, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "j/#", QoS: 1}}})
	sub.read()
	_ = sub.c.Close()
	waitOffline(t, f, "durable")
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
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
	d.FailNext(1)
	if code := publish(3, []byte("rolls")); code != packet.ImplementationSpecificError {
		t.Fatalf("a publish whose subscriber's queued record failed to append answered %v, want 0x83", code)
	}
	if d.Calls() == 0 {
		t.Fatal("precondition: the fault disk saw no fsync")
	}
	if code := publish(4, []byte("rolls")); code != packet.Success {
		t.Fatalf("the retry answered %v, want Success once the journal is repaired", code)
	}
	rec, _ := f.srv.Sessions().Record("durable")
	if rec.Queued != 3 {
		t.Fatalf("the subscriber holds %d queued messages, want the two fillers and the retry: the refused copy was kept", rec.Queued)
	}
	durable.CleanStart = false
	again, ack := f.connect("tcp", packet.V5, durable, false)
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

// TestReplicaJournalFailuresReachTheScrape (WI 1817): a replica store's
// failed fsync and its repair are counted under store="replica", and the
// node's own journal reads none.
func TestReplicaJournalFailuresReachTheScrape(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/replica/")
	withDisk(t, d)
	nodes := startTrio(t, func(_ string, doc *config.Document) {
		metricsOn(doc)
		doc.Cluster.Shards = 8
		doc.Cluster.Replicas = 2
	})
	waitMap(t, nodes, 3)
	exp := uint32(600)
	x, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "x", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	x.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "r/#", QoS: 1}}})
	x.read()
	d.FailNext(1)
	for i := uint16(1); i <= 20; i++ {
		x.send(&packet.Publish{Topic: "r/1", QoS: 1, PacketID: i, Payload: []byte("m")})
		for {
			switch pk := x.read().(type) {
			case *packet.Puback:
				if pk.PacketID == i {
					goto acked
				}
			case *packet.Publish:
				x.send(&packet.Puback{Ack: packet.Ack{PacketID: pk.PacketID}})
			}
		}
	acked:
		if d.Calls() > 0 && failuresOn(t, nodes) == 1 {
			break
		}
	}
	var failed string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && failed == "" {
		for name, f := range nodes {
			if v, _ := scrapeValue(mustScrape(t, f), `hoardmq_journal_fsync_failures_total{store="replica"}`); v == 1 {
				failed = name
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if failed == "" {
		t.Fatalf("no node counted the replica store's failed fsync (fault disk calls %d)", d.Calls())
	}
	repaired := false
	for time.Now().Before(deadline) && !repaired {
		x.send(&packet.Publish{Topic: "r/2", QoS: 1, PacketID: 100, Payload: []byte("again")})
		for {
			pk := x.read()
			if a, ok := pk.(*packet.Puback); ok && a.PacketID == 100 {
				break
			}
			if pp, ok := pk.(*packet.Publish); ok {
				x.send(&packet.Puback{Ack: packet.Ack{PacketID: pp.PacketID}})
			}
		}
		v, _ := scrapeValue(mustScrape(t, nodes[failed]), `hoardmq_journal_repairs_total{store="replica"}`)
		repaired = v >= 1
		time.Sleep(50 * time.Millisecond)
	}
	if !repaired {
		t.Fatalf("%s never counted the replica store's repair", failed)
	}
	for name, f := range nodes {
		body := mustScrape(t, f)
		requireSeries(t, body, `hoardmq_journal_fsync_failures_total{store="node"}`, func(v float64) bool { return v == 0 }, "0 on "+name)
		if name != failed {
			requireSeries(t, body, `hoardmq_journal_fsync_failures_total{store="replica"}`, func(v float64) bool { return v == 0 }, "0 on "+name)
		}
	}
}

func failuresOn(t *testing.T, nodes map[string]*fixture) float64 {
	t.Helper()
	var n float64
	for _, f := range nodes {
		v, _ := scrapeValue(mustScrape(t, f), `hoardmq_journal_fsync_failures_total{store="replica"}`)
		n += v
	}
	return n
}
