package broker

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store/faultdisk"
)

// waitProbe polls path until it answers code, and reports whether it did.
func waitProbe(t *testing.T, f *fixture, path string, code int, within time.Duration) (int, string, bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		got, body := scrape(t, f, path)
		if got == code {
			return got, body, true
		}
		if time.Now().After(deadline) {
			return got, body, false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLivenessFailsOnceTheConsensusLoopStops: every fsync of the raft log
// fails, so the next save ends the consensus loop. The process keeps
// running; /healthz must then fail so the probe restarts it, and /readyz must
// take it out of service.
func TestLivenessFailsOnceTheConsensusLoopStops(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/logs/raft/")
	withDisk(t, d)
	f := start(t, metricsOn)
	if code, body := scrape(t, f, "/healthz"); code != 200 {
		t.Fatalf("precondition: healthz %d %q before the fault", code, body)
	}

	d.FailNext(1 << 20)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.srv.Consensus().Propose(ctx, consensus.KindConfig, consensus.ConfigCommand("liveness", "x")[1:]); err == nil {
		t.Fatal("precondition: a proposal committed although every raft log fsync fails")
	}

	if code, body, ok := waitProbe(t, f, "/healthz", 503, 3*time.Second); !ok {
		t.Errorf("healthz answered %d %q after the consensus loop stopped on a failed save, want 503", code, body)
	} else if !strings.Contains(body, "consensus") {
		t.Errorf("healthz body %q does not name the consensus loop", body)
	}
	if code, body := scrape(t, f, "/readyz"); code != 503 {
		t.Errorf("readyz answered %d %q after the consensus loop stopped, want 503", code, body)
	}
}

// TestLivenessFailsWhenTheJournalCannotBeRewritten: a record the journal
// synced is damaged on the disk, then an fsync fails. Every rewrite reads the
// damaged record and fails, so the node refuses every QoS 1 or 2 publish and
// would until a restart: /healthz must fail.
func TestLivenessFailsWhenTheJournalCannotBeRewritten(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/logs/state/")
	withDisk(t, d)
	f := start(t, metricsOn)
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	exp := uint32(600)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "l/#", QoS: 1}}})
	sub.read()
	_ = sub.c.Close()
	waitOffline(t, f, "durable")
	publish := func(id uint16) packet.ReasonCode {
		t.Helper()
		pub.send(&packet.Publish{Topic: "l/1", QoS: 1, PacketID: id, Payload: []byte("x")})
		a, ok := pub.read().(*packet.Puback)
		if !ok || a.PacketID != id {
			t.Fatalf("PUBACK %d: %+v", id, a)
		}
		return a.ReasonCode
	}
	if code, body := scrape(t, f, "/healthz"); code != 200 {
		t.Fatalf("precondition: healthz %d %q before the fault", code, body)
	}

	segs, err := filepath.Glob(filepath.Join(f.dir, "data", "logs", "state", "*.seg"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("precondition: no state segment (%v)", err)
	}
	sort.Strings(segs)
	damageFirstRecord(t, segs[len(segs)-1])
	d.FailNext(1)
	if code := publish(1); code != packet.ImplementationSpecificError {
		t.Fatalf("precondition: a publish whose journal fsync failed answered %v, want 0x83", code)
	}
	if code := publish(2); code != packet.ImplementationSpecificError {
		t.Fatalf("precondition: the rewrite over a damaged record succeeded (%v)", code)
	}

	if code, body, ok := waitProbe(t, f, "/healthz", 503, 3*time.Second); !ok {
		t.Fatalf("healthz answered %d %q while the journal cannot be rewritten, want 503", code, body)
	} else if !strings.Contains(body, "journal") {
		t.Errorf("healthz body %q does not name the journal", body)
	}
}

// damageFirstRecord flips the first payload byte of the segment's first
// record, past the 32-byte header and the 9-byte record header.
func damageFirstRecord(t *testing.T, path string) {
	t.Helper()
	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	b := make([]byte, 1)
	if _, err := fh.ReadAt(b, 32+9); err != nil {
		t.Fatalf("precondition: the segment holds no record: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := fh.WriteAt(b, 32+9); err != nil {
		t.Fatal(err)
	}
}

// TestLivenessHoldsWhileTheDiskFails: every fsync of the journal failing
// refuses QoS 1 publishes and fails each rewrite, but nothing on the disk is
// damaged and the store retries, so the node stays live.
func TestLivenessHoldsWhileTheDiskFails(t *testing.T) {
	d := faultdisk.New()
	d.Match = onPath("/data/logs/state/")
	withDisk(t, d)
	f := start(t, metricsOn)
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	exp := uint32(600)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "l/#", QoS: 1}}})
	sub.read()
	_ = sub.c.Close()
	waitOffline(t, f, "durable")

	d.FailNext(1 << 20)
	for id := uint16(1); id <= 2; id++ {
		pub.send(&packet.Publish{Topic: "l/1", QoS: 1, PacketID: id, Payload: []byte("x")})
		if a, ok := pub.read().(*packet.Puback); !ok || a.ReasonCode != packet.ImplementationSpecificError {
			t.Fatalf("precondition: publish %d on a failing disk answered %+v, want 0x83", id, a)
		}
	}
	if v := metric(f, "hoardmq_journal_repair_failures_total", map[string]string{"store": "node"}); v < 1 {
		t.Fatalf("precondition: %v failed rewrites, want at least 1", v)
	}
	if code, body := scrape(t, f, "/healthz"); code != 200 {
		t.Errorf("healthz answered %d %q while the disk fails and nothing is damaged, want 200", code, body)
	}
	d.Heal() // so the shutdown's checkpoint can sync
}
