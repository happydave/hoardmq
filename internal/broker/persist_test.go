package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// A durable subscriber's queued and in-flight messages and a retained
// message survive a shutdown and a new server on the same data directory.
func TestStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	acl := filepath.Join(dir, "acl.json")
	if err := os.WriteFile(acl, []byte(`{"rules":[{"topic":"#","allow":["publish","subscribe","unsubscribe"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	mk := func() config.Document {
		doc := config.Default()
		doc.Node.DataDir = filepath.Join(dir, "data")
		doc.Auth.Anonymous = true
		doc.Listeners = []config.Listener{{Name: "tcp", Type: "tcp", Address: "127.0.0.1:0"}}
		doc.Modules = map[string]json.RawMessage{"authz.file": json.RawMessage(`{"path":"` + acl + `"}`)}
		return doc
	}
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("broker log:\n%s", logBuf.String())
		}
	})
	srv, err := New(mk(), logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, srv: srv, dir: dir}
	exp := uint32(3600)
	sub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: true, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	sub.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "q/#", QoS: 2}}})
	sub.read()
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "p", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "q/1", QoS: 2, PacketID: 1, Payload: []byte("inflight")})
	pub.read()
	pub.send(&packet.Pubrel{Ack: packet.Ack{PacketID: 1}})
	pub.read()
	first := sub.readPublish() // left in flight: no PUBREC from the subscriber
	_ = sub.c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := srv.Sessions().Record("durable"); ok && !rec.Online {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := 2; i <= 3; i++ {
		pub.send(&packet.Publish{Topic: "q/2", QoS: 1, PacketID: uint16(i), Payload: []byte{byte('0' + i)}})
		pub.read()
	}
	pub.send(&packet.Publish{Topic: "state/last", QoS: 1, PacketID: 4, Retain: true, Payload: []byte("kept")})
	pub.read()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	// Second life.
	srv2, err := New(mk(), logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv2.Shutdown(ctx)
	})
	f2 := &fixture{t: t, srv: srv2, dir: dir}
	if rec, ok := srv2.Sessions().Record("durable"); !ok || rec.Online || rec.InFlight != 1 || rec.Queued != 2 || rec.Subscriptions != 1 {
		t.Fatalf("recovered record %+v %v", rec, ok)
	}
	// A restarted node admits nobody until it has recovered. A single node
	// is its own only voter, so it elects itself at once rather than waiting
	// out an election timeout (a second at the defaults).
	// Recovery (what the lease service refuses on) must end well inside the
	// election timeout; readiness also waits out the topology-stable window.
	started := time.Now()
	for srv2.Consensus().Recovering() {
		if time.Since(started) > 10*time.Second {
			t.Fatal("still recovering 10 s after the restart")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if took := time.Since(started); took > 250*time.Millisecond {
		t.Fatalf("a sole voter took %v to recover after its restart; it should campaign at once", took)
	}
	for deadline := started.Add(15 * time.Second); ; {
		if ready, _ := srv2.Readiness(); ready {
			break
		}
		if time.Now().After(deadline) {
			_, reason := srv2.Readiness()
			t.Fatalf("not ready after the restart: %s", reason)
		}
		time.Sleep(10 * time.Millisecond)
	}
	sub2, ack := f2.connect("tcp", packet.V5, &packet.Connect{ClientID: "durable", CleanStart: false, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	if !ack.SessionPresent {
		t.Fatal("session not present after restart")
	}
	// All three arrive before any acknowledgement exchange, so collect them
	// first and acknowledge afterwards.
	var got []string
	var pubs []*packet.Publish
	for i := 0; i < 3; i++ {
		p := sub2.readPublish()
		pubs = append(pubs, p)
		got = append(got, string(p.Payload))
		if i == 0 && (!p.Dup || p.PacketID != first.PacketID || p.QoS != 2) {
			t.Errorf("in-flight replay %+v (first id %d)", p, first.PacketID)
		}
	}
	for _, p := range pubs {
		switch p.QoS {
		case 1:
			sub2.send(&packet.Puback{Ack: packet.Ack{PacketID: p.PacketID}})
		case 2:
			sub2.send(&packet.Pubrec{Ack: packet.Ack{PacketID: p.PacketID}})
			for {
				if _, ok := sub2.read().(*packet.Pubrel); ok {
					break
				}
			}
			sub2.send(&packet.Pubcomp{Ack: packet.Ack{PacketID: p.PacketID}})
		}
	}
	if got[0] != "inflight" || got[1] != "2" || got[2] != "3" {
		t.Fatalf("order %v", got)
	}
	sub2.send(&packet.Subscribe{PacketID: 2, Subscriptions: []packet.Subscription{{Filter: "state/+", QoS: 0}}})
	sub2.read()
	if r := sub2.readPublish(); !r.Retain || string(r.Payload) != "kept" {
		t.Fatalf("retained after restart %+v", r)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "data", "logs", "state"))
	if len(entries) == 0 {
		t.Fatal("no log files")
	}
}
