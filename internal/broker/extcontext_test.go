package broker

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/audit"
	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestModuleContextPasswords: a module verifies a stored credential through
// its context the way the built-in authenticators do. Serving, the
// verification goes through the node's cache, so a repeat is a hit; at
// hoardmq check the verifier still verifies, caches nothing, and the context
// carries no services.
func TestModuleContextPasswords(t *testing.T) {
	phc, err := auth.HashPassword([]byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()

	cache := auth.NewVerifyCache(time.Minute, 10)
	serving := moduleContext(cfg, log, services{}, Hooks{}, cache)
	for i := 0; i < 2; i++ {
		if ok, err := serving.Passwords.Verify(phc, []byte("pw")); !ok || err != nil {
			t.Fatalf("serving verification %d: %v, %v", i, ok, err)
		}
	}
	if ok, _ := serving.Passwords.Verify(phc, []byte("wrong")); ok {
		t.Fatal("a wrong password verified")
	}
	if st := cache.Stats(); st.Hits != 1 || st.Derivations != 2 {
		t.Fatalf("cache stats %+v, want 1 hit and 2 derivations", st)
	}

	checking := moduleContext(cfg, log, nil, Hooks{}, nil)
	if checking.Passwords == nil {
		t.Fatal("hoardmq check's context has no password verifier")
	}
	if ok, err := checking.Passwords.Verify(phc, []byte("pw")); !ok || err != nil {
		t.Fatalf("check-time verification: %v, %v", ok, err)
	}
	if checking.Services != nil {
		t.Fatal("hoardmq check's context carries services")
	}
}

// TestModuleAuditor: a module's audit event is queued without waiting; with
// the queue full it is dropped, counted in the log's own drops, and Audit
// reports false. Without an audit log Audit reports false.
func TestModuleAuditor(t *testing.T) {
	s := &Server{}
	a := moduleAuditor{s: s}
	if a.Audit("c", "payload.invalid", "r", "x", nil) {
		t.Fatal("recorded with no audit log")
	}
	l, err := audit.Open(t.TempDir(), audit.Options{Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	l.Pause()
	t.Cleanup(l.Resume) // before Close, which waits for the writer
	s.audit.Store(l)
	if !a.Audit("c", "payload.invalid", "r", "x", nil) {
		t.Fatal("the first event was not queued")
	}
	done := make(chan bool, 1)
	go func() { done <- a.Audit("c", "payload.invalid", "r", "x", nil) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("an event past a full queue reported queued")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Audit waited on a full queue")
	}
	if st := l.Stats(); st.Dropped != 1 {
		t.Fatalf("dropped %d, want 1", st.Dropped)
	}
}

// TestPayloadSchemaWithAuditOff: with the node's audit log off a module's
// Context.Audit is nil, and an auditing rule still refuses.
func TestPayloadSchemaWithAuditOff(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Observability.Audit.Enabled = false
		d.Modules["schema.json"] = json.RawMessage(`{"audit":true,"rules":[{"name":"r","filter":"r/#","schema":{"type":"object"}}]}`)
	})
	pub, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	pub.send(&packet.Publish{Topic: "r/1", QoS: 1, PacketID: 1, Payload: []byte(`[]`)})
	if a := pub.read().(*packet.Puback); a.ReasonCode != packet.PayloadFormatInvalid {
		t.Fatalf("refusal with the audit log off: %s", a.ReasonCode)
	}
	// The module was handed no auditor, so it tried to record nothing: an
	// auditor over a missing log would have reported each event dropped.
	if got := f.series("hoardmq_payload_schema_refusals_total", map[string]string{"rule": "r", "action": "drop"}); got != 1 {
		t.Fatalf("refusals %v, want 1", got)
	}
	if got := f.series("hoardmq_payload_schema_audit_dropped_total", nil); got != 0 {
		t.Fatalf("audit dropped %v with the audit log off", got)
	}
}
