package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// TestEveryUnversionedPathRefusesOverAVersionedEntry: with no leader for the
// shard, a module's publish is dropped, a hand-over (InjectRetained) and a
// forward (InjectRetainedWrite) are refused, and the admin delete is not
// appended, each over a stamped entry, each counted older on its path; a copy
// drop still removes the entry. Into a topic holding no versioned entry each
// is taken.
func TestEveryUnversionedPathRefusesOverAVersionedEntry(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics })
	rep := &leaderReplication{epoch: 3}
	m.SetReplication(rep)
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	for _, tp := range []string{"mod", "hand", "fwd", "adm"} {
		m.Publish(c, &packet.Publish{Topic: tp, Retain: true, Payload: []byte("stamped")})
	}
	rep.epoch = 0
	msg := func(tp, payload string) *message.Message {
		return &message.Message{Namespace: "default", Topic: tp, Payload: []byte(payload), Retain: true}
	}
	m.Inject(msg("mod", "module"))
	if durable, refusal := m.InjectRetained(msg("hand", "handed")); durable || refusal != store.RetainVersionedHeld {
		t.Fatalf("an unversioned hand-over: durable %v, refusal %v; want refused, a versioned entry held", durable, refusal)
	}
	if durable, refusal := m.InjectRetainedWrite(msg("fwd", "forwarded")); durable || refusal != store.RetainVersionedHeld {
		t.Fatalf("an unversioned forward: durable %v, refusal %v; want refused, a versioned entry held", durable, refusal)
	}
	if _, appended := m.DeleteRetained("default", "adm"); appended {
		t.Fatal("an unversioned admin delete over a stamped entry was appended")
	}
	for _, tp := range []string{"mod", "hand", "fwd", "adm"} {
		if e := held(t, m, tp); string(e.Msg.Payload) != "stamped" || e.RV.Zero() {
			t.Errorf("%s holds %q at %v, want the stamped write", tp, e.Msg.Payload, e.RV)
		}
	}
	got := metrics.snapshot()
	for path, want := range map[string]float64{"local": 1, "handover": 1, "forward": 1, "admin": 1} {
		if v := got[seriesKey("hoardmq_retained_merges_total", map[string]string{"path": path, "outcome": "older"})]; v != want {
			t.Errorf("older on %s counted %v, want %v", path, v, want)
		}
	}
	if durable, _ := m.InjectRetained(&message.Message{Namespace: "default", Topic: "hand"}); !durable {
		t.Fatal("a copy drop was not durable")
	}
	if _, ok := m.router.HeldRetained("default", "hand"); ok {
		t.Fatal("a copy drop left the stamped entry")
	}
	m.Inject(msg("free", "module"))
	if durable, refusal := m.InjectRetained(msg("free2", "handed")); !durable || refusal != store.RetainStored {
		t.Fatalf("an unversioned hand-over into an empty topic: durable %v, refusal %v", durable, refusal)
	}
	if durable, refusal := m.InjectRetainedWrite(msg("free", "forwarded")); !durable || refusal != store.RetainStored {
		t.Fatalf("an unversioned forward over an unversioned entry: durable %v, refusal %v", durable, refusal)
	}
	if e := held(t, m, "free"); string(e.Msg.Payload) != "forwarded" {
		t.Fatalf("free holds %q, want the later unversioned write", e.Msg.Payload)
	}
	if _, appended := m.DeleteRetained("default", "free"); !appended {
		t.Fatal("an unversioned admin delete of an unversioned entry was not appended")
	}
}
