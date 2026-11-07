package session

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// droppingLeader is a leader's replication whose tombstone drop frees what it
// is told, counting each call.
type droppingLeader struct {
	leaderReplication
	m     *Manager
	asked int
}

func (r *droppingLeader) DropEligibleTombstones(ns string) int {
	r.asked++
	n := 0
	for _, e := range r.m.router.RetainedEntries() {
		if e.Tombstone && e.Msg.Namespace == ns && r.m.router.DropTombstoneIf(ns, e.Msg.Topic, e.RV) {
			n++
		}
	}
	return n
}

// TestLimitAsksForDropsOnlyForAClear: at the retained-topic limit a clear of a
// topic not held asks the replication to drop eligible tombstones and, room
// made, is taken; a message write past the limit asks nothing and is refused.
func TestLimitAsksForDropsOnlyForAClear(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.MaxRetainedTopics = 1 })
	rep := &droppingLeader{leaderReplication: leaderReplication{epoch: 2}, m: m}
	m.SetReplication(rep)
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	m.Publish(c, &packet.Publish{Topic: "a", Retain: true})
	if !held(t, m, "a").Tombstone {
		t.Fatal("precondition: a's clear left no tombstone")
	}
	if code, _ := m.Publish(c, &packet.Publish{Topic: "b", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("x")}); code != packet.QuotaExceeded {
		t.Fatalf("a write past the limit: %v, want QuotaExceeded", code)
	}
	if rep.asked != 0 {
		t.Fatalf("a write past the limit asked for tombstone drops %d times, want none", rep.asked)
	}
	if code, _ := m.Publish(c, &packet.Publish{Topic: "b", QoS: 1, PacketID: 2, Retain: true}); !acknowledged(code) {
		t.Fatalf("a clear of a topic not held, room made by a drop: %v", code)
	}
	if rep.asked != 1 || !held(t, m, "b").Tombstone {
		t.Fatalf("the clear asked %d times and left b's tombstone %v, want asked once and taken", rep.asked, held(t, m, "b").Tombstone)
	}
}

// TestForwardAndAdminClearAreStamped: a forwarded write at the shard's leader
// and the admin delete there are stamped, the delete as a tombstone, each
// counted newer on its path; a forward past the limit is refused by it.
func TestForwardAndAdminClearAreStamped(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics; o.MaxRetainedTopics = 2 })
	m.SetReplication(&leaderReplication{epoch: 6})
	fwd := &message.Message{Namespace: "default", Topic: "f", Payload: []byte("x"), Retain: true}
	if durable, refusal := m.InjectRetainedWriteAt(fwd, 6); !durable || refusal != store.RetainStored {
		t.Fatalf("the forward: durable %v, refusal %v", durable, refusal)
	}
	if e := held(t, m, "f"); e.RV.E != 6 || e.RV.N == 0 {
		t.Fatalf("the forward is held at %v, want stamped in epoch 6", e.RV)
	}
	m.Inject(&message.Message{Topic: "a", Payload: []byte("x"), Retain: true})
	if _, appended := m.DeleteRetained("default", "a"); !appended {
		t.Fatal("the admin delete at the leader was not appended")
	}
	if e := held(t, m, "a"); !e.Tombstone || e.RV.E != 6 {
		t.Fatalf("the admin delete left %+v, want a tombstone stamped in epoch 6", e)
	}
	if _, refusal := m.InjectRetainedWriteAt(&message.Message{Namespace: "default", Topic: "g", Payload: []byte("x"), Retain: true}, 6); refusal != store.RetainOverLimit {
		t.Fatalf("a forward past the limit: refusal %v, want the limit's", refusal)
	}
	got := metrics.snapshot()
	for path, want := range map[string]float64{"forward": 1, "admin": 1} {
		if v := got[seriesKey("hoardmq_retained_merges_total", map[string]string{"path": path, "outcome": "newer"})]; v != want {
			t.Errorf("newer on %s counted %v, want %v", path, v, want)
		}
	}
}

// notLeading names no shard this node leads.
type notLeading struct{ leaderReplication }

func (notLeading) LeadsRetained(string, string) (uint64, bool) { return 0, false }

// TestSweepAwayFromTheLeaderRemovesOnly: at a node that does not lead the
// shard the sweep removes an expired stamped message and stamps nothing.
func TestSweepAwayFromTheLeaderRemovesOnly(t *testing.T) {
	m, clock := newManager(t, nil)
	m.SetReplication(&notLeading{})
	exp := clock.Now().Add(time.Second)
	if installed, _, _ := m.InjectRetainedAt(&message.Message{Namespace: "default", Topic: "r", Payload: []byte("x"), Retain: true, Expiry: exp}, store.Version{E: 3, N: 4}, false); !installed {
		t.Fatal("precondition")
	}
	clock.advance(2 * time.Second)
	m.sweepRetained(clock.Now())
	if e, ok := m.router.HeldRetained("default", "r"); ok {
		t.Fatalf("the non-leader's sweep left %+v, want the topic removed", e)
	}
}

// TestExpiryKeepsAnOlderHandOnOut: at the shard's leader an expired message's
// sweep leaves a tombstone above it, so an older copy handed on afterwards,
// stamped or not, is not installed.
func TestExpiryKeepsAnOlderHandOnOut(t *testing.T) {
	m, clock := newManager(t, nil)
	m.SetReplication(&leaderReplication{epoch: 4})
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	exp := uint32(1)
	m.Publish(c, &packet.Publish{Topic: "r", Retain: true, Payload: []byte("m1"), Properties: packet.Properties{MessageExpiryInterval: &exp}})
	v := held(t, m, "r").RV
	clock.advance(2 * time.Second)
	m.sweepRetained(clock.Now())
	for _, in := range []store.Version{{E: 4, N: v.N - 1}, v, {}} {
		if installed, _, _ := m.InjectRetainedAt(&message.Message{Namespace: "default", Topic: "r", Payload: []byte("old"), Retain: true}, in, true); installed {
			t.Fatalf("an older hand-on at %v installed after the expiry", in)
		}
	}
	if !held(t, m, "r").Tombstone {
		t.Fatal("the expiry's tombstone did not stay")
	}
}

// TestAnswerSyncsAnAppendedEntryFirst: a held entry appended but not yet
// synced (a QoS 0 write's) is synced before an equal or lower copy is
// answered durable.
func TestAnswerSyncsAnAppendedEntryFirst(t *testing.T) {
	m, _, pub := journaledManager(t, nil)
	m.SetReplication(&leaderReplication{epoch: 2})
	m.Publish(pub, &packet.Publish{Topic: "r", Retain: true, Payload: []byte("q0")})
	e := held(t, m, "r")
	if e.Appended == 0 || m.synced.Load() >= e.Appended {
		t.Fatalf("precondition: appended at %d, synced through %d; want appended and not synced", e.Appended, m.synced.Load())
	}
	if installed, durable, _ := m.InjectRetainedAt(retained("older"), store.Version{E: 1, N: 1}, true); installed || !durable {
		t.Fatalf("a lower copy: installed %v durable %v, want held and durable", installed, durable)
	}
	if m.synced.Load() < e.Appended {
		t.Fatalf("answered durable with the held entry's append at %d not synced (synced through %d)", e.Appended, m.synced.Load())
	}
}
