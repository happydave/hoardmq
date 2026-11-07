package session

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// leaderReplication says this node leads every retained shard under map
// version epoch, as a shard leader's replication does.
type leaderReplication struct{ epoch uint64 }

func (r *leaderReplication) Barrier(context.Context, []store.Scope, []message.ForwardRef) error {
	return nil
}
func (r *leaderReplication) Durable(context.Context, []store.Scope) error { return nil }
func (r *leaderReplication) StoreRetained(*message.Message) bool          { return true }
func (r *leaderReplication) StoreRetainedAt(*message.Message, string) (bool, uint64) {
	return true, r.epoch
}
func (r *leaderReplication) LeadsRetained(string, string) (uint64, bool) { return r.epoch, true }
func (r *leaderReplication) QueryRetained(string, string, time.Time) []*message.Message {
	return nil
}

func held(t *testing.T, m *Manager, topic string) RetainedEntry {
	t.Helper()
	e, ok := m.router.HeldRetained("default", topic)
	if !ok {
		t.Fatalf("nothing held for %s", topic)
	}
	return e
}

// TestStampedWritesRiseInAnEpoch: at the shard's leader a client's retained
// write is stamped (epoch, n), and a later write outranks it in the same epoch
// whatever happened between: the message's expiry (its tombstone at the next
// number), or a tombstone's drop.
func TestStampedWritesRiseInAnEpoch(t *testing.T) {
	m, clock := newManager(t, nil)
	m.SetReplication(&leaderReplication{epoch: 4})
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	exp := uint32(1)
	m.Publish(c, &packet.Publish{Topic: "r", Retain: true, Payload: []byte("m1"), Properties: packet.Properties{MessageExpiryInterval: &exp}})
	first := held(t, m, "r")
	if first.RV.E != 4 || first.RV.N == 0 || first.Tombstone {
		t.Fatalf("the first write is held at %v (tombstone %v), want epoch 4 stamped", first.RV, first.Tombstone)
	}
	clock.advance(2 * time.Second)
	m.sweepRetained(clock.Now())
	expired := held(t, m, "r")
	if !expired.Tombstone || !first.RV.Less(expired.RV) || expired.RV.E != 4 {
		t.Fatalf("after the expiry the topic holds %v (tombstone %v), want a tombstone above %v", expired.RV, expired.Tombstone, first.RV)
	}
	m.Publish(c, &packet.Publish{Topic: "r", Retain: true, Payload: []byte("m2")})
	second := held(t, m, "r")
	if !expired.RV.Less(second.RV) || string(second.Msg.Payload) != "m2" {
		t.Fatalf("the write after the expiry is held at %v, want above %v", second.RV, expired.RV)
	}
	m.Publish(c, &packet.Publish{Topic: "s", Retain: true})
	clear := held(t, m, "s")
	if !clear.Tombstone || !m.router.DropTombstoneIf("default", "s", clear.RV) {
		t.Fatal("the clear is not a tombstone a compare-and-delete takes")
	}
	m.Publish(c, &packet.Publish{Topic: "s", Retain: true, Payload: []byte("m3")})
	if after := held(t, m, "s"); !clear.RV.Less(after.RV) {
		t.Fatalf("the write after the tombstone's drop is held at %v, want above %v", after.RV, clear.RV)
	}
}

// TestAnswerWaitsOnTheHeldEntrysDurability: a copy at an equal or lower
// version than the held entry is answered durable only once the held entry
// is appended and synced: when its own append failed, the answer appends it
// again and syncs first.
func TestAnswerWaitsOnTheHeldEntrysDurability(t *testing.T) {
	m, d, _ := journaledManager(t, nil)
	newer := store.Version{E: 5, N: 9}
	d.FailNext(1)
	if _, durable, _ := m.InjectRetainedAt(retained("newer"), newer, true); durable {
		t.Fatal("precondition: the held entry's append was meant to fail")
	}
	if e := held(t, m, "r"); e.Appended != 0 {
		t.Fatalf("precondition: the held entry is marked appended at %d", e.Appended)
	}
	d.FailNext(1)
	if installed, durable, _ := m.InjectRetainedAt(retained("older"), store.Version{E: 5, N: 3}, true); installed || durable {
		t.Fatalf("a lower copy over an unappended entry: installed %v durable %v, want neither while the disk fails", installed, durable)
	}
	if installed, durable, _ := m.InjectRetainedAt(retained("older"), store.Version{E: 5, N: 3}, true); installed || !durable {
		t.Fatalf("a lower copy once the disk takes the held entry: installed %v durable %v, want held and durable", installed, durable)
	}
	e := held(t, m, "r")
	if string(e.Msg.Payload) != "newer" || e.Appended == 0 {
		t.Fatalf("the held entry: %q appended at %d, want newer, appended", e.Msg.Payload, e.Appended)
	}
	if installed, durable, _ := m.InjectRetainedAt(retained("newer"), newer, true); installed || !durable {
		t.Fatalf("the equal copy over a durable entry: installed %v durable %v, want held and durable", installed, durable)
	}
}

// TestClearAtTheLimit: with the namespace at its retained-topic limit, a
// clear of a held topic is taken (its message becomes its tombstone, which
// still counts), and a clear of a topic not held is refused, counted as a
// clear's refusal. Each stamped install is counted on the local path.
func TestClearAtTheLimit(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics; o.MaxRetainedTopics = 1 })
	m.SetReplication(&leaderReplication{epoch: 2})
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	m.Publish(c, &packet.Publish{Topic: "a", Retain: true, Payload: []byte("x")})
	if code, _ := m.Publish(c, &packet.Publish{Topic: "a", QoS: 1, PacketID: 1, Retain: true}); code != packet.Success && code != packet.NoMatchingSubscribers {
		t.Fatalf("a clear of a held topic at the limit: %v", code)
	}
	if !held(t, m, "a").Tombstone {
		t.Fatal("the clear left no tombstone")
	}
	if code, _ := m.Publish(c, &packet.Publish{Topic: "b", QoS: 1, PacketID: 2, Retain: true}); code != packet.QuotaExceeded {
		t.Fatalf("a clear of a topic not held at the limit: %v, want QuotaExceeded", code)
	}
	got := metrics.snapshot()
	if v := got[seriesKey("hoardmq_retained_limit_refusals_total", map[string]string{"namespace": "default", "op": "clear"})]; v != 1 {
		t.Fatalf("clear refusals counted %v, want 1", v)
	}
	if v := got[seriesKey("hoardmq_retained_merges_total", map[string]string{"path": "local", "outcome": "newer"})]; v != 2 {
		t.Fatalf("local stamped installs counted %v, want 2 (the write and the clear taken)", v)
	}
}

// TestLoadKeepsVersionsAndTombstones: a recovered state's versions and
// tombstones are installed, and the next stamp is above every number in it.
func TestLoadKeepsVersionsAndTombstones(t *testing.T) {
	metrics := &seriesMetrics{}
	m, _ := newManager(t, func(o *Options) { o.Metrics = metrics })
	far := uint64(1) << 62
	st := store.NewState()
	st.Retained = append(st.Retained, retained("x"))
	st.RetainedVersions = append(st.RetainedVersions, store.RetainedVersion{Namespace: "default", Topic: "r", RV: store.Version{E: 3, N: far}})
	st.RetainedTombstones = append(st.RetainedTombstones, store.Tombstone{Namespace: "default", Topic: "gone", RV: store.Version{E: 3, N: far + 1}})
	m.Load(st)
	if e := held(t, m, "r"); e.RV != (store.Version{E: 3, N: far}) {
		t.Fatalf("r loaded at %v", e.RV)
	}
	if e := held(t, m, "gone"); !e.Tombstone {
		t.Fatal("the tombstone did not load")
	}
	if v := metrics.snapshot()[seriesKey("hoardmq_retained_merges_total", map[string]string{"path": "reload", "outcome": "newer"})]; v != 2 {
		t.Fatalf("reloaded installs counted %v, want 2, r and the tombstone for gone", v)
	}
	m.SetReplication(&leaderReplication{epoch: 3})
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	m.Publish(c, &packet.Publish{Topic: "r", Retain: true, Payload: []byte("y")})
	if e := held(t, m, "r"); e.RV.N <= far+1 {
		t.Fatalf("the stamp after the load is %v, want above %d", e.RV, far+1)
	}
}
