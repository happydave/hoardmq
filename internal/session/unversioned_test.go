package session

import (
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

func acknowledged(code packet.ReasonCode) bool {
	return code == packet.Success || code == packet.NoMatchingSubscribers
}

// TestUnversionedWriteOverAVersionedEntryIsRefused: a retained write the
// replication stores unversioned (the shard has no leader, or there is no
// map) over a topic whose entry is stamped is refused. Taken, it replaced the
// stamped entry in memory and was acknowledged, and a reload, which merges by
// version, put the stamped one back: an acknowledged write reverted.
func TestUnversionedWriteOverAVersionedEntryIsRefused(t *testing.T) {
	st, _ := rollingJournal(t)
	m, _ := newManager(t, func(o *Options) { o.Journal = st })
	rep := &leaderReplication{epoch: 3}
	m.SetReplication(rep)
	c := newFake("c", packet.V5, 100)
	connectV5(m, c, true, 0, nil)
	if code, _ := m.Publish(c, &packet.Publish{Topic: "r", QoS: 1, PacketID: 1, Retain: true, Payload: []byte("stamped")}); !acknowledged(code) {
		t.Fatalf("precondition: the stamped write was answered %v", code)
	}
	rep.epoch = 0
	code, _ := m.Publish(c, &packet.Publish{Topic: "r", QoS: 1, PacketID: 2, Retain: true, Payload: []byte("later")})
	rec, err := st.Recover()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, _ := newManager(t, nil)
	reloaded.Load(rec)
	after := string(held(t, reloaded, "r").Msg.Payload)
	if acknowledged(code) {
		if after != "later" {
			t.Fatalf("the unversioned write was acknowledged (%v), and the reload holds %q: an acknowledged write reverted", code, after)
		}
		t.Fatalf("the unversioned write over a stamped entry was acknowledged (%v), want refused", code)
	}
	if code != packet.ImplementationSpecificError {
		t.Fatalf("the unversioned write was answered %v, want ImplementationSpecificError", code)
	}
	if now := string(held(t, m, "r").Msg.Payload); now != "stamped" || after != "stamped" {
		t.Fatalf("held %q in memory and %q after the reload, want the stamped write in both", now, after)
	}
}
