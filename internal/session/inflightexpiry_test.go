package session

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

// inflightExpired is a queue env whose subscriber, online at qos, is sent a
// message with a 10 s expiry, leaves it unacknowledged when it disconnects,
// and reconnects 30 s later: the resumption drops the in-flight message as
// expired instead of retransmitting it.
func inflightExpired(t *testing.T, qos byte) *queueEnv {
	t.Helper()
	e := newQueueEnv(t, qos)
	sub := newFake("sub", packet.V5, 100)
	connectV5(e.m, sub, false, 3600, nil)
	e.m.Publish(e.pub, &packet.Publish{Topic: "t", QoS: qos, PacketID: 1, Payload: []byte("short"), Properties: packet.Properties{MessageExpiryInterval: u32(10)}})
	if n := countPayload(sub.publishes(), "short"); n != 1 {
		t.Fatalf("precondition: the online session was sent the message %d times, want 1", n)
	}
	if ss := e.replica.Find("default", "sub"); len(ss.Entries) != 1 || ss.Entries[0].State == store.StateQueued {
		t.Fatalf("precondition: the replica does not hold the message in flight: %+v", ss.Entries)
	}
	e.m.Disconnect(sub, false, packet.NormalDisconnection, false)
	e.clock.advance(30 * time.Second)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(e.m, sub2, false, 3600, nil)
	if n := countPayload(sub2.publishes(), "short"); n != 0 {
		t.Fatalf("precondition: the expired message was retransmitted %d times", n)
	}
	if rec, _ := e.m.Record("sub"); rec.Expired != 1 || rec.InFlight != 0 {
		t.Fatalf("precondition: the session did not drop the message as expired: %+v", rec)
	}
	return e
}

// TestExpiredInflightMessageLeavesTheLogAndTheReplica: an in-flight message
// dropped for expiry on a resumption leaves the log's state and a replica's,
// and the replica applies the record that drops it.
func TestExpiredInflightMessageLeavesTheLogAndTheReplica(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(map[byte]string{1: "QoS 1", 2: "QoS 2"}[qos], func(t *testing.T) {
			t.Run("the replica's state", func(t *testing.T) {
				e := inflightExpired(t, qos)
				if len(e.unapplied) != 0 {
					t.Fatalf("the replica could not apply %q", e.unapplied)
				}
				if got := entries(e.replica.Find("default", "sub")); len(got) != 0 {
					t.Fatalf("the replica still holds %q after the message was dropped", got)
				}
			})
			t.Run("a restart from the log", func(t *testing.T) {
				e := inflightExpired(t, qos)
				_, rec := e.restart(t)
				if got := entries(rec.Find("default", "sub")); len(got) != 0 {
					t.Fatalf("the log's state still holds %q after the message was dropped", got)
				}
			})
		})
	}
}

// TestOnlyTheExpiredInflightMessageLeavesTheLog: of two messages in flight,
// the one that expired before the resumption leaves the log and the replica,
// and the one that did not stays in both, as it stays in the session.
func TestOnlyTheExpiredInflightMessageLeavesTheLog(t *testing.T) {
	e := newQueueEnv(t, 1)
	sub := newFake("sub", packet.V5, 100)
	connectV5(e.m, sub, false, 3600, nil)
	e.m.Publish(e.pub, &packet.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("short"), Properties: packet.Properties{MessageExpiryInterval: u32(10)}})
	e.m.Publish(e.pub, &packet.Publish{Topic: "t", QoS: 1, PacketID: 2, Payload: []byte("long"), Properties: packet.Properties{MessageExpiryInterval: u32(100)}})
	if got := entries(e.replica.Find("default", "sub")); len(got) != 2 {
		t.Fatalf("precondition: the replica holds %q, want both messages in flight", got)
	}
	e.m.Disconnect(sub, false, packet.NormalDisconnection, false)
	e.clock.advance(30 * time.Second)
	sub2 := newFake("sub", packet.V5, 100)
	connectV5(e.m, sub2, false, 3600, nil)
	if p := sub2.publishes(); len(p) != 1 || string(p[0].Payload) != "long" || !p[0].Dup {
		t.Fatalf("precondition: the resumption sent %+v, want the unexpired message again", p)
	}
	if len(e.unapplied) != 0 {
		t.Fatalf("the replica could not apply %q", e.unapplied)
	}
	if got := entries(e.replica.Find("default", "sub")); len(got) != 1 || got[0] != "long" {
		t.Fatalf("the replica holds %q, want [long]", got)
	}
	_, rec := e.restart(t)
	if got := entries(rec.Find("default", "sub")); len(got) != 1 || got[0] != "long" {
		t.Fatalf("the log's state holds %q, want [long]", got)
	}
}
