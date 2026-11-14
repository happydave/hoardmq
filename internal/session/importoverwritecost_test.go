package session

import (
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/packet"
)

// What a CONNECT inside an import costs the client, each with the CONNECT
// after the import as its control.

func received(c *fakeConn, payload string) int {
	n := 0
	for _, p := range c.publishes() {
		if string(p.Payload) == payload {
			n++
		}
	}
	return n
}

// overwrittenClient connects c2 inside the import (or after it, the control)
// and subscribes it to t once the import has returned; pub is a publisher.
func overwrittenClient(t *testing.T, inside bool) (f *overwriteFixture, c2, pub *fakeConn) {
	t.Helper()
	f = newOverwriteFixture(t, nil)
	pub = newFake("pub", packet.V5, 100)
	connectAt(f.m, pub, true, 0, 1)
	c2 = newFake("c", packet.V5, 100)
	connect := func() { connectAt(f.m, c2, false, 600, 7) }
	if inside {
		f.importAround(connect, nil)
	} else {
		f.importAround(nil, nil)
		connect()
	}
	subscribe(f.m, c2, 1, "t")
	return f, c2, pub
}

func whenName(inside bool) string {
	if inside {
		return "inside"
	}
	return "after"
}

// TestAPublishToASubscriptionMadeAfterAnImportReachesTheClientsNextSession:
// the client subscribes on its connection, disconnects, and a QoS 1 publish
// matching only that subscription is acknowledged; its next connection
// resumes the session and receives it.
func TestAPublishToASubscriptionMadeAfterAnImportReachesTheClientsNextSession(t *testing.T) {
	for _, inside := range []bool{true, false} {
		t.Run(whenName(inside), func(t *testing.T) {
			f, c2, pub := overwrittenClient(t, inside)
			f.m.Disconnect(c2, false, packet.NormalDisconnection, false)
			code := publish(f.m, pub, "t", 1, 1, "m1")
			c3 := newFake("c", packet.V5, 100)
			present := connectAt(f.m, c3, false, 600, 7).SessionPresent
			if code != packet.Success || received(c3, "m1") != 1 {
				t.Fatalf("CONNECT %s the import: the publish was answered Success (0x%02x) and the client's next session (present %v) received %d of it, want 1", whenName(inside), byte(code), present, received(c3, "m1"))
			}
		})
	}
}

// TestASecondConnectionClosesTheFirst: the client connects again without
// Clean Start; the connection it had is closed (MQTT-3.1.4-3), and no longer
// receives its subscriptions' messages.
func TestASecondConnectionClosesTheFirst(t *testing.T) {
	for _, inside := range []bool{true, false} {
		t.Run(whenName(inside), func(t *testing.T) {
			f, c2, pub := overwrittenClient(t, inside)
			c3 := newFake("c", packet.V5, 100)
			connectAt(f.m, c3, false, 600, 7)
			publish(f.m, pub, "t", 1, 1, "m1")
			c2.mu.Lock()
			closed := len(c2.closed)
			c2.mu.Unlock()
			if closed == 0 || received(c2, "m1") != 0 {
				t.Fatalf("CONNECT %s the import: after the client's second connection the first is closed %d time(s) and received %d publish(es), want closed and 0", whenName(inside), closed, received(c2, "m1"))
			}
		})
	}
}

// TestTheReconcilerLeavesALiveClientsSubscription: two reconciler passes, then
// a QoS 1 publish matching the subscription the client holds a SUBACK for on
// its live connection reaches it. The orphan arm puts that connection's object
// out of the map directly; the reconciler then leaves it, counted and logged.
func TestTheReconcilerLeavesALiveClientsSubscription(t *testing.T) {
	t.Run("orphan", func(t *testing.T) {
		f := newReconcileFixture(t, nil)
		pub := newFake("pub", packet.V5, 100)
		connectAt(f.m, pub, true, 0, 1)
		c, _ := orphanOf(t, f.m, 600)
		f.pass()
		f.pass()
		code := publish(f.m, pub, "t", 1, 1, "m1")
		if received(c, "m1") != 1 {
			t.Fatalf("after two reconciler passes a QoS 1 publish on t was answered 0x%02x and the connected client subscribed to t received %d, want 1", byte(code), received(c, "m1"))
		}
		if n := f.refused("dispatched-unheld", "connected"); n != 1 {
			t.Fatalf("refused connected %v, want 1", n)
		}
		if w := f.log.lines("left a connected client's subscriber"); len(w) != 1 || !strings.Contains(w[0], "level=WARN") || !strings.Contains(w[0], "client=c") {
			t.Fatalf("the refusal's Warn: %q, want one Warn naming client c", w)
		}
	})
	for _, inside := range []bool{true, false} {
		t.Run(whenName(inside), func(t *testing.T) {
			f, c2, pub := overwrittenClient(t, inside)
			for i := 0; i < 2; i++ {
				if !f.m.ReconcilePass() {
					t.Fatal("the router cannot be reconciled")
				}
			}
			code := publish(f.m, pub, "t", 1, 1, "m1")
			if received(c2, "m1") != 1 {
				t.Fatalf("CONNECT %s the import: after two reconciler passes a QoS 1 publish on t was answered 0x%02x and the client's live connection, subscribed to t, received %d, want 1", whenName(inside), byte(code), received(c2, "m1"))
			}
		})
	}
}
