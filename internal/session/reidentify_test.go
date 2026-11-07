package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// onlyWithAttribute allows a publish only to an identity carrying the
// attribute "may" as "yes", and anything else to anyone.
type onlyWithAttribute struct{}

func (onlyWithAttribute) Authorize(_ context.Context, action auth.Action, topic string, id *auth.Identity) auth.Decision {
	if action == auth.Publish && topic == "w" && id.Attributes["may"] != "yes" {
		return auth.Deny
	}
	return auth.Allow
}

// TestReidentifyDecidesTheWill: the Will of a session handed an identity
// without the attribute that allowed it is refused; one handed an identity
// that keeps it is published; a connection the session is no longer attached
// to changes nothing.
func TestReidentifyDecidesTheWill(t *testing.T) {
	for _, c := range []struct {
		name   string
		handed map[string]string
		stale  bool
		want   int
	}{
		{"stripped", nil, false, 0},
		{"kept", map[string]string{"may": "yes"}, false, 1},
		{"handed to a connection taken over", nil, true, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, _ := newManager(t, func(o *Options) { o.Authorizers = auth.AuthorizerChain{onlyWithAttribute{}} })
			watcher := newFake("watcher", packet.V5, 100)
			connectV5(m, watcher, true, 0, nil)
			subscribe(m, watcher, 1, "w")
			conn := newFake("c", packet.V5, 100)
			conn.identity.Attributes = map[string]string{"may": "yes"}
			connectV5(m, conn, false, 3600, &packet.Will{Topic: "w", Payload: []byte("gone"), QoS: 1})
			target := conn
			if c.stale {
				target = newFake("c", packet.V5, 100)
			}
			m.Reidentify(target, auth.Identity{Namespace: "default", ClientID: "c", Attributes: c.handed})
			m.Disconnect(conn, false, packet.AdministrativeAction, true)
			if got := len(watcher.publishes()); got != c.want {
				t.Fatalf("the Will was published %d times, want %d", got, c.want)
			}
		})
	}
}

// TestReidentifySetsTheQueueBound: the handed identity's queue limit bounds
// the session while it is offline, though the identity it connected with set
// none.
func TestReidentifySetsTheQueueBound(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.MaxQueued = 10 })
	pub := newFake("pub", packet.V5, 1000)
	connectV5(m, pub, true, 0, nil)
	sub := newFake("q", packet.V5, 1000)
	connectV5(m, sub, false, 600, nil)
	subscribe(m, sub, 1, "q/q")
	m.Reidentify(sub, auth.Identity{Namespace: "default", ClientID: "q", Attributes: map[string]string{QueueLimitAttribute: "2"}})
	m.Disconnect(sub, false, packet.AdministrativeAction, false)
	for i := 0; i < 5; i++ {
		publish(m, pub, "q/q", 1, uint16(i+1), fmt.Sprint(i))
	}
	back := newFake("q", packet.V5, 1000)
	connectV5(m, back, false, 600, nil)
	if got := len(back.publishes()); got != 2 {
		t.Fatalf("%d sent on resume, want the handed bound of 2", got)
	}
}
