package conn

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/packet"
)

// The client identifier and username a CONNECT carries reach authz.file's
// templates; a value containing a wildcard or a level separator makes the rule
// unusable, so it grants nothing beyond the client's own topics.
func TestACLTemplateValuesFromConnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acl.json")
	if err := os.WriteFile(path, []byte(`{"rules":[
		{"topic": "devices/${clientid}/#", "allow": ["publish", "subscribe", "unsubscribe"]},
		{"topic": "users/${username}/#", "allow": ["publish", "subscribe", "unsubscribe"]}
	]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	acl, err := auth.LoadACLFile(path)
	if err != nil {
		t.Fatal(err)
	}
	opts := defaultOptions()
	opts.Anonymous = false
	opts.Authenticators = auth.AuthenticatorChain{fixedAuth{d: auth.Allow}}
	opts.Authorizers = auth.AuthorizerChain{acl}

	cases := []struct {
		name, clientID, username string
		live                     string // a topic the other template grants, showing the rules apply
		refused                  []string
	}{
		{"client id", "+", "alice", "users/alice/x", []string{"devices/other/x", "devices/+/#"}},
		{"username", "c1", "+", "devices/c1/x", []string{"users/other/x", "users/+/#"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, opts, &fakeCore{})
			ack := h.connect(packet.V5, &packet.Connect{ClientID: c.clientID, CleanStart: true, HasUsername: true, Username: c.username})
			if ack.ReasonCode != packet.Success {
				t.Fatalf("connack %+v", ack)
			}
			h.send(&packet.Publish{Topic: c.live, QoS: 1, PacketID: 1})
			if a := h.read().(*packet.Puback); a.ReasonCode != packet.Success {
				t.Fatalf("publish %q: %+v, want success", c.live, a)
			}
			h.send(&packet.Publish{Topic: c.refused[0], QoS: 1, PacketID: 2})
			if a := h.read().(*packet.Puback); a.ReasonCode != packet.NotAuthorized {
				t.Errorf("publish %q: %+v, want not authorized", c.refused[0], a)
			}
			h.send(&packet.Subscribe{PacketID: 3, Subscriptions: []packet.Subscription{{Filter: c.refused[1], QoS: 1}}})
			if s := h.read().(*packet.Suback); s.ReasonCodes[0] != packet.NotAuthorized {
				t.Errorf("subscribe %q: %+v, want not authorized", c.refused[1], s)
			}
			h.send(&packet.Unsubscribe{PacketID: 4, Filters: []string{c.refused[1]}})
			if u := h.read().(*packet.Unsuback); u.ReasonCodes[0] != packet.NotAuthorized {
				t.Errorf("unsubscribe %q: %+v, want not authorized", c.refused[1], u)
			}
		})
	}
}
