package broker

import (
	"testing"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// willTrio starts three nodes with the admin API, a watcher on n1 subscribed
// to will/#, and on n1 one client per id, each with a Will on will/<id> and a
// session expiry, once n2's table names n1 as each one's owner.
func willTrio(t *testing.T, ids ...string) (map[string]*fixture, *client, map[string]*client) {
	t.Helper()
	dir := t.TempDir()
	nodes := startTrio(t, func(_ string, d *config.Document) {
		withAdmin(t, dir)(d)
		d.Cluster.Shards = 8
		d.Cluster.Readiness.StableMs = 0
	})
	waitMap(t, nodes, 3)
	watcher, _ := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: "watcher", CleanStart: true}, false)
	watcher.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "will/#", QoS: 0}}})
	if _, ok := watcher.read().(*packet.Suback); !ok {
		t.Fatal("no SUBACK for the watcher")
	}
	clients := map[string]*client{}
	exp := uint32(600)
	for _, id := range ids {
		c, ack := nodes["n1"].connect("tcp", packet.V5, &packet.Connect{ClientID: id, CleanStart: true,
			Will:       &packet.Will{Topic: "will/" + id, Payload: []byte(id)},
			Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s: %s", id, ack.ReasonCode)
		}
		clients[id] = c
		adminWait(t, "n2 to see "+id+"'s lease on n1", func() bool {
			l, ok := nodes["n2"].srv.Leases().Table().Lookup("default", id)
			return ok && l.Owner == "n1"
		})
	}
	return nodes, watcher, clients
}

// TestAdminDisconnectWithoutTheWill: called on n2 for clients held on n1, a
// disconnect with will=false closes the client and publishes no Will, and one
// without the parameter publishes it; both are forwarded, and the owner's
// audit record notes will=false.
func TestAdminDisconnectWithoutTheWill(t *testing.T) {
	nodes, watcher, clients := willTrio(t, "dropped", "kept")
	n1, n2 := nodes["n1"], nodes["n2"]
	for _, c := range []struct{ id, query string }{{"dropped", "?will=false"}, {"kept", ""}} {
		code, data, hdr := apiH(t, n2, "POST", "/api/v1/clients/"+c.id+"/disconnect"+c.query, adminOps, "")
		if code != 200 || hdr.Get(admin.NodeHeader) != "n1" {
			t.Fatalf("n2 disconnect %s%s: %d %q %s", c.id, c.query, code, hdr.Get(admin.NodeHeader), data)
		}
		if d, ok := clients[c.id].read().(*packet.Disconnect); !ok || d.ReasonCode != packet.AdministrativeAction {
			t.Fatalf("%s did not read DISCONNECT 0x98", c.id)
		}
	}
	if pub := watcher.readPublish(); pub.Topic != "will/kept" {
		t.Fatalf("the first Will published is on %s, want will/kept: the dropped client's Will went out", pub.Topic)
	}
	watcher.expectNoPacket()
	// Offline now, the client's disconnect is refused as before, whatever will says.
	if code, data := api(t, n2, "POST", "/api/v1/clients/dropped/disconnect?will=false", adminOps, ""); code != 409 {
		t.Fatalf("disconnect of the offline client with will=false: %d %s, want 409", code, data)
	}
	noted := false
	for _, e := range adminAudit(t, n1, "client.disconnect", "dropped") {
		noted = noted || (e.Detail["will"] == "false" && e.Detail["forwardedFrom"] == "n2")
	}
	if !noted {
		t.Fatalf("n1's audit of the forwarded disconnect does not note will=false: %+v", adminAudit(t, n1, "client.disconnect", "dropped"))
	}
}

// TestAdminInvalidateWithoutTheWill: an invalidation with will=false, called
// on n2, ends the session on n1 and publishes no Will; one without the
// parameter publishes it.
func TestAdminInvalidateWithoutTheWill(t *testing.T) {
	nodes, watcher, _ := willTrio(t, "dropped", "kept")
	for _, c := range []struct {
		id, query string
		published bool
	}{{"dropped", "?will=false", false}, {"kept", "", true}} {
		code, data := api(t, nodes["n2"], "DELETE", "/api/v1/clients/"+c.id+"/session"+c.query, adminOps, "")
		if code != 200 {
			t.Fatalf("n2 invalidate %s%s: %d %s", c.id, c.query, code, data)
		}
		if got := decode[admin.Invalidated](t, data); got.WillPublished != c.published {
			t.Fatalf("invalidate %s%s answered %+v, want willPublished %v", c.id, c.query, got, c.published)
		}
	}
	if pub := watcher.readPublish(); pub.Topic != "will/kept" {
		t.Fatalf("the first Will published is on %s, want will/kept", pub.Topic)
	}
	watcher.expectNoPacket()
}
