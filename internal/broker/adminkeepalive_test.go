package broker

import (
	"testing"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestAdminConnectionKeepAliveAndCleanStart: a client's connection record
// carries the keep alive in force, the one its CONNECT asked for, and its
// Clean Start. With limits.maxKeepAlive 20 a client asking 15 keeps 15, one
// asking 60 or 0 is held to 20, and a resumed session's connection reads
// Clean Start false.
func TestAdminConnectionKeepAliveAndCleanStart(t *testing.T) {
	f := start(t, func(d *config.Document) {
		withAdmin(t, t.TempDir())(d)
		d.Limits.MaxKeepAlive = 20
	})
	exp := uint32(600)
	first, _ := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "resumed", CleanStart: true, KeepAlive: 10, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
	first.send(&packet.Disconnect{ReasonCode: packet.NormalDisconnection})
	_ = first.c.Close()
	waitOffline(t, f, "resumed")
	for _, c := range []struct {
		id             string
		ask            uint16
		clean          bool
		inForce, asked int
	}{
		{"own", 15, true, 15, 15},
		{"capped", 60, true, 20, 60},
		{"none", 0, true, 20, 0},
		{"resumed", 10, false, 10, 10},
	} {
		cl, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: c.id, CleanStart: c.clean, KeepAlive: c.ask, Properties: packet.Properties{SessionExpiryInterval: &exp}}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %s: %s", c.id, ack.ReasonCode)
		}
		t.Cleanup(func() { _ = cl.c.Close() })
		code, data := api(t, f, "GET", "/api/v1/clients/"+c.id, adminReader, "")
		if code != 200 {
			t.Fatalf("inspect %s: %d %s", c.id, code, data)
		}
		conn := decode[admin.ClientDetail](t, data).Connection
		if conn == nil || conn.KeepAlive != c.inForce || conn.RequestedKeepAlive != c.asked || conn.CleanStart != c.clean {
			t.Fatalf("%s's connection record %+v, want keepAlive %d, requestedKeepAlive %d, cleanStart %v", c.id, conn, c.inForce, c.asked, c.clean)
		}
	}
}
