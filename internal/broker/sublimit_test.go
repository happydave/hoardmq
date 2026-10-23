package broker

import (
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// limits.maxSubscriptions on the wire: a filter past the limit is refused
// with 0x97 at MQTT 5.0 and 0x80 at 3.1.1, the connection stays open, the
// refused filter delivers nothing, the refusals reach the scrape, and a reload
// that raises the limit to 2 lets the filter in and refuses a third.
func TestSubscriptionLimitOnTheWire(t *testing.T) {
	n := startReloadable(t, func(d *config.Document) {
		metricsOn(d)
		d.Limits.MaxSubscriptions = 1
	})
	subscribe := func(cl *client, id uint16, filters ...string) []packet.ReasonCode {
		t.Helper()
		s := &packet.Subscribe{PacketID: id}
		for _, f := range filters {
			s.Subscriptions = append(s.Subscriptions, packet.Subscription{Filter: f, QoS: 1})
		}
		cl.send(s)
		ack, ok := cl.read().(*packet.Suback)
		if !ok || ack.PacketID != id {
			t.Fatalf("SUBACK %d: %+v", id, ack)
		}
		return ack.ReasonCodes
	}
	v5, _ := n.connect("tcp", packet.V5, &packet.Connect{ClientID: "five", CleanStart: true, KeepAlive: 30}, false)
	if got := subscribe(v5, 1, "s/a", "s/b"); !slices.Equal(got, []packet.ReasonCode{1, packet.QuotaExceeded}) {
		t.Fatalf("5.0 SUBACK %v, want [1 0x97]", got)
	}
	v4, _ := n.connect("tcp", packet.V4, &packet.Connect{ClientID: "four", CleanStart: true, KeepAlive: 30}, false)
	if got := subscribe(v4, 1, "s/a", "s/b"); !slices.Equal(got, []packet.ReasonCode{1, 0x80}) {
		t.Fatalf("3.1.1 SUBACK %v, want [1 0x80]", got)
	}
	v5.send(&packet.Pingreq{})
	if p := v5.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("the 5.0 connection after the refusal: %T %+v", p, p)
	}

	pub, _ := n.connect("tcp", packet.V5, &packet.Connect{ClientID: "pub", CleanStart: true}, false)
	publishQoS1(t, pub, "s/b", 1)
	publishQoS1(t, pub, "s/a", 2)
	if p, ok := v5.read().(*packet.Publish); !ok || p.Topic != "s/a" {
		t.Fatalf("the 5.0 subscriber's first delivery: %+v, want s/a alone", p)
	}

	_, body := scrape(t, n.fixture, "/metrics")
	if v, ok := metricValue(t, body, `hoardmq_subscription_limit_refusals_total{namespace="default"}`); !ok || v != 2 {
		t.Fatalf("refusals on the scrape %v %v, want 2", v, ok)
	}

	n.rewrite(t, func(d *config.Document) { d.Limits.MaxSubscriptions = 2 })
	if r := n.srv.Reload(); !r.Applied {
		t.Fatalf("reload: %+v", r)
	}
	if got := subscribe(v5, 2, "s/b", "s/c"); !slices.Equal(got, []packet.ReasonCode{1, packet.QuotaExceeded}) {
		t.Fatalf("after the reload to 2: %v, want [1 0x97]", got)
	}
}

// A connection a listener closes at its maxConnections is counted on the
// scrape.
func TestMaxConnectionsRefusalsReachTheScrape(t *testing.T) {
	f := start(t, func(d *config.Document) {
		metricsOn(d)
		d.Listeners[0].MaxConnections = 1
	})
	held, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: "held", CleanStart: true, KeepAlive: 30}, false)
	if ack.ReasonCode != packet.Success {
		t.Fatalf("precondition: connack %v", ack.ReasonCode)
	}
	extra, err := net.Dial("tcp", f.srv.Addr("tcp").String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = extra.Close() }()
	_ = extra.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := extra.Read(make([]byte, 1)); err == nil {
		t.Fatal("the connection past the bound was not closed")
	} else if ne := net.Error(nil); errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("the connection past the bound was left open")
	}
	_, body := scrape(t, f, "/metrics")
	if v, ok := metricValue(t, body, `hoardmq_max_connections_refused_total{listener="tcp"}`); !ok || v != 1 {
		t.Fatalf("refusals on the scrape %v %v, want 1", v, ok)
	}
	held.send(&packet.Pingreq{})
	if p := held.read(); p.Type() != packet.PINGRESP {
		t.Fatalf("the held connection: %T %+v", p, p)
	}
}
