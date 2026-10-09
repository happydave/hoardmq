package routing

import (
	"context"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/dispatch"
	"github.com/happydave/hoardmq/internal/message"
)

// benchPlanes starts one plane per name on loopback transports, joined, with
// sinks that hold every delivery durable at once.
func benchPlanes(b *testing.B, names []string) map[string]*Plane {
	b.Helper()
	planes := map[string]*Plane{}
	trs := map[string]*transport.Transport{}
	for _, name := range names {
		var ps []config.Peer
		for _, o := range names {
			if o != name {
				ps = append(ps, config.Peer{Name: o, Address: "127.0.0.1:1"})
			}
		}
		tr, err := transport.New(transport.Options{Node: name, ClusterID: "t", Listen: "127.0.0.1:0", Peers: ps, Auth: transport.Auth{Mode: "secret", Secret: []byte(secret)},
			Heartbeat: 50 * time.Millisecond, HeartbeatMisses: 4, HandshakeTimeout: 2 * time.Second, Logger: quiet})
		if err != nil {
			b.Fatal(err)
		}
		p := New(Options{Node: name, Transport: tr, Strategy: dispatch.RoundRobin, SyncInterval: 300 * time.Millisecond, Logger: quiet})
		p.SetSink(&fakeSink{})
		if err := tr.Start(); err != nil {
			b.Fatal(err)
		}
		p.Start()
		b.Cleanup(func() {
			p.Stop()
			tr.Stop()
		})
		planes[name], trs[name] = p, tr
	}
	for _, name := range names {
		var ps []config.Peer
		for _, o := range names {
			if o != name {
				ps = append(ps, config.Peer{Name: o, Address: trs[o].Addr().String()})
			}
		}
		trs[name].SetPeers(ps)
	}
	benchWait(b, "all peers up", func() bool {
		for _, tr := range trs {
			for _, pi := range tr.Peers() {
				if pi.State != transport.Up {
					return false
				}
			}
		}
		return true
	})
	return planes
}

func benchWait(b *testing.B, what string, cond func() bool) {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Fatalf("timed out waiting for %s", what)
}

// BenchmarkGroupGiveOnward is a QoS 1 shared-group publish on a whose group's
// dispatch owner b alternates between its own member and cc's, timed to the
// publisher's barrier. An op is two publishes: one delivered at b, one given
// on to cc.
func BenchmarkGroupGiveOnward(b *testing.B) {
	planes := benchPlanes(b, []string{"a", "b", "cc"})
	a := planes["a"]
	planes["b"].Observe("default", "$share/g/t/#", 2)
	planes["cc"].Observe("default", "$share/g/t/#", 1)
	benchWait(b, "group routes", func() bool {
		_, groups := a.Table().Match("default", "t/1")
		return len(groups) == 1 && len(groups[0].Members) == 2
	})
	seq := uint64(0)
	once := func() {
		seq++
		m := &message.Message{Namespace: "default", Topic: "t/1", Payload: []byte("x"), QoS: 1, Publisher: message.Publisher{Namespace: "default", ClientID: "p", Node: "a"}, Seq: seq, Arrived: time.Now()}
		_, _ = a.Forward(m)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := a.Barrier(ctx, m.Forwards)
		cancel()
		if err != nil {
			b.Fatal(err)
		}
	}
	once()
	once()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		once()
		once()
	}
}
