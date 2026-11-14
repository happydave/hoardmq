package sim

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestScaleClientKeepAliveSetting: the clients' MQTT keep alive is 0 by
// default, takes 0 to 65535 seconds, and refuses anything else.
func TestScaleClientKeepAliveSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layout.json")
	data, _ := json.Marshal(goodLayout())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{scaleLayoutEnv: path, scaleClientsEnv: "100000"}
	get := func(k string) string { return env[k] }
	if c, _, err := readScaleConfig(get); err != nil || c.ClientKeepAliveSec != 0 {
		t.Fatalf("default client keep alive %d, %v", c.ClientKeepAliveSec, err)
	}
	env[scaleClientKAEnv] = "60"
	if c, _, err := readScaleConfig(get); err != nil || c.ClientKeepAliveSec != 60 {
		t.Fatalf("client keep alive 60: got %d, %v", c.ClientKeepAliveSec, err)
	}
	for _, bad := range []string{"-1", "65536", "60s", "off"} {
		env[scaleClientKAEnv] = bad
		if _, _, err := readScaleConfig(get); err == nil {
			t.Fatalf("client keep alive %q accepted", bad)
		}
	}
}

// TestDriverClientKeepAliveProblem: a run refuses a driver whose clients ask
// for another keep alive than it asked for.
func TestDriverClientKeepAliveProblem(t *testing.T) {
	if p := driverClientKeepAliveProblem(60, DriverInfo{KeepAlive: 60}); p != "" {
		t.Fatalf("matching keep alive refused: %s", p)
	}
	for _, info := range []DriverInfo{{KeepAlive: 0}, {KeepAlive: 30}} {
		if p := driverClientKeepAliveProblem(60, info); p == "" {
			t.Fatalf("a driver applying keep alive %d passed a run asking for 60", info.KeepAlive)
		}
	}
}

// TestPopulationClientsTakeClientKeepAlive: every client the fill makes
// carries the population's keep alive.
func TestPopulationClientsTakeClientKeepAlive(t *testing.T) {
	p := newLocalPopulation(3, []string{"127.0.0.1:1"}, nil, nil)
	p.keepAlive = 60
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var completed atomic.Int64
	if _, err := p.fill(ctx, &completed); err != nil {
		t.Fatal(err)
	}
	all := p.clientsSnapshot()
	for _, cl := range all {
		cl.Close()
	}
	if len(all) != 3 {
		t.Fatalf("%d clients, want 3", len(all))
	}
	for _, cl := range all {
		if cl.KeepAlive != 60 {
			t.Fatalf("client %s keep alive %d, want 60", cl.ID, cl.KeepAlive)
		}
	}
}

// TestHelloCarriesClientKeepAlive: the hello asks the driver for the
// clients' keep alive, and the driver reports the one it applies.
func TestHelloCarriesClientKeepAlive(t *testing.T) {
	c := New(t, Options{})
	d := startPipedDriver(t, c, 10, testDriverOptions())
	t.Cleanup(d.pop.close)
	d.pop.settings.KeepAlive = 60
	info, err := d.pop.hello()
	if err != nil {
		t.Fatal(err)
	}
	if info.KeepAlive != 60 {
		t.Fatalf("the driver applies keep alive %d, want 60", info.KeepAlive)
	}
}

// pingBroker accepts connections, answers each CONNECT with a CONNACK and
// each PINGREQ with a PINGRESP, and records the keep alive asked and the
// PINGREQs counted.
type pingBroker struct {
	asked atomic.Int64
	pings atomic.Int64
}

func (f *pingBroker) serve(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = nc.Close() }()
				dec := packet.NewDecoder(nc)
				enc := &packet.Encoder{Version: packet.V5}
				connect, err := dec.ReadConnect()
				if err != nil {
					return
				}
				dec.Version = packet.V5
				f.asked.Store(int64(connect.KeepAlive))
				if enc.Encode(nc, &packet.Connack{ReasonCode: packet.Success}) != nil {
					return
				}
				for {
					p, err := dec.Read()
					if err != nil {
						return
					}
					if _, ok := p.(*packet.Pingreq); ok {
						f.pings.Add(1)
						if enc.Encode(nc, &packet.Pingresp{}) != nil {
							return
						}
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

// TestClientKeepAlivePings: a client with a keep alive of 1 asks for it in
// its CONNECT and sends a PINGREQ about every second; one with 0 asks for
// none and sends none.
func TestClientKeepAlivePings(t *testing.T) {
	for _, ka := range []uint16{1, 0} {
		f := &pingBroker{}
		f.asked.Store(-1)
		cl := NewClient("ka", []string{f.serve(t)}, 0, 1)
		cl.KeepAlive = ka
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := cl.Run(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2500 * time.Millisecond) // two keep alives of 1 s and a half
		cl.Close()
		if got := f.asked.Load(); got != int64(ka) {
			t.Errorf("keep alive %d: the CONNECT asked for %d", ka, got)
		}
		n := f.pings.Load()
		if ka == 1 && n < 2 {
			t.Errorf("keep alive 1: %d PINGREQs in 2.5 s, want at least 2", n)
		}
		if ka == 0 && n != 0 {
			t.Errorf("keep alive 0: %d PINGREQs, want none", n)
		}
	}
}
