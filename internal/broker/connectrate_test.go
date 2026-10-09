package broker

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/auth"
	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// connectOnce connects over plain TCP, reads the CONNACK and disconnects,
// returning the CONNACK's reason; safe off the test's goroutine.
func connectOnce(addr, id, user, pw string) (packet.ReasonCode, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = nc.Close() }()
	_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
	enc := &packet.Encoder{Version: packet.V5}
	c := &packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true}
	if user != "" {
		c.HasUsername, c.Username, c.HasPassword, c.Password = true, user, true, []byte(pw)
	}
	if err := enc.Encode(nc, c); err != nil {
		return 0, err
	}
	dec := packet.NewDecoder(nc)
	dec.Version = packet.V5
	p, err := dec.Read()
	if err != nil {
		return 0, err
	}
	ack, ok := p.(*packet.Connack)
	if !ok {
		return 0, fmt.Errorf("a %T where a CONNACK was expected", p)
	}
	if ack.ReasonCode == packet.Success {
		_ = enc.Encode(nc, &packet.Disconnect{})
	}
	return ack.ReasonCode, nil
}

// usersFile writes n users, user-<i> with password pw-<i>, in the SCRAM form
// at its iteration floor so the test derives cheaply, and returns its path.
func usersFile(t *testing.T, n int) string {
	t.Helper()
	type user struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	var doc struct {
		Users []user `json:"users"`
	}
	for i := 0; i < n; i++ {
		phc, err := auth.HashSCRAM([]byte(fmt.Sprintf("pw-%d", i)), auth.MinSCRAMIterations)
		if err != nil {
			t.Fatal(err)
		}
		doc.Users = append(doc.Users, user{Username: fmt.Sprintf("user-%d", i), Password: phc})
	}
	b, _ := json.Marshal(doc)
	path := filepath.Join(t.TempDir(), "users.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// storm connects every user six times, each user's connections in sequence
// and the users at once, and returns how many CONNACKs said Success.
func storm(t *testing.T, f *fixture, users, rounds int) int {
	t.Helper()
	addr := f.srv.Addr("tcp").String()
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	var firstErr error
	for i := 0; i < users; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				code, err := connectOnce(addr, fmt.Sprintf("c-%d", i), fmt.Sprintf("user-%d", i), fmt.Sprintf("pw-%d", i))
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				if err == nil && code == packet.Success {
					ok++
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	return ok
}

// A reconnect storm on one listener derives each client's key once: 40
// clients connecting six times each is 240 CONNECTs and 40 derivations. With
// the cache off, every CONNECT derives.
func TestReconnectStormDerivesOncePerClient(t *testing.T) {
	const users, rounds = 40, 6
	for _, c := range []struct {
		name        string
		ttl         int
		derivations int64
		hits        int64
	}{{"cache on", 60, users, users * (rounds - 1)}, {"cache off", 0, users * rounds, 0}} {
		t.Run(c.name, func(t *testing.T) {
			path := usersFile(t, users)
			f := start(t, func(d *config.Document) {
				d.Auth.Anonymous = false
				d.Auth.Cache.TTLSeconds = c.ttl
				d.Modules["auth.file"] = json.RawMessage(`{"path":"` + path + `"}`)
			})
			if got := storm(t, f, users, rounds); got != users*rounds {
				t.Fatalf("%d of %d CONNECTs accepted", got, users*rounds)
			}
			if s := f.srv.passwords.Stats(); s.Derivations != c.derivations || s.Hits != c.hits {
				t.Fatalf("%d CONNECTs: %d derivations and %d hits, want %d and %d", users*rounds, s.Derivations, s.Hits, c.derivations, c.hits)
			}
		})
	}
}

// The scrape carries the cache's derivations, hits and entries.
func TestPasswordCacheMetrics(t *testing.T) {
	path := usersFile(t, 1)
	f := start(t, func(d *config.Document) {
		d.Auth.Anonymous = false
		d.Modules["auth.file"] = json.RawMessage(`{"path":"` + path + `"}`)
	})
	addr := f.srv.Addr("tcp").String()
	for i := 0; i < 2; i++ {
		if code, err := connectOnce(addr, "c", "user-0", "pw-0"); err != nil || code != packet.Success {
			t.Fatalf("connect %d: %v %v", i, code, err)
		}
	}
	for name, want := range map[string]float64{"hoardmq_auth_password_derivations_total": 1, "hoardmq_auth_password_cache_hits_total": 1, "hoardmq_auth_password_cache_entries": 1} {
		if got := metric(f, name, nil); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// sendBackToBack connects n times over the named listeners in turn and
// returns the reasons and how long the first to the last took.
func sendBackToBack(t *testing.T, f *fixture, names []string) ([]packet.ReasonCode, time.Duration) {
	t.Helper()
	var codes []packet.ReasonCode
	began := time.Now()
	for i, name := range names {
		_, ack := f.connect(name, packet.V5, &packet.Connect{ClientID: fmt.Sprintf("r-%d", i), CleanStart: true}, false)
		codes = append(codes, ack.ReasonCode)
	}
	return codes, time.Since(began)
}

// requireWithin fails as a precondition when the CONNECTs were not back to
// back: a token refills in a second, and the refusal under test needs them
// inside it.
func requireWithin(t *testing.T, span time.Duration) {
	t.Helper()
	if span > 500*time.Millisecond {
		t.Fatalf("precondition: the CONNECTs took %v, over the 500 ms they must fit in for a refill not to intervene", span)
	}
}

// A listener's rate refuses its own CONNECTs past the burst and leaves the
// other listeners alone; the node's rate refuses across listeners.
func TestConnectRateLimits(t *testing.T) {
	f := start(t, func(d *config.Document) {
		d.Listeners[0].ConnectRate, d.Listeners[0].ConnectBurst = 1, 2 // tcp
		d.Listeners[3].ConnectRate = 1                                 // ws: burst 0, so twice the rate
	})
	codes, span := sendBackToBack(t, f, []string{"tcp", "tcp", "tcp"})
	requireWithin(t, span)
	if codes[0] != packet.Success || codes[1] != packet.Success || codes[2] != packet.ConnectionRateExceeded {
		t.Fatalf("three CONNECTs to a listener of rate 1, burst 2: %v", codes)
	}
	var tls []string
	for i := 0; i < 10; i++ {
		tls = append(tls, "tls")
	}
	codes, _ = sendBackToBack(t, f, tls)
	for _, c := range codes {
		if c != packet.Success {
			t.Fatalf("an unlimited listener beside it: %v", codes)
		}
	}
	codes, span = sendBackToBack(t, f, []string{"ws", "ws", "ws"})
	requireWithin(t, span)
	if codes[0] != packet.Success || codes[1] != packet.Success || codes[2] != packet.ConnectionRateExceeded {
		t.Fatalf("three CONNECTs to a listener of rate 1 and no burst set: %v, want two admitted", codes)
	}
	if got := f.srv.rateGate().Refused(); len(got) != 2 || got["tcp/listener"] != 1 || got["ws/listener"] != 1 {
		t.Fatalf("refusals %v, want tcp/listener 1 and ws/listener 1", got)
	}

	n := start(t, func(d *config.Document) {
		d.Limits.ConnectRate, d.Limits.ConnectBurst = 1, 3
	})
	codes, span = sendBackToBack(t, n, []string{"tcp", "tls", "tcp", "tls"})
	requireWithin(t, span)
	if codes[0] != packet.Success || codes[1] != packet.Success || codes[2] != packet.Success || codes[3] != packet.ConnectionRateExceeded {
		t.Fatalf("four CONNECTs to a node of rate 1, burst 3: %v", codes)
	}
	if got := metric(n, "hoardmq_connect_rate_refused_total", map[string]string{"listener": "tls", "limit": "node"}); got != 1 {
		t.Fatalf("hoardmq_connect_rate_refused_total{listener=tls,limit=node} = %v, want 1", got)
	}
}

// With no rate set anywhere there is no gate, and a connection asks nothing.
func TestNoRateNoGate(t *testing.T) {
	d := config.Default()
	d.Listeners = []config.Listener{{Name: "a", Type: "tcp", Address: "127.0.0.1:0"}}
	if g := newConnectGate(d); g != nil {
		t.Fatal("a gate with every rate unlimited")
	}
	d.Listeners[0].ConnectRate = 1
	if g := newConnectGate(d); g == nil {
		t.Fatal("no gate with a listener's rate set")
	}
}
