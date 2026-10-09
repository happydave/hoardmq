package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/config"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestRedirectDueAt: the client at position i of n is due at the earlier of
// i ÷ rate and i ÷ n of the budget after the redirect begins, to the
// nanosecond, the first at once; a rate below 1 reads as 1, and with no budget
// left every client is due at once.
func TestRedirectDueAt(t *testing.T) {
	for _, tc := range []struct {
		name         string
		i, n, rate   int
		budget, want time.Duration
	}{
		{"the first", 0, 10, 100, time.Second, 0},
		{"the rate binds", 1, 50, 100, 1500 * time.Millisecond, 10 * time.Millisecond},
		{"the deadline binds", 1, 600, 100, 1500 * time.Millisecond, 2500 * time.Microsecond},
		{"the last, before the deadline", 599, 600, 100, 1500 * time.Millisecond, 1497500 * time.Microsecond},
		{"a fast rate", 1, 2500, 2500, time.Hour, 400 * time.Microsecond},
		{"a fast rate's last", 2499, 2500, 2500, time.Hour, 999600 * time.Microsecond},
		{"a rate below 1", 3, 10, 0, time.Hour, 3 * time.Second},
		{"no budget left", 5, 10, 100, 0, 0},
		{"the deadline passed", 5, 10, 100, -time.Second, 0},
	} {
		if got := redirectDueAt(tc.i, tc.n, tc.rate, tc.budget); got != tc.want {
			t.Errorf("%s: redirectDueAt(%d, %d, %d, %v) = %v, want %v", tc.name, tc.i, tc.n, tc.rate, tc.budget, got, tc.want)
		}
	}
}

// TestRedirectBudget: a drain finishes redirecting by its timeout less the
// takeover timeout, or less half its timeout when that is the smaller.
func TestRedirectBudget(t *testing.T) {
	for _, tc := range []struct {
		timeout, takeover, want time.Duration
	}{
		{30 * time.Second, 5 * time.Second, 25 * time.Second},
		{3 * time.Second, 5 * time.Second, 1500 * time.Millisecond},
		{8 * time.Second, 4 * time.Second, 4 * time.Second},
		{10 * time.Second, 100 * time.Millisecond, 9900 * time.Millisecond},
	} {
		if got := redirectBudget(tc.timeout, tc.takeover); got != tc.want {
			t.Errorf("redirectBudget(%v, %v) = %v, want %v", tc.timeout, tc.takeover, got, tc.want)
		}
	}
}

// TestDrainRedirectsAtItsRate: a drain of n clients at r a second spreads
// their disconnects over about (n − 1) ÷ r. A sleep after each client would
// spread them over about a millisecond each. The timeout leaves the
// deadline's pace, 750 a second, below the rate.
func TestDrainRedirectsAtItsRate(t *testing.T) {
	const n, rate = 1500, 2500
	spread := drainSpread(t, n, rate, 4000)
	ideal := time.Duration(n-1) * time.Second / rate
	lo, hi := ideal*9/10, ideal*3/2
	t.Logf("%d disconnects over %v at %d a second (ideal %v)", n, spread, rate, ideal)
	if spread < lo || spread > hi {
		t.Fatalf("%d disconnects at %d a second spread over %v, want %v–%v", n, rate, spread, lo, hi)
	}
}

// TestDrainRedirectsByItsDeadline: a drain with more clients than its rate
// moves in its budget (a 3 s timeout less half of it, the takeover timeout
// being longer) redirects them over the budget; one with fewer keeps to its
// rate. The bounds separate the rate alone (6 s for 600), a deadline at the
// whole timeout (3 s) and a deadline with no floor (1.5 s for 50).
func TestDrainRedirectsByItsDeadline(t *testing.T) {
	const rate, timeoutMs = 100, 3000
	const budget = 1500 * time.Millisecond // 3 s less half of it
	for _, tc := range []struct {
		name  string
		n     int
		ideal time.Duration
	}{
		{"the deadline binds", 600, budget * 599 / 600},
		{"the rate binds", 50, 49 * time.Second / rate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spread := drainSpread(t, tc.n, rate, timeoutMs)
			lo, hi := tc.ideal*9/10, tc.ideal*5/4
			t.Logf("%d disconnects over %v (ideal %v)", tc.n, spread, tc.ideal)
			if spread < lo || spread > hi {
				t.Fatalf("%d disconnects at %d a second with a %d ms timeout spread over %v, want %v–%v", tc.n, rate, timeoutMs, spread, lo, hi)
			}
		})
	}
}

// drainSpread drains a node with no live peer holding n MQTT 5 clients, at
// rate a second with timeoutMs, and returns the time from the first client's
// DISCONNECT to the last's, failing unless every client got Server Busy.
func drainSpread(t *testing.T, n, rate, timeoutMs int) time.Duration {
	t.Helper()
	f := start(t, func(d *config.Document) {
		d.Cluster.Drain.RatePerSecond = rate
		d.Cluster.Drain.TimeoutMs = timeoutMs
		d.Cluster.Leases.TakeoverTimeoutMs = 5000
	})
	clients := make([]*client, n)
	for i := range clients {
		c, ack := f.connect("tcp", packet.V5, &packet.Connect{ClientID: fmt.Sprintf("rate-%04d", i), CleanStart: true}, false)
		if ack.ReasonCode != packet.Success {
			t.Fatalf("connect %d: %s", i, ack.ReasonCode)
		}
		clients[i] = c
	}
	type arrival struct {
		at     time.Time
		reason packet.ReasonCode
		err    error
	}
	got := make([]arrival, n)
	var wg sync.WaitGroup
	for i, c := range clients {
		_ = c.c.SetReadDeadline(time.Now().Add(20 * time.Second))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				p, err := c.dec.Read()
				if err != nil {
					got[i] = arrival{err: err}
					return
				}
				if d, ok := p.(*packet.Disconnect); ok {
					got[i] = arrival{at: time.Now(), reason: d.ReasonCode}
					return
				}
			}
		}()
	}
	f.srv.Drain()
	wg.Wait()
	var first, last time.Time
	for i, a := range got {
		if a.err != nil || a.reason != packet.ServerBusy {
			t.Fatalf("client %d: disconnect %s, err %v; want Server Busy for every client", i, a.reason, a.err)
		}
		if first.IsZero() || a.at.Before(first) {
			first = a.at
		}
		if a.at.After(last) {
			last = a.at
		}
	}
	return last.Sub(first)
}
