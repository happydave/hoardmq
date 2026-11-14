package sim

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestReconnectAttemptsCountFromDropToRecovery: a client's attempts are the
// connects from its first drop to its first accepted CONNACK, that one
// included. Connects before the drop and after the recovery are not the
// storm's.
func TestReconnectAttemptsCountFromDropToRecovery(t *testing.T) {
	c := NewClient("f000001", []string{"127.0.0.1:1"}, 3600, 1)
	at := time.Now()
	ev := func(kind string, code packet.ReasonCode) {
		at = at.Add(time.Millisecond)
		c.record(Event{Client: c.ID, Kind: kind, Code: code, At: at})
	}
	ev("failed", 0) // before any drop: the fill's, not the storm's
	ev("connack", packet.Success)
	if _, ok := c.ReconnectAttempts(); ok {
		t.Fatal("attempts reported before any drop")
	}
	ev("dropped", 0)
	ev("failed", 0)
	ev("connack", packet.ServerUnavailable)
	ev("connack", packet.UseAnotherServer)
	ev("failed", 0)
	if _, ok := c.ReconnectAttempts(); ok {
		t.Fatal("attempts reported before the client was accepted again")
	}
	ev("connack", packet.Success)
	ev("dropped", 0) // a second drop after recovery is not the first's
	ev("failed", 0)
	ev("connack", packet.Success)
	n, ok := c.ReconnectAttempts()
	if !ok || n != 5 {
		t.Fatalf("attempts %d (reported %v), want 5: two failed dials, two refusals, the accepted connect", n, ok)
	}
	c.TakeEvents()
	if _, ok := c.ReconnectAttempts(); ok {
		t.Fatal("attempts survived TakeEvents")
	}
}

// TestReconnectAttemptsCrossTheDriverSocket: in the cross-host run the
// summary is computed in the driver process, so the counts must survive the
// wire conversion.
func TestReconnectAttemptsCrossTheDriverSocket(t *testing.T) {
	got := summaryFromDriver(driverSummary{ReconnectsNs: []int64{1, 2, 3}, ReconnectAttempts: []int{1, 4, 9}})
	if len(got.Attempts) != 3 || got.Attempts[0] != 1 || got.Attempts[2] != 9 {
		t.Fatalf("the conversion lost the attempts: %v", got.Attempts)
	}
}

// TestAttemptStats: the percentiles follow the failover report's rule.
func TestAttemptStats(t *testing.T) {
	if attemptStats(nil) != nil {
		t.Fatal("figures for no reconnecting client")
	}
	sorted := make([]int, 100)
	for i := range sorted {
		sorted[i] = i + 1
	}
	a := attemptStats(sorted)
	if a.Clients != 100 || a.Median != 51 || a.P90 != 91 || a.P99 != 100 || a.Max != 100 || a.Mean != 50.5 {
		t.Fatalf("figures %+v", *a)
	}
}
