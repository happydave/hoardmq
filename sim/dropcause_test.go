package sim

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestDropCauseMapping is the pure half: a reason code to a bucket, testable
// without a connection. The codes that matter to WI 1495 are named
// individually because the whole question is whether an excess drop was a
// takeover detaching a live client.
func TestDropCauseMapping(t *testing.T) {
	for _, tc := range []struct {
		code packet.ReasonCode
		want int
		name string
	}{
		{0, dropCauseNone, "none"},
		{packet.SessionTakenOver, dropCauseTakenOver, "session-taken-over"},
		{packet.ServerShuttingDown, dropCauseShuttingDown, "server-shutting-down"},
		{packet.KeepAliveTimeout, dropCauseKeepAlive, "keep-alive-timeout"},
		{packet.UseAnotherServer, dropCauseUseAnother, "use-another-server"},
		{packet.NotAuthorized, dropCauseOther, "other"},
	} {
		if got := dropCause(tc.code); got != tc.want {
			t.Errorf("dropCause(%v) = %d, want %d", tc.code, got, tc.want)
		}
		if dropCauseNames[tc.want] != tc.name {
			t.Errorf("bucket %d is named %q, want %q", tc.want, dropCauseNames[tc.want], tc.name)
		}
	}
	// Every bucket has a name: an unnamed bucket would reach the artifact as
	// an empty map key and read as a cause that does not exist.
	for i, n := range dropCauseNames {
		if n == "" {
			t.Errorf("bucket %d has no name", i)
		}
	}
}

// TestDropCausesAreWindowedAndSumToDrops is the accounting claim the plan
// makes: the buckets cover the window, not the run, and they add up to the
// same Drops figure the artifact already reports. A histogram that does not
// sum to the count it explains is not an explanation.
func TestDropCausesAreWindowedAndSumToDrops(t *testing.T) {
	fill := &Client{ID: "dropped-while-filling"}
	storm := &Client{ID: "dropped-by-the-storm"}
	p := &localPopulation{clients: 2, all: []*Client{fill, storm}}

	// Before the mark: one transport loss, outside the measurement.
	fill.record(Event{Kind: "dropped", Code: 0, At: time.Now()})
	if err := p.mark(); err != nil {
		t.Fatal(err)
	}
	// Inside the window: two takeovers and one keep-alive timeout.
	storm.record(Event{Kind: "dropped", Code: packet.SessionTakenOver, At: time.Now()})
	storm.record(Event{Kind: "dropped", Code: packet.SessionTakenOver, At: time.Now()})
	fill.record(Event{Kind: "dropped", Code: packet.KeepAliveTimeout, At: time.Now()})

	sum, err := p.finish()
	if err != nil {
		t.Fatal(err)
	}
	if got := sum.DropCauses[dropCauseTakenOver]; got != 2 {
		t.Errorf("session-taken-over in the window: %d, want 2", got)
	}
	if got := sum.DropCauses[dropCauseKeepAlive]; got != 1 {
		t.Errorf("keep-alive-timeout in the window: %d, want 1", got)
	}
	if got := sum.DropCauses[dropCauseNone]; got != 0 {
		t.Errorf("the fill's transport loss leaked into the window: %d, want 0", got)
	}
	var total int64
	for _, n := range sum.DropCauses {
		total += n
	}
	if total != sum.Drops {
		t.Errorf("the causes sum to %d but Drops is %d — a histogram that does not sum to its count", total, sum.Drops)
	}
	if sum.Drops != 3 {
		t.Errorf("Drops in the window: %d, want 3", sum.Drops)
	}

	m := dropCauseMap(sum.DropCauses)
	if m["session-taken-over"] != 2 || m["keep-alive-timeout"] != 1 {
		t.Errorf("the artifact map lost a bucket: %v", m)
	}
	// Zero buckets are kept, so a cause that did not occur is a measurement
	// rather than an absent field (the WI 1509 lesson).
	if _, ok := m["server-shutting-down"]; !ok {
		t.Error("a cause that did not occur was dropped from the map")
	}
}

// TestDropCausesCrossTheDriverSocket covers the hop that would fail silently,
// as WI 1509's did: in remote mode the summary is computed on another host.
func TestDropCausesCrossTheDriverSocket(t *testing.T) {
	var sent driverSummary
	causes := [dropCauseCount]int64{}
	causes[dropCauseTakenOver] = 412
	causes[dropCauseNone] = 7
	sent.DropCauses = causes[:]
	sent.Drops = 419

	got := summaryFromDriver(sent)
	if got.DropCauses[dropCauseTakenOver] != 412 || got.DropCauses[dropCauseNone] != 7 {
		t.Errorf("the conversion lost the causes: %v", got.DropCauses)
	}

	// A driver from an older build sends fewer buckets; that must fill what it
	// has rather than panic, because the two sides are separate processes and
	// can be separate builds.
	short := driverSummary{DropCauses: []int64{5}}
	if out := summaryFromDriver(short); out.DropCauses[dropCauseNone] != 5 {
		t.Errorf("a short bucket list did not fill what it had: %v", out.DropCauses)
	}
	// And a longer one must not write past the array.
	long := driverSummary{DropCauses: make([]int64, dropCauseCount+4)}
	long.DropCauses[dropCauseCount+3] = 1
	_ = summaryFromDriver(long)
}
