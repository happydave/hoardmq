package sim

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

func probesAt(healed time.Time, steps ...any) []Probe {
	// steps alternate an offset and an outcome: a packet.ReasonCode or an error.
	var out []Probe
	for i := 0; i+1 < len(steps); i += 2 {
		p := Probe{At: healed.Add(steps[i].(time.Duration)), Node: "n1"}
		switch v := steps[i+1].(type) {
		case packet.ReasonCode:
			p.Code = v
		case error:
			p.Err = v
		}
		out = append(out, p)
	}
	return out
}

// TestRecoveryOf covers the reading of one node's probe sequence: the
// duration ends at the first accepted probe, and a node that never accepts
// has a lower bound instead of a duration.
func TestRecoveryOf(t *testing.T) {
	healed := time.Unix(1_000, 0)
	refused := packet.ImplementationSpecificError

	at := recoveryOf("n1", healed, probesAt(healed, 40*time.Millisecond, packet.NoMatchingSubscribers), healed.Add(40*time.Millisecond))
	if !at.Recovered || at.Wedged() || at.Refused != 0 || at.AfterMs != 40 {
		t.Errorf("accepted at once: %+v", at)
	}

	// Refused twice, accepted at 600 ms, and probed again afterwards: the
	// duration is the first acceptance, not the last probe.
	seq := probesAt(healed,
		100*time.Millisecond, refused,
		350*time.Millisecond, refused,
		600*time.Millisecond, packet.NoMatchingSubscribers,
		900*time.Millisecond, packet.NoMatchingSubscribers)
	got := recoveryOf("n1", healed, seq, healed.Add(900*time.Millisecond))
	if !got.Recovered || !got.Wedged() || got.Refused != 2 || got.AfterMs != 600 {
		t.Errorf("recovered after two refusals: %+v, want refused=2 after_ms=600", got)
	}
	if want := map[string]int{refused.String(): 2}; !reflect.DeepEqual(got.ByCode, want) {
		t.Errorf("by code %v, want %v", got.ByCode, want)
	}

	// Never accepted, one probe unanswered: not recovered, and the time is how
	// long it was watched.
	never := probesAt(healed,
		100*time.Millisecond, refused,
		350*time.Millisecond, errors.New("connection refused"),
		5*time.Second, refused)
	got = recoveryOf("n1", healed, never, healed.Add(5*time.Second))
	if got.Recovered || got.Refused != 3 || got.AfterMs != 5000 {
		t.Errorf("never recovered: %+v, want recovered=false refused=3 after_ms=5000", got)
	}
	if got.ByCode["no answer"] != 1 || got.ByCode[refused.String()] != 2 || got.LastErr != "connection refused" {
		t.Errorf("an unanswered probe was not kept apart: %+v", got)
	}
}

// TestBarrierDelta: the record carries what failed since the heal, so a count
// from the fault window must not reach it.
func TestBarrierDelta(t *testing.T) {
	before := map[string]int{"replication:n2/refused": 108, "forward:send-failed": 4}
	after := map[string]int{"replication:n2/refused": 131, "forward:send-failed": 4, "replication:n3/down": 2}
	want := map[string]int{"replication:n2/refused": 23, "replication:n3/down": 2}
	if got := barrierDelta(before, after); !reflect.DeepEqual(got, want) {
		t.Errorf("delta %v, want %v", got, want)
	}
	if got := barrierDelta(before, before); got != nil {
		t.Errorf("nothing moved, yet the delta is %v", got)
	}
}

// TestRecoveryStats: a node that never recovered is counted and never becomes
// the longest duration, and a run where nothing refused reports no wedge.
func TestRecoveryStats(t *testing.T) {
	rec := []NodeRecovery{
		{Node: "n1", Recovered: true, AfterMs: 12, Probes: 1},
		{Node: "n2", Recovered: true, AfterMs: 2600, Probes: 9, Refused: 8},
		{Node: "n3", Recovered: false, AfterMs: 8000, Probes: 30, Refused: 30},
	}
	st := recoveryStats(rec)
	want := map[string]int{"recovery.watched": 3, "recovery.wedgedNodes": 2, "recovery.notRecovered": 1, "recovery.longestMs": 2600, "recovery.refusedProbes": 38}
	if !reflect.DeepEqual(st, want) {
		t.Errorf("stats %v, want %v", st, want)
	}
	if got := describeRecovery(rec); !strings.Contains(got, "n3 refused 30 probes and had NOT recovered") || strings.Contains(got, "n1 ") {
		t.Errorf("description %q", got)
	}

	quiet := []NodeRecovery{{Node: "n1", Recovered: true, AfterMs: 9, Probes: 1}}
	if st := recoveryStats(quiet); st["recovery.wedgedNodes"] != 0 || st["recovery.longestMs"] != 0 {
		t.Errorf("a run where nothing refused reported a wedge: %v", st)
	}
	if got := describeRecovery(quiet); got != "recovery: no node refused a publish after the heal" {
		t.Errorf("a quiet run described as %q", got)
	}
	if got := describeRecovery(nil); !strings.Contains(got, "not watched") {
		t.Errorf("an unwatched run described as %q", got)
	}
	if len(recoveryStats(nil)) != 0 {
		t.Error("an unwatched run contributed stats")
	}
}

// TestLivenessViolationCarriesTheRecovery: the violation names how long and
// how much, for the node it names and no other.
func TestLivenessViolationCarriesTheRecovery(t *testing.T) {
	rec := []NodeRecovery{
		{Node: "n1", Recovered: true, AfterMs: 10, Probes: 1},
		{Node: "n2", Recovered: false, AfterMs: 8000, Probes: 31, Refused: 31, Barrier: map[string]int{"forward:send-failed": 31}},
	}
	v := livenessFindingsWith([]Probe{{Node: "n2", Code: packet.ImplementationSpecificError}}, nil, rec)
	if len(v) != 1 || !strings.Contains(v[0].Detail, "n2 refused 31 probes and had NOT recovered") || !strings.Contains(v[0].Detail, "forward:send-failed=31") {
		t.Fatalf("violation %v", v)
	}
}

func outRecords(t *testing.T, n *Node) int {
	t.Helper()
	snap, ok := n.Server().Registry().Metrics().(interface{ Snapshot() []module.Metric })
	if !ok {
		t.Fatal("the registry's metrics cannot be read")
	}
	total := 0
	for _, m := range snap.Snapshot() {
		if m.Name == "hoardmq_replication_records_total" && m.Labels["direction"] == "out" {
			total += int(m.Value)
		}
	}
	return total
}

// TestProbeWritesNoReplicationRecord is the probe tested as a suspect. A wedged
// stream's mark is escaped by new records on it, so a probe that wrote one
// would shorten the wedge it is timing. The counter is first shown to move for
// a publish that does replicate, or a dead counter would pass this.
func TestProbeWritesNoReplicationRecord(t *testing.T) {
	c := New(t, Options{})
	n := c.Nodes[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sub := NewClient("wedge-sub", []string{n.TCP}, 300, 1)
	defer sub.Close()
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(ctx, "wedge/t", 1); err != nil {
		t.Fatal(err)
	}
	pub := NewClient("wedge-pub", []string{n.TCP}, 300, 2)
	defer pub.Close()
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	before := outRecords(t, n)
	start := time.Now()
	if _, err := pub.Publish(ctx, "wedge/t", 1, 1); err != nil {
		t.Fatalf("%v (%s)", err, refusalDiagnosis(c, time.Since(start)))
	}
	if moved := outRecords(t, n) - before; moved == 0 {
		t.Fatal("a QoS 1 publish into a durable session wrote no outbound replication record, so this counter cannot show what a probe writes")
	}

	before = outRecords(t, n)
	for i := 0; i < 20; i++ {
		if code, err := ProbePublish(n.TCP, "recovery-"+n.Name, "liveness/recovery"); err != nil || code.IsError() {
			t.Fatalf("probe %d against a healthy node: %s %v", i, code, err)
		}
	}
	if wrote := outRecords(t, n) - before; wrote != 0 {
		t.Errorf("20 probes wrote %d outbound replication records: probing perturbs the stream it times", wrote)
	}
}
