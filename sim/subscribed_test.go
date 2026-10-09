package sim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

func refusalEvent(client, node string, code packet.ReasonCode) Event {
	return Event{Client: client, Node: node, Kind: "suback-refused", Code: code, At: time.Now()}
}

// TestSubscribedViolations covers the decision. The clean case comes first so
// a checker that flagged everything would fail before it could satisfy the
// refusal cases.
func TestSubscribedViolations(t *testing.T) {
	// Granted subscriptions, no refusals: nothing to report.
	if v := subscribedViolations(3, nil); len(v) != 0 {
		t.Errorf("a clean run: %v, want no violation", v)
	}

	// One refusal, with everything a reader needs to find it. The time is
	// fixed rather than time.Now() so the assertion below can name it: the
	// acceptance criteria ask for four fields in the violation and a test that
	// checks three of them would report the fourth as covered.
	at := time.Date(2026, 9, 15, 22, 41, 58, 123456789, time.UTC)
	refusals := subscribeRefusals([]Event{
		{Client: "sub-1", Kind: "connack", Code: packet.Success},
		{Client: "sub-2", Node: "n2", Kind: "suback-refused", Code: packet.ImplementationSpecificError, At: at},
	})
	if len(refusals) != 1 {
		t.Fatalf("read %d refusals from the events, want 1", len(refusals))
	}
	v := subscribedViolations(5, refusals)
	if len(v) != 1 {
		t.Fatalf("%d violations, want 1", len(v))
	}
	if v[0].Property != PropSubscribed {
		t.Errorf("property %q, want %q", v[0].Property, PropSubscribed)
	}
	for _, want := range []string{"sub-2", "n2", packet.ImplementationSpecificError.String(), at.Format(time.RFC3339Nano)} {
		if !strings.Contains(v[0].Detail, want) {
			t.Errorf("the violation does not name %q: %s", want, v[0].Detail)
		}
	}
}

// TestSubscribedIsNotVacuous is the case this property exists to avoid being.
// A run that subscribed to nothing has no refusals either, and reporting that
// as clean is how a check comes to be trusted without ever having run.
func TestSubscribedIsNotVacuous(t *testing.T) {
	v := subscribedViolations(0, nil)
	if len(v) != 1 {
		t.Fatalf("a run that subscribed to nothing: %d violations, want 1", len(v))
	}
	if !strings.Contains(v[0].Detail, "subscribed to nothing") {
		t.Errorf("the message does not say the run subscribed to nothing: %s", v[0].Detail)
	}

	// And the neighbouring case, which leaves the granted list empty for a
	// completely different reason: everything was refused. A message that
	// could not tell these apart would send a reader to the wrong place.
	v = subscribedViolations(0, subscribeRefusals([]Event{
		refusalEvent("sub-1", "n1", packet.ImplementationSpecificError),
		refusalEvent("sub-2", "n1", packet.ImplementationSpecificError),
	}))
	if len(v) != 3 {
		t.Fatalf("two refusals and nothing granted: %d violations, want 3 (two refusals and the summary)", len(v))
	}
	last := v[len(v)-1].Detail
	if !strings.Contains(last, "none of them was subscribed") {
		t.Errorf("the summary does not distinguish 'everything refused' from 'nothing subscribed': %s", last)
	}
	if strings.Contains(last, "subscribed to nothing") {
		t.Errorf("the two empty-granted cases report the same message: %s", last)
	}
}

func TestRefusalsByClient(t *testing.T) {
	// One client refused many times and many clients refused once are
	// different failures; the total alone cannot tell them apart.
	many := subscribeRefusals([]Event{
		refusalEvent("sub-1", "n1", packet.ImplementationSpecificError),
		refusalEvent("sub-1", "n1", packet.ImplementationSpecificError),
		refusalEvent("sub-1", "n1", packet.ImplementationSpecificError),
	})
	total, clients := refusalsByClient(many)
	if total != 3 || clients != 1 {
		t.Errorf("one client refused three times: total %d clients %d, want 3 and 1", total, clients)
	}

	spread := subscribeRefusals([]Event{
		refusalEvent("sub-1", "n1", packet.ImplementationSpecificError),
		refusalEvent("sub-2", "n2", packet.ImplementationSpecificError),
		refusalEvent("sub-3", "n3", packet.ImplementationSpecificError),
	})
	total, clients = refusalsByClient(spread)
	if total != 3 || clients != 3 {
		t.Errorf("three clients refused once: total %d clients %d, want 3 and 3", total, clients)
	}

	if total, clients := refusalsByClient(nil); total != 0 || clients != 0 {
		t.Errorf("no refusals: total %d clients %d, want 0 and 0", total, clients)
	}
}

// TestSubscribeRefusalsReachTheBatchSummary is the artifact half of WI 1509.
// The property fires inside a corpus run; a failover batch has no corpus, and
// its run.json is what the published results are read from. So the counts have
// to survive the same path the leadership fields take: run.json → readRun →
// summarise → the line an operator reads.
func TestSubscribeRefusalsReachTheBatchSummary(t *testing.T) {
	base := t.TempDir()
	inner := filepath.Join(base, "TestFailover-1000-120000")
	if err := os.MkdirAll(inner, 0o750); err != nil {
		t.Fatal(err)
	}
	art := map[string]any{
		"fill_time": "22.68s", "recovery": "1m39.804s", "recovery_sec": 99.804,
		"online_at_end": 100000, "sub_refusals": 412, "sub_refused_clients": 37,
	}
	raw, err := json.Marshal(art)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "run.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeComplete(inner, 1000, "1m39.804s"); err != nil {
		t.Fatal(err)
	}

	r := readRun(1, base)
	if r.Failed {
		t.Fatalf("the artifact did not read: %s", r.Why)
	}
	if r.SubRefusals != 412 || r.SubRefusedClients != 37 {
		t.Errorf("run.json's refusals did not reach the run: %d across %d clients, want 412 across 37",
			r.SubRefusals, r.SubRefusedClients)
	}

	// A batch of two, only one of which saw refusals: the total must not be
	// mistakable for "every run had some".
	quiet := repeatRun{Index: 2, RecoverySec: 31.2}
	s := summarise([]repeatRun{r, quiet})
	if s.SubRefusals != 412 {
		t.Errorf("batch total %d, want 412", s.SubRefusals)
	}
	if s.SubRefusedRuns != 1 {
		t.Errorf("runs that saw a refusal: %d, want 1", s.SubRefusedRuns)
	}
	if !strings.Contains(s.String(), "412 subscribe refusals in 1 runs") {
		t.Errorf("the operator's line does not carry the refusals: %s", s.String())
	}

	// And a batch with none says nothing, so the line stays readable on the
	// healthy case that every published batch so far has been.
	if line := summarise([]repeatRun{quiet}).String(); strings.Contains(line, "subscribe refusal") {
		t.Errorf("a clean batch mentioned refusals: %s", line)
	}
}

// TestSubRefusedClientsCountsTheWindowNotTheRun is the bug this work item's
// own code review found: the first version counted a client whose lifetime
// refusals were non-zero, so a client refused while the population was filling
// was reported as a client the storm refused. Every other counter in the
// summary is windowed by subtracting one total at the mark; this one cannot
// be, because "how many clients" is not a sum.
func TestSubRefusedClientsCountsTheWindowNotTheRun(t *testing.T) {
	before := &Client{ID: "refused-while-filling"}
	during := &Client{ID: "refused-by-the-storm"}
	quiet := &Client{ID: "never-refused"}
	p := &localPopulation{clients: 3, all: []*Client{before, during, quiet}}

	before.SubRefusals.Store(4) // happened before the window opened
	if err := p.mark(); err != nil {
		t.Fatal(err)
	}
	before.SubRefusals.Store(4) // unchanged since the mark: not the storm's
	during.SubRefusals.Store(2)

	sum, err := p.finish()
	if err != nil {
		t.Fatal(err)
	}
	if sum.SubRefusals != 2 {
		t.Errorf("refusals in the window: %d, want 2", sum.SubRefusals)
	}
	if sum.SubRefusedClients != 1 {
		t.Errorf("clients refused in the window: %d, want 1 — the fill's client is not the storm's",
			sum.SubRefusedClients)
	}

	// And a client that appeared after the mark has no entry at all; its whole
	// count belongs to the window rather than being silently dropped.
	late := &Client{ID: "joined-late"}
	late.SubRefusals.Store(1)
	p.all = append(p.all, late)
	sum, err = p.finish()
	if err != nil {
		t.Fatal(err)
	}
	if sum.SubRefusedClients != 2 {
		t.Errorf("a client that appeared after the mark was not counted: %d, want 2", sum.SubRefusedClients)
	}
}

// TestSubscribeRefusalsCrossTheDriverSocket covers the hop that would fail
// silently. In the remote-client mode the summary is computed in a driver
// process on another host and arrives as JSON, so a field added to popSummary
// but not to driverSummary — or to driverSummary but not to the conversion —
// reads zero on precisely the runs the published sweeps are made of, and a
// zero is what a healthy run reports.
func TestSubscribeRefusalsCrossTheDriverSocket(t *testing.T) {
	sent := driverSummary{
		ReconnectsNs: []int64{int64(time.Second)}, DroppedClients: 7,
		Refusals: 11, Drops: 13, Fails: 17, SubRefusals: 412, SubRefusedClients: 37,
	}
	raw, err := json.Marshal(driverMsg{Type: msgSummary, Summary: &sent})
	if err != nil {
		t.Fatal(err)
	}
	var got driverMsg
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Summary == nil {
		t.Fatal("the summary did not survive encoding")
	}
	if got.Summary.SubRefusals != 412 || got.Summary.SubRefusedClients != 37 {
		t.Errorf("the wire lost the refusals: %d across %d clients",
			got.Summary.SubRefusals, got.Summary.SubRefusedClients)
	}

	out := summaryFromDriver(*got.Summary)
	if out.SubRefusals != 412 || out.SubRefusedClients != 37 {
		t.Errorf("the conversion lost the refusals: %d across %d clients, want 412 across 37",
			out.SubRefusals, out.SubRefusedClients)
	}
	// The figures beside them, so a rewrite of the conversion that drops one
	// of the older fields is caught here too.
	if out.DroppedClients != 7 || out.Refusals != 11 || out.Drops != 13 || out.Fails != 17 {
		t.Errorf("the conversion lost a figure: %+v", out)
	}
	if len(out.Reconnects) != 1 || out.Reconnects[0] != time.Second {
		t.Errorf("reconnects: %v", out.Reconnects)
	}
}
