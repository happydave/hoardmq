package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/packet"
)

// TestServedEmptyTotalsSumEverySurvivor: the artifact's figure is every
// survivor's count by reason, and a survivor with no lease stats adds nothing.
func TestServedEmptyTotalsSumEverySurvivor(t *testing.T) {
	stats := map[string]broker.StormStats{
		"n2": {Lease: &lease.LivenessStats{ServedEmpty: map[string]int64{lease.EmptyGrantUnfetched: 3, lease.EmptyTakeover: 1}}},
		"n3": {Lease: &lease.LivenessStats{ServedEmpty: map[string]int64{lease.EmptyGrantUnfetched: 2}}},
		"n4": {},
	}
	got := servedEmptyTotals(stats)
	if len(got) != 2 || got[lease.EmptyGrantUnfetched] != 5 || got[lease.EmptyTakeover] != 1 {
		t.Fatalf("totals %v; want grant-unfetched 5, takeover 1", got)
	}
	if line := servedEmptyLine(got); line != "6 (grant-unfetched 5, takeover 1)" {
		t.Fatalf("report line %q", line)
	}
	if line := servedEmptyLine(map[string]int64{}); line != "0" {
		t.Fatalf("report line for none %q", line)
	}
}

// TestSessionLostSampleNamesClientsUpToItsCap: each client told Session
// Present false since the mark is named once, and no more than the cap are.
func TestSessionLostSampleNamesClientsUpToItsCap(t *testing.T) {
	p := &localPopulation{sessionLostAt: map[*Client]int64{}, subRefusedAt: map[*Client]int64{}}
	for i := 0; i < 2*sessionLostSampleCap+10; i++ {
		cl := &Client{ID: fmt.Sprint("c", i)}
		if i%2 == 0 {
			cl.SessionLost.Store(1)
		}
		p.all = append(p.all, cl)
	}
	before := &Client{ID: "before"}
	before.SessionLost.Store(1)
	p.sessionLostAt[before] = 1 // lost before the mark only
	p.sessionLostBefore = 1
	p.all = append(p.all, before)
	sum, err := p.finish()
	if err != nil {
		t.Fatal(err)
	}
	want := (2*sessionLostSampleCap + 10) / 2
	if sum.SessionLostClients != want || len(sum.SessionLostSample) != min(want, sessionLostSampleCap) {
		t.Fatalf("clients %d, sample %d; want %d and %d", sum.SessionLostClients, len(sum.SessionLostSample), want, min(want, sessionLostSampleCap))
	}
	for _, id := range sum.SessionLostSample {
		if id == "before" {
			t.Fatal("a client lost before the mark is named")
		}
	}
}

// TestServedEmptyViolations: zero passes; a count names its node and reasons.
func TestServedEmptyViolations(t *testing.T) {
	if v := servedEmptyViolations(map[string]map[string]int{"n1": {}, "n2": {lease.EmptyTakeover: 0}}); len(v) != 0 {
		t.Fatalf("zero counts reported %v", v)
	}
	v := servedEmptyViolations(map[string]map[string]int{"n1": {}, "n2": {lease.EmptyTakeover: 1, lease.EmptyGrantUnfetched: 2}})
	if len(v) != 1 || v[0].Property != PropNoServedEmpty || v[0].Detail != "n2 served 3 sessions empty although their leases showed one existed (by reason: grant-unfetched=2, takeover=1)" {
		t.Fatalf("violations %v", v)
	}
	// A session with no expiry ended with its connection: no violation alone,
	// and not added to one.
	if v := servedEmptyViolations(map[string]map[string]int{"n1": {lease.EmptyNoExpiry: 2}}); len(v) != 0 {
		t.Fatalf("no-expiry counts reported %v", v)
	}
	v = servedEmptyViolations(map[string]map[string]int{"n1": {lease.EmptyNoExpiry: 2, lease.EmptyRaced: 1}})
	if len(v) != 1 || v[0].Detail != "n1 served 1 sessions empty although their leases showed one existed (by reason: no-expiry=2, raced=1)" {
		t.Fatalf("violations %v", v)
	}
	if noExpiryReason != lease.EmptyNoExpiry {
		t.Fatalf("the harness reads %q, the lease service counts %q", noExpiryReason, lease.EmptyNoExpiry)
	}
}

// TestSessionWithNoCopyLeftIsCountedServedEmptyOnce is the property's
// control: with one replica a session lives only on its owner, so crashing
// the owner removes every copy. The client reconnects, is told Session
// Present false, and the corpus's reader finds exactly one served-empty
// session, which the property reports.
func TestSessionWithNoCopyLeftIsCountedServedEmptyOnce(t *testing.T) {
	c := New(t, Options{Replicas: 1, Log: halfLog(t, "servedempty")})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	leader := c.Node(leaderName)
	var victim *Node
	for _, n := range c.Nodes {
		if n != leader {
			victim = n
			break
		}
	}
	setup := NewClient("empty-sub", []string{victim.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, "empty/t", 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "the subscriber recorded offline on its node", 5*time.Second, func() bool {
		rec, ok := victim.Server().Sessions().Record("empty-sub")
		return ok && !rec.Online
	})
	c.Crash(victim)
	waitUntil(t, "the leader adopting the subscriber's lease", 30*time.Second, func() bool {
		srv := leader.Server()
		if srv == nil || srv.Leases() == nil {
			return false
		}
		l, ok := srv.Leases().Table().Lookup("default", "empty-sub")
		return ok && l.Owner == leader.Name
	})
	sub := NewClient("empty-sub", []string{leader.TCP}, 600, 2)
	if err := sub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	var present, seen bool
	for _, e := range sub.TakeEvents() {
		if e.Kind == "connack" && e.Code == packet.Success && !seen {
			present, seen = e.Present, true
		}
	}
	if !seen || present {
		t.Fatalf("the reconnect: accepted %v, Session Present %v; want accepted without its session", seen, present)
	}
	counts := servedEmptyByNode(c)
	total := 0
	for _, byReason := range counts {
		for _, n := range byReason {
			total += n
		}
	}
	if total != 1 {
		t.Fatalf("served empty read %v across the running nodes; want exactly 1", counts)
	}
	if v := servedEmptyViolations(counts); len(v) != 1 {
		t.Fatalf("the property reported %v; want one violation", v)
	}
}

// TestExemptionCoversOnlyItsSeedAndProperty: an exempted seed's served-empty
// violation is set aside and reported as fired; the same violation on another
// seed, and another property on the exempted seed, are judged.
func TestExemptionCoversOnlyItsSeedAndProperty(t *testing.T) {
	exemptions := map[int64]map[string]string{4: {PropNoServedEmpty: "WI 0000: an example"}}
	served := Violation{Property: PropNoServedEmpty, Detail: "n2 served 1"}
	other := Violation{Property: PropNoAckedLoss, Detail: "lost"}
	judged, fired := exempt(exemptions, 4, []Violation{served, other})
	if len(fired) != 1 || fired[0] != served || len(judged) != 1 || judged[0] != other {
		t.Fatalf("seed 4: judged %v, fired %v", judged, fired)
	}
	judged, fired = exempt(exemptions, 5, []Violation{served})
	if len(fired) != 0 || len(judged) != 1 {
		t.Fatalf("seed 5: judged %v, fired %v", judged, fired)
	}
}

// TestCorpusExemptionsNameTheirWorkItem: an exemption nobody owns is load-bearing
// scope nobody is tracking, so each one names the work item that removes it.
func TestCorpusExemptionsNameTheirWorkItem(t *testing.T) {
	if bad := unownedExemptions(corpusExemptions); len(bad) != 0 {
		t.Fatalf("exemptions naming no work item: %v", bad)
	}
	if bad := unownedExemptions(map[int64]map[string]string{3: {PropNoAckedLoss: "flaky"}}); len(bad) != 1 {
		t.Fatalf("an exemption with no work item was not reported: %v", bad)
	}
}
