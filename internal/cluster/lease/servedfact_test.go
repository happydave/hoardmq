package lease

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TestServedFactIsRecordedPerNodeAndOnlyRaised: a node's served fact is kept
// on its node record, only ever raised, and never creates a record, because
// a record is what makes a node read as alive.
func TestServedFactIsRecordedPerNodeAndOnlyRaised(t *testing.T) {
	tb := NewTable()
	var told []string
	tb.onServed = func(node string) { told = append(told, node) }
	now := time.Now()
	apply := func(index uint64, ops ...Op) []Result {
		t.Helper()
		v, err := tb.Apply(index, Encode(ops))
		if err != nil {
			t.Fatal(err)
		}
		return v.([]Result)
	}
	apply(2, Op{Kind: OpAlive, Node: "n2", Now: now})

	if r := apply(3, Op{Kind: OpServed, Node: "n9", Served: 3, Routes: 70, Now: now}); r[0].OK || r[0].Reason != "no record" {
		t.Fatalf("a fact for a node with no record: %+v, want refused", r[0])
	}
	if tb.Live("n9") {
		t.Fatal("a refused fact made its node read as alive")
	}
	if i, m := tb.ServedFact("n9"); i != 0 || m != 0 {
		t.Fatalf("a refused fact was recorded: %d, %d", i, m)
	}

	if r := apply(5, Op{Kind: OpServed, Node: "n2", Served: 4, Routes: 100, Now: now}); !r[0].OK {
		t.Fatalf("n2's first fact: %+v", r[0])
	}
	if i, m := tb.ServedFact("n2"); i != 4 || m != 100 {
		t.Fatalf("n2's fact: index %d mark %d, want 4 and 100", i, m)
	}
	if r := apply(6, Op{Kind: OpServed, Node: "n2", Served: 4, Routes: 120, Now: now}); r[0].OK || r[0].Reason != "not a raise" {
		t.Fatalf("an equal fact: %+v, want refused as not a raise", r[0])
	}
	if r := apply(7, Op{Kind: OpServed, Node: "n2", Served: 3, Routes: 130, Now: now}); r[0].OK {
		t.Fatalf("a lower fact: %+v, want refused", r[0])
	}
	if i, m := tb.ServedFact("n2"); i != 4 || m != 100 {
		t.Fatalf("n2's fact after a lower one: index %d mark %d, want 4 and 100 kept", i, m)
	}
	if r := apply(9, Op{Kind: OpServed, Node: "n2", Served: 8, Routes: 140, Now: now}); !r[0].OK {
		t.Fatalf("a higher fact: %+v", r[0])
	}
	if len(told) != 2 || told[0] != "n2" || told[1] != "n2" {
		t.Fatalf("the hook was told %v, want n2 twice: once per fact recorded", told)
	}

	data, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewTable()
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	if i, m := restored.ServedFact("n2"); i != 8 || m != 140 {
		t.Fatalf("after a snapshot round trip: %d, %d, want 8 and 140", i, m)
	}
	older := NewTable()
	if err := older.Restore([]byte(`{"leases":[],"nodes":{"n2":{"alive":"2026-09-25T00:00:00Z","aliveIndex":2}}}`)); err != nil {
		t.Fatal(err)
	}
	if i, m := older.ServedFact("n2"); i != 0 || m != 0 {
		t.Fatalf("a snapshot from before the fact: %d, %d, want none", i, m)
	}

	tb.SetRecovering(true)
	apply(11, Op{Kind: OpServed, Node: "n2", Served: 10, Routes: 150, Now: now})
	tb.SetRecovering(false)
	if i, _ := tb.ServedFact("n2"); i != 10 {
		t.Fatalf("a replayed fact was not applied: %d", i)
	}
	if len(told) != 2 {
		t.Fatalf("the hook ran for a replayed fact: %v", told)
	}
}

// TestAnOlderTableLeavesAnUnknownKindAlone is what a build from before the
// served fact does with one: its apply takes the default arm. The same arm is
// taken here with a kind this build does not know either. The table must come
// out unchanged, or two builds would hold different tables from one log.
func TestAnOlderTableLeavesAnUnknownKindAlone(t *testing.T) {
	tb := NewTable()
	now := time.Now()
	if _, err := tb.Apply(2, Encode([]Op{{Kind: OpAlive, Node: "n2", Now: now}, {Kind: OpAcquire, Namespace: "default", ClientID: "x", Node: "n2", Now: now}})); err != nil {
		t.Fatal(err)
	}
	before, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	v, err := tb.Apply(3, Encode([]Op{{Kind: OpKind(99), Node: "n2", Served: 3, Routes: 70, Now: now}}))
	if err != nil {
		t.Fatal(err)
	}
	if r := v.([]Result); r[0].OK || r[0].Reason != "unknown operation" {
		t.Fatalf("an unknown kind: %+v", r[0])
	}
	after, err := tb.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("an unknown kind changed the table:\n%s\n%s", before, after)
	}
	// Non-vacuity: the snapshot does see a change a known kind makes.
	if _, err := tb.Apply(4, Encode([]Op{{Kind: OpServed, Node: "n2", Served: 3, Routes: 70, Now: now}})); err != nil {
		t.Fatal(err)
	}
	if again, _ := tb.Snapshot(); bytes.Equal(before, again) {
		t.Fatal("the snapshot did not change for a served fact; the comparison above proves nothing")
	}
}

// TestAServedFactReachesEveryNodesTable: a fact one node proposes is in every
// member's table, however that member is linked to the proposer, and each
// member's hook is told of it once.
func TestAServedFactReachesEveryNodesTable(t *testing.T) {
	var mu sync.Mutex
	told := map[string][]string{}
	fixtureOptions = func(o *Options) {
		name := o.Node
		o.Served = func(node string) {
			mu.Lock()
			told[name] = append(told[name], node)
			mu.Unlock()
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.nodes["n2"].svc.ProposeServed(ctx, 40, 7001); err != nil {
		t.Fatalf("proposing n2's fact: %v", err)
	}
	waitFor(t, "n2's fact on every node", 5*time.Second, func() bool {
		for _, n := range c.nodes {
			if i, m := n.svc.ServedFact("n2"); i != 40 || m != 7001 {
				return false
			}
		}
		return true
	})
	waitFor(t, "every node's hook told", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, name := range c.names {
			if len(told[name]) != 1 || told[name][0] != "n2" {
				return false
			}
		}
		return true
	})
}

// TestTheAdoptionSetSaysWhenItEmpties: the moment the last unfinished
// adoption ends is recorded and told, whichever of the three ways it ends,
// and not before.
func TestTheAdoptionSetSaysWhenItEmpties(t *testing.T) {
	k1, k2 := key{"default", "a"}, key{"default", "b"}
	for _, c := range []struct {
		name string
		last func(a *adoptionSet)
	}{
		{"end", func(a *adoptionSet) { ad, _ := a.claim(k2); a.end(k2, ad) }},
		{"abandon", func(a *adoptionSet) { a.abandon(k2) }},
		{"takeLateFor", func(a *adoptionSet) { a.deliver(k2, adoptJob{epoch: 5}, &store.SessionState{}); _ = a.takeLateFor(k2) }},
	} {
		a := newAdoptionSet()
		told := 0
		a.onIdle = func() { told++ }
		a.begin(k1, adoptJob{epoch: 5})
		a.begin(k2, adoptJob{epoch: 5})
		ad, _ := a.claim(k1)
		a.end(k1, ad)
		if told != 0 || a.idleAt() != 0 {
			t.Fatalf("%s: told %d, idle %d with an adoption left", c.name, told, a.idleAt())
		}
		before := time.Now().UnixNano()
		c.last(a)
		if a.unfinished() != 0 || told != 1 || a.idleAt() < before {
			t.Fatalf("%s: the last adoption ended: unfinished %d, told %d, idle at %d (before %d)", c.name, a.unfinished(), told, a.idleAt(), before)
		}
	}
}
