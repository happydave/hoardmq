package lease

import (
	"testing"
	"time"
)

// TestResultCarriesItsIndex: every result of a command names the log index
// the command applied at.
func TestResultCarriesItsIndex(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for _, r := range applyAt(t, tb, 41, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpAcquire, Namespace: "d", ClientID: "c", Node: "n1", Now: now}) {
		if r.Index != 41 {
			t.Fatalf("result %+v, want index 41", r)
		}
	}
}

// TestRenewalRecordsItsContact: a node's successful renewal records the index
// its alive record applied at, its own table holds that record, and an
// older renewal succeeding late moves nothing.
func TestRenewalRecordsItsContact(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	n2 := c.nodes["n2"]
	var index uint64
	var at time.Time
	deadline := time.Now().Add(10 * time.Second)
	for index == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		index, at = n2.svc.Contact()
	}
	if index == 0 {
		t.Fatal("no contact recorded within 10 s")
	}
	if n2.svc.Steady().Sub(at) > 2*time.Second {
		t.Fatalf("contact at %v, more than a ttl ago", at)
	}
	waitFor(t, "n2's table to apply its contact", 5*time.Second, func() bool { return n2.svc.Table().AppliedIndex() >= index })
	info, _ := n2.svc.Table().Node("n2")
	if info.AliveIndex < index {
		t.Fatalf("n2's alive record at %d in its table, below its contact %d", info.AliveIndex, index)
	}
	n2.svc.noteContact(index - 1)
	if got, gotAt := n2.svc.Contact(); got < index || gotAt.Before(at) {
		t.Fatalf("an older renewal moved the contact to %d at %v", got, gotAt)
	}
}
