package records

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// TestAPutIsDecidedByTheCapItCarries: a put carrying a cap is refused full
// once the collection holds that many, a put carrying none at
// DefaultMaxRecords, and a cap outside 0 to MaxRecordsCeiling is invalid.
func TestAPutIsDecidedByTheCapItCarries(t *testing.T) {
	p := put("new", `{}`, nil)
	p.Max = 3
	if why := Decide(p, Record{}, false, 3); why != RefusedFull {
		t.Errorf("max 3 at 3 records: %q, want full", why)
	}
	if why := Decide(p, Record{}, false, 2); why != "" {
		t.Errorf("max 3 at 2 records: %q", why)
	}
	p.Max = 200000
	if why := Decide(p, Record{}, false, 150000); why != "" {
		t.Errorf("max 200,000 at 150,000 records: %q", why)
	}
	p.Max = 0
	if why := Decide(p, Record{}, false, DefaultMaxRecords); why != RefusedFull {
		t.Errorf("no max at %d records: %q, want full", DefaultMaxRecords, why)
	}
	if why := Decide(p, Record{}, false, DefaultMaxRecords-1); why != "" {
		t.Errorf("no max at %d records: %q", DefaultMaxRecords-1, why)
	}
	for _, m := range []int{-1, MaxRecordsCeiling + 1} {
		p.Max = m
		if why := Decide(p, Record{}, false, 0); why != RefusedInvalid {
			t.Errorf("max %d: %q, want invalid", m, why)
		}
	}
	p.Max = MaxRecordsCeiling
	if why := Decide(p, Record{}, false, 0); why != "" {
		t.Errorf("max at the ceiling: %q", why)
	}
}

// TestAnEntryWithoutTheCapReplaysAsBefore: a put encoded before writes
// carried a cap, with no max member at all, is refused full at 100,000
// records and taken at 99,999, whatever cap the applying node writes with.
func TestAnEntryWithoutTheCapReplaysAsBefore(t *testing.T) {
	c := New(MaxRecordsCeiling)
	for i := 0; i < DefaultMaxRecords-1; i++ {
		apply(t, c, uint64(i+1), Op{Collection: "auth.users", Key: fmt.Sprintf("u%06d", i), Value: json.RawMessage(`{}`), Max: MaxRecordsCeiling})
	}
	old := func(key string) []byte {
		return []byte(`{"c":"auth.users","k":"` + key + `","v":{},"by":"ops","at":"2026-09-25T12:00:00Z"}`)
	}
	v, err := c.Apply(DefaultMaxRecords, old("a"))
	if err != nil {
		t.Fatal(err)
	}
	if r := v.(Result); !r.OK {
		t.Fatalf("an old-form put at %d records: %+v", DefaultMaxRecords-1, r)
	}
	v, err = c.Apply(DefaultMaxRecords+1, old("b"))
	if err != nil {
		t.Fatal(err)
	}
	if r := v.(Result); r.OK || r.Reason != RefusedFull {
		t.Fatalf("an old-form put at %d records: %+v, want full", DefaultMaxRecords, r)
	}
}

// TestALowerCapRefusesNewKeysAndKeepsEveryRecord: a cap below a collection's
// size removes nothing; a new key is refused full and counted, and an
// existing key still updates.
func TestALowerCapRefusesNewKeysAndKeepsEveryRecord(t *testing.T) {
	c := New(10)
	for i := uint64(1); i <= 5; i++ {
		p := put(fmt.Sprintf("k%d", i), `{"n":1}`, nil)
		p.Max = 10
		apply(t, c, i, p)
	}
	lower := put("k6", `{}`, nil)
	lower.Max = 3
	if r := apply(t, c, 6, lower); r.OK || r.Reason != RefusedFull {
		t.Fatalf("a new key past the lower cap: %+v, want full", r)
	}
	update := put("k2", `{"n":2}`, nil)
	update.Max = 3
	if r := apply(t, c, 7, update); !r.OK {
		t.Fatalf("an existing key's update under the lower cap: %+v", r)
	}
	if n := c.Counts()["auth.users"]; n != 5 {
		t.Fatalf("%d records after lowering the cap, want 5", n)
	}
	if rec, _ := c.Get("auth.users", "k2"); string(rec.Value) != `{"n":2}` {
		t.Fatalf("k2 %s, want the update", rec.Value)
	}
	if got := c.FullRefusals()["auth.users"]; got != 1 {
		t.Fatalf("full refusals %d, want 1", got)
	}
}

// sharedLog stands for consensus: every proposal is appended in order and
// applied to every node's component at the same index.
type sharedLog struct {
	mu    sync.Mutex
	index uint64
	nodes []*Component
}

func (l *sharedLog) propose(_ context.Context, kind consensus.Kind, payload []byte) (json.RawMessage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.index++
	var first any
	for i, c := range l.nodes {
		v, err := c.Apply(l.index, payload)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			first = v
		}
	}
	return json.Marshal(first)
}

// TestNodesWithDifferentCapsHoldTheSameStore: three nodes, two of them
// proposing with caps 3 and 5. Each put is decided by its proposer's cap,
// and every node ends with the same store.
func TestNodesWithDifferentCapsHoldTheSameStore(t *testing.T) {
	a, b, c := New(3), New(5), New(5)
	log := &sharedLog{nodes: []*Component{a, b, c}}
	sa := &Store{Component: a, Propose: log.propose}
	sb := &Store{Component: b, Propose: log.propose}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if r, err := sa.Put(ctx, "auth.users", fmt.Sprintf("a%d", i), json.RawMessage(`{}`), nil, "ops", at); err != nil || !r.OK {
			t.Fatalf("a's put %d: %+v %v", i, r, err)
		}
	}
	if r, err := sa.Put(ctx, "auth.users", "a3", json.RawMessage(`{}`), nil, "ops", at); err != nil || r.Reason != RefusedFull {
		t.Fatalf("a's fourth put under its cap of 3: %+v %v, want full", r, err)
	}
	for i := 0; i < 2; i++ {
		if r, err := sb.Put(ctx, "auth.users", fmt.Sprintf("b%d", i), json.RawMessage(`{}`), nil, "ops", at); err != nil || !r.OK {
			t.Fatalf("b's put %d under its cap of 5: %+v %v", i, r, err)
		}
	}
	if r, err := sb.Put(ctx, "auth.users", "b2", json.RawMessage(`{}`), nil, "ops", at); err != nil || r.Reason != RefusedFull {
		t.Fatalf("b's put at 5 records: %+v %v, want full", r, err)
	}
	sa0, _ := a.Snapshot()
	for name, n := range map[string]*Component{"b": b, "c": c} {
		s, _ := n.Snapshot()
		if !bytes.Equal(sa0, s) {
			t.Fatalf("node %s's store differs from a's", name)
		}
	}
	if n := a.Counts()["auth.users"]; n != 5 {
		t.Fatalf("%d records, want 5", n)
	}
}

// TestAPutCarriesItsNodesCap: the store writes its component's cap into the
// operation it proposes.
func TestAPutCarriesItsNodesCap(t *testing.T) {
	var got Op
	st := &Store{Component: New(1234), Propose: func(_ context.Context, _ consensus.Kind, payload []byte) (json.RawMessage, error) {
		if err := json.Unmarshal(payload, &got); err != nil {
			return nil, err
		}
		return json.Marshal(Result{Index: 0, OK: true})
	}}
	if _, err := st.Put(context.Background(), "auth.users", "x", json.RawMessage(`{}`), nil, "ops", at); err != nil {
		t.Fatal(err)
	}
	if got.Max != 1234 {
		t.Fatalf("the proposed put carries max %d, want the node's 1234", got.Max)
	}
}
