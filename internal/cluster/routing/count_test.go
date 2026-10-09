package routing

import (
	"fmt"
	"math/rand"
	"testing"
)

// walkCount is the count Table.Count gave before it was kept incrementally.
func walkCount(t *Table) (local, remote int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for k, e := range t.entries {
		if e.Tombstone {
			continue
		}
		if k.owner == t.self {
			local++
		} else {
			remote++
		}
	}
	return local, remote
}

// TestCountAgreesWithTheWalk is WI 1481: after every Merge, MergeFull and
// Forget in seeded random sequences, the kept counts equal a full walk.
func TestCountAgreesWithTheWalk(t *testing.T) {
	owners := []string{"a", "b", "c"}
	for seed := int64(1); seed <= 8; seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // seeded test
		tb := NewTable("a")
		entry := func() Entry {
			return Entry{Namespace: "d", Filter: fmt.Sprintf("t/%d", rng.Intn(40)), Owner: owners[rng.Intn(len(owners))],
				Version: uint64(rng.Intn(12)), Tombstone: rng.Intn(3) == 0, Count: 1}
		}
		ops := map[string]int{}
		for i := 0; i < 3000; i++ {
			var op string
			switch r := rng.Intn(10); {
			case r < 7:
				op = "merge"
				in := make([]Entry, 1+rng.Intn(4))
				for j := range in {
					in[j] = entry()
				}
				tb.Merge(in)
			case r < 9:
				op = "mergefull"
				owner := owners[rng.Intn(len(owners))]
				in := make([]Entry, rng.Intn(6))
				for j := range in {
					in[j] = entry()
					in[j].Owner = owner
				}
				tb.MergeFull(owner, in, uint64(rng.Intn(14)))
			default:
				op = "forget"
				tb.Forget(owners[rng.Intn(len(owners))])
			}
			ops[op]++
			gl, gr := tb.Count()
			wl, wr := walkCount(tb)
			if gl != wl || gr != wr {
				t.Fatalf("seed %d op %d (%s): Count = %d local, %d remote; the walk = %d, %d", seed, i, op, gl, gr, wl, wr)
			}
		}
		// The sequence must have exercised every path, or agreement proves
		// little; TestCountSeesBothScopes pins non-zero counts.
		if ops["merge"] == 0 || ops["mergefull"] == 0 || ops["forget"] == 0 {
			t.Fatalf("seed %d did not exercise every operation: %v", seed, ops)
		}
	}
}

// TestCountSeesBothScopes pins the counts on a hand-built sequence, so the
// property test's agreement is not agreement at zero.
func TestCountSeesBothScopes(t *testing.T) {
	tb := NewTable("a")
	tb.Merge([]Entry{
		{Namespace: "d", Filter: "t/1", Owner: "a", Version: 1, Count: 1},
		{Namespace: "d", Filter: "t/2", Owner: "a", Version: 1, Count: 1},
		{Namespace: "d", Filter: "t/1", Owner: "b", Version: 1, Count: 1},
	})
	if l, r := tb.Count(); l != 2 || r != 1 {
		t.Fatalf("after three live entries: %d local, %d remote; want 2, 1", l, r)
	}
	tb.Merge([]Entry{{Namespace: "d", Filter: "t/1", Owner: "a", Version: 2, Tombstone: true}})
	if l, r := tb.Count(); l != 1 || r != 1 {
		t.Fatalf("after a local tombstone: %d local, %d remote; want 1, 1", l, r)
	}
	tb.MergeFull("b", nil, 5)
	if l, r := tb.Count(); l != 1 || r != 0 {
		t.Fatalf("after b's empty full sync: %d local, %d remote; want 1, 0", l, r)
	}
	tb.Forget("a")
	if l, r := tb.Count(); l != 0 || r != 0 {
		t.Fatalf("after forgetting a: %d local, %d remote; want 0, 0", l, r)
	}
}

func benchmarkCount(b *testing.B, n int) {
	tb := NewTable("a")
	in := make([]Entry, n)
	for i := range in {
		in[i] = Entry{Namespace: "d", Filter: fmt.Sprintf("c/%d", i), Owner: []string{"a", "b", "c"}[i%3], Version: 1, Count: 1}
	}
	tb.Merge(in)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Count()
	}
}

func BenchmarkCount1k(b *testing.B)   { benchmarkCount(b, 1000) }
func BenchmarkCount100k(b *testing.B) { benchmarkCount(b, 100000) }
