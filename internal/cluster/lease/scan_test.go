package lease

import (
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestScanPagesInIdentifierOrder: pages walked by cursor give every match
// once, in order, from leases applied in a shuffled order; a substring and an
// owner narrow the set; only the last page says there is no more.
func TestScanPagesInIdentifierOrder(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	applyAt(t, tb, 1, Op{Kind: OpAlive, Node: "n1", Now: now}, Op{Kind: OpAlive, Node: "n2", Now: now})
	ids := make([]string, 1000)
	for i := range ids {
		ids[i] = fmt.Sprintf("c%04d", i)
	}
	shuffled := slices.Clone(ids)
	rand.New(rand.NewSource(7)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	ops := make([]Op, 0, len(ids)+1)
	for i, id := range shuffled {
		ops = append(ops, Op{Kind: OpAcquire, Namespace: "default", ClientID: id, Node: []string{"n1", "n2"}[i%2], Now: now})
	}
	ops = append(ops, Op{Kind: OpAcquire, Namespace: "other", ClientID: "c0001", Node: "n1", Now: now})
	applyAt(t, tb, 2, ops...)

	walk := func(search, owner string) []string {
		var got []string
		after := ""
		for page := 0; ; page++ {
			if page > len(ids) {
				t.Fatalf("search %q owner %q: no last page", search, owner)
			}
			items, more := tb.Scan("default", search, after, owner, 7)
			if len(items) > 7 {
				t.Fatalf("a page of %d, limit 7", len(items))
			}
			for _, l := range items {
				got = append(got, l.ClientID)
			}
			if !more {
				return got
			}
			if len(items) == 0 {
				t.Fatalf("search %q owner %q: an empty page that says there is more", search, owner)
			}
			after = items[len(items)-1].ClientID
		}
	}
	if got := walk("", ""); !slices.Equal(got, ids) {
		t.Fatalf("every lease: %d identifiers, sorted %v; want the %d in order", len(got), slices.IsSorted(got), len(ids))
	}
	var want []string
	for _, id := range ids {
		if strings.Contains(id, "12") {
			want = append(want, id)
		}
	}
	if got := walk("12", ""); !slices.Equal(got, want) {
		t.Fatalf("substring 12: %d identifiers, want %d (%v)", len(got), len(want), want)
	}
	owned := map[string]string{}
	for i, id := range shuffled {
		owned[id] = []string{"n1", "n2"}[i%2]
	}
	want = want[:0]
	for _, id := range ids {
		if owned[id] == "n2" {
			want = append(want, id)
		}
	}
	if got := walk("", "n2"); !slices.Equal(got, want) {
		t.Fatalf("owner n2: %d identifiers, want %d", len(got), len(want))
	}
	if items, more := tb.Scan("default", "", "c0999", "", 7); len(items) != 0 || more {
		t.Fatalf("after the last identifier: %d items, more %v", len(items), more)
	}
	if items, more := tb.Scan("default", "", "", "", 1000); len(items) != 1000 || more {
		t.Fatalf("a page holding every lease: %d items, more %v", len(items), more)
	}
}

// TestScanKeepsOnlyAPage: a page of ten from 20,000 leases copies about ten
// leases, not the table. A scan that collected every match would allocate
// a copy of each.
func TestScanKeepsOnlyAPage(t *testing.T) {
	tb := NewTable()
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	applyAt(t, tb, 1, Op{Kind: OpAlive, Node: "n1", Now: now})
	ops := make([]Op, 0, 20000)
	for i := range 20000 {
		ops = append(ops, Op{Kind: OpAcquire, Namespace: "default", ClientID: fmt.Sprintf("c%05d", i), Node: "n1", Now: now})
	}
	applyAt(t, tb, 2, ops...)
	tb.Scan("default", "", "", "", 10)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	items, _ := tb.Scan("default", "", "", "", 10)
	runtime.ReadMemStats(&after)
	if len(items) != 10 {
		t.Fatalf("a page of %d", len(items))
	}
	// 20,000 copies of a lease are more than 2 MB; one page is a few kilobytes.
	got := after.TotalAlloc - before.TotalAlloc
	if got > 64<<10 {
		t.Fatalf("one page allocated %d bytes", got)
	}
	t.Logf("one page of 10 from 20,000 leases allocated %d bytes", got)
}
