package lease

import (
	"container/heap"
	"sort"
	"strings"
)

// Scan returns one page of a namespace's leases in client-identifier order:
// those whose identifier contains search and sorts after after, owned by
// owner when it is not empty, at most limit of them, and whether more match.
//
// Under the read lock it keeps only the limit+1 smallest matches, so the lock
// is held for the walk and not for copying the table; the page is sorted
// after the lock is released.
func (t *Table) Scan(ns, search, after, owner string, limit int) (out []Lease, more bool) {
	if limit < 1 {
		return []Lease{}, false
	}
	h := make(byIDDesc, 0, limit+1)
	t.mu.RLock()
	for k, l := range t.leases {
		if k.ns != ns || k.id <= after || (owner != "" && l.Owner != owner) || !strings.Contains(k.id, search) {
			continue
		}
		if len(h) < limit+1 {
			heap.Push(&h, *l)
			continue
		}
		if k.id >= h[0].ClientID {
			continue
		}
		h[0] = *l
		heap.Fix(&h, 0)
	}
	t.mu.RUnlock()
	out = []Lease(h)
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	if len(out) > limit {
		return out[:limit], true
	}
	return out, false
}

// byIDDesc is a max-heap of leases by client identifier: its root is the
// largest kept, the one a smaller match replaces.
type byIDDesc []Lease

func (h byIDDesc) Len() int           { return len(h) }
func (h byIDDesc) Less(i, j int) bool { return h[i].ClientID > h[j].ClientID }
func (h byIDDesc) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *byIDDesc) Push(x any)        { *h = append(*h, x.(Lease)) }
func (h *byIDDesc) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
