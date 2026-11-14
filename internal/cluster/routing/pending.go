package routing

import (
	"encoding/json"
	"sort"
)

// The route changes waiting for the flusher are cut into delta batches of at
// most a part's size as they are queued, so that a mark read while they wait
// can promise numbers for them without sizing every one: it numbers the
// groups formed so far. A numbered group keeps its number, and its entries
// keep their group when replaced, whatever their size becomes: a mark
// promises that every change queued before it is in a batch at most that
// number, and that every number up to it is sent. An entry replaced while its
// group is unnumbered moves to the open group, so no group outgrows the part
// size before it is numbered; a numbered one can grow only by what a
// replacement adds (a count's digits, the tombstone flag), which the part
// size's headroom below every frame limit absorbs.

// pendingEntry is one waiting change with its encoded size.
type pendingEntry struct {
	e    Entry
	size int
	grp  *batchGroup
}

// batchGroup is a delta batch being formed: number is zero until a mark or a
// flush numbers it. keys may hold entries that have since moved to another
// group; a key belongs to the group its pendingEntry names.
type batchGroup struct {
	number uint64
	bytes  int
	n      int
	keys   []entryKey
}

// entrySize is an entry's share of a batch's JSON: its encoding and a comma.
func entrySize(e Entry) int {
	b, _ := json.Marshal(e)
	return len(b) + 1
}

// queueLocked adds or replaces a waiting change. Callers hold p.mu.
func (p *Plane) queueLocked(e Entry) {
	k := entryKey{e.Owner, e.Namespace, e.Filter}
	prev := p.pending[k]
	if prev != nil && e.Version < prev.e.Version {
		return
	}
	size := entrySize(e)
	if prev != nil && prev.grp.number != 0 {
		prev.grp.bytes += size - prev.size
		prev.e, prev.size = e, size
		return
	}
	if prev != nil {
		prev.grp.bytes -= prev.size
		prev.grp.n--
	}
	pe := prev
	if pe == nil {
		pe = &pendingEntry{}
		p.pending[k] = pe
	}
	pe.e, pe.size = e, size
	g := p.openGroupLocked(size)
	pe.grp = g
	g.bytes += size
	g.n++
	g.keys = append(g.keys, k)
}

// openGroupLocked is the unnumbered group an entry of size goes into: the last
// one while it has room, else a new one. An entry larger than the part size
// goes alone.
func (p *Plane) openGroupLocked(size int) *batchGroup {
	if n := len(p.groups); n > 0 {
		g := p.groups[n-1]
		if g.number == 0 && (g.n == 0 || g.bytes+size <= p.batchBudget()) {
			return g
		}
	}
	g := &batchGroup{}
	p.groups = append(p.groups, g)
	return g
}

// batchBudget is how many bytes of entries a delta batch carries: a part's
// size less the kind byte, the array's brackets and the batch number on its
// first entry.
func (p *Plane) batchBudget() int {
	if p.legacy() {
		return int(^uint(0) >> 1)
	}
	return p.partSize - 64
}

// numberGroupsLocked gives every unnumbered group that holds an entry its
// number, in order. Callers hold p.mu.
func (p *Plane) numberGroupsLocked() {
	for _, g := range p.groups {
		if g.number == 0 && g.n > 0 {
			p.assigned++
			g.number = p.assigned
		}
	}
}

// takeBatchesLocked empties the waiting changes into numbered batches, the
// numbered groups first under their numbers and the rest numbered after
// them, each batch sorted by version. Callers hold p.mu.
func (p *Plane) takeBatchesLocked() [][]Entry {
	p.numberGroupsLocked()
	var out [][]Entry
	for _, g := range p.groups {
		if g.n == 0 {
			continue
		}
		batch := make([]Entry, 0, g.n)
		for _, k := range g.keys {
			if pe := p.pending[k]; pe != nil && pe.grp == g {
				batch = append(batch, pe.e)
				pe.grp = nil
			}
		}
		sort.Slice(batch, func(i, j int) bool { return batch[i].Version < batch[j].Version })
		batch[0].Batch = g.number
		out = append(out, batch)
		p.batches = g.number
	}
	clear(p.pending)
	p.groups = p.groups[:0]
	return out
}
