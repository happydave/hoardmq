package lease

import "sync"

// claimRace records, per previous owner, how far that owner's clients had got
// in claiming their own leases when the leader's reassignment swept up the
// rest. Every node sees every applied lease change, so it is fed from all of
// them and one node's figures are the cluster's.
//
// A lease that changes owner and arrives online was claimed by its client on
// the new node. One that arrives offline was moved: a reassignment or a drain
// hand-off moves every lease the owner still holds in a single apply.
type claimRace struct {
	mu        sync.Mutex
	claims    map[string]int64 // leases clients took from the owner
	moved     map[string]int64 // leases moved from the owner by reassignment or hand-off
	claimedAt map[string]int64 // claims when the first lease was moved
	movedAtMs map[string]int64 // when that was
}

// observe reads the clock only when it records a first moved lease: it runs on
// every applied change, and nearly all of them keep their owner.
func (c *claimRace) observe(before, after *Lease, nowMs func() int64) {
	if before == nil || after == nil || before.Owner == after.Owner {
		return
	}
	from := before.Owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.claims == nil {
		c.claims, c.moved = map[string]int64{}, map[string]int64{}
		c.claimedAt, c.movedAtMs = map[string]int64{}, map[string]int64{}
	}
	if after.Online {
		c.claims[from]++
		return
	}
	if _, seen := c.movedAtMs[from]; !seen {
		c.claimedAt[from], c.movedAtMs[from] = c.claims[from], nowMs()
	}
	c.moved[from]++
}

// snapshot copies the four maps; each is nil when nothing was recorded.
func (c *claimRace) snapshot() (claims, moved, claimedAt, movedAtMs map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyCounts(c.claims), copyCounts(c.moved), copyCounts(c.claimedAt), copyCounts(c.movedAtMs)
}

func copyCounts(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
