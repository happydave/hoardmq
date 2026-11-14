package replication

import "sync/atomic"

// Histogram is one distribution of the lock trace or the fetch timing, read
// for export: counts per power-of-two bucket of microseconds (bucketOf), the
// count, which is the buckets' sum, and the sum of the samples.
type Histogram struct {
	Buckets [histBuckets]int64
	Count   int64
	SumNs   int64
}

// waitedKinds are the holders whose waits for the lock are recorded: a fetch's
// find and the applies. Another holder's waits are not timed, so no wait
// histogram is read for it.
var waitedKinds = [...]int{holdApply, holdApplyOther, holdFind}

func readHistogram(h *[histBuckets]atomic.Int64, sum int64) Histogram {
	out := Histogram{SumNs: sum}
	for i := range h {
		out.Buckets[i] = h[i].Load()
		out.Count += out.Buckets[i]
	}
	return out
}

// LockHistograms reads the follower's lock trace by holder: the holds of
// every kind, and the waits of the kinds whose waits are recorded.
func (r *Replicator) LockHistograms() (holds, waits map[string]Histogram) {
	c := &r.follower.lockHolds
	holds = make(map[string]Histogram, holdKinds)
	for k := 0; k < holdKinds; k++ {
		holds[holdNames[k]] = readHistogram(&c.holds[k], c.total[k].Load())
	}
	waits = make(map[string]Histogram, len(waitedKinds))
	for _, k := range waitedKinds {
		waits[holdNames[k]] = readHistogram(&c.waits[k], c.waitTotal[k].Load())
	}
	return holds, waits
}

// FetchRequestHistogram reads the time of this node's fetch requests to a
// holder, from before the send to the answer, the request's end or a failed
// send.
func (r *Replicator) FetchRequestHistogram() Histogram {
	return readHistogram(&r.fetches.requestHist, r.fetches.requestNs.Load())
}
