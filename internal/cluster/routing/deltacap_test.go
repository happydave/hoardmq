package routing

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// markCase is one run of random emits, mark reads and flushes against a
// plane whose deltas a stub records.
type markRead struct {
	mark   uint64
	queued map[string]uint64 // filter → the version emitted last before the read
}

// runMarks drives one seeded interleaving and returns the marks read and the
// delta batches sent, by number.
func runMarks(t *testing.T, seed int64) ([]markRead, map[uint64][]Entry, []int) {
	t.Helper()
	tr := newStubTransport("p")
	tr.peers[0].State = transport.Up
	p := New(Options{Node: "n1", Transport: tr, Logger: quiet})
	p.partSize = 512
	rng := rand.New(rand.NewSource(seed))
	filters := make([]string, 40)
	for i := range filters {
		filters[i] = fmt.Sprintf("f/%d/%s", i, strings.Repeat("x", rng.Intn(60)))
	}
	counts := []int{0, 1, 7, 123456789}
	queued := map[string]uint64{}
	var marks []markRead
	for step := 0; step < 400; step++ {
		switch r := rng.Intn(10); {
		case r < 7:
			f := filters[rng.Intn(len(filters))]
			p.emit("default", f, counts[rng.Intn(len(counts))])
			e, _ := p.Table().Lookup("n1", "default", f)
			queued[f] = e.Version
		case r < 9:
			m := markRead{mark: p.RouteMark(), queued: map[string]uint64{}}
			for f, v := range queued {
				m.queued[f] = v
			}
			marks = append(marks, m)
		default:
			p.flushOnce()
		}
	}
	for p.flushOnce() {
	}
	sent := map[uint64][]Entry{}
	var sizes []int
	for _, f := range tr.frames {
		if f.payload[0] != routeDelta {
			continue
		}
		var es []Entry
		if err := json.Unmarshal(f.payload[1:], &es); err != nil {
			t.Fatal(err)
		}
		sent[es[0].Batch] = es
		sizes = append(sizes, len(f.payload))
	}
	return marks, sent, sizes
}

// Safety: every change queued before a mark read is sent in a batch at or
// below that mark, whatever replaced it since, so a forget proved by the mark
// never goes early.
func TestMarkCoversEveryChangeQueuedBeforeIt(t *testing.T) {
	for seed := int64(1); seed <= 200; seed++ {
		marks, sent, _ := runMarks(t, seed)
		for _, m := range marks {
			for f, v := range m.queued {
				ok := false
				for n, es := range sent {
					for _, e := range es {
						if e.Filter == f && e.Version >= v && n <= m.mark {
							ok = true
						}
					}
				}
				if !ok {
					t.Fatalf("seed %d: %s at version %d, queued before mark %d, is in no batch at or below it", seed, f, v, m.mark)
				}
			}
		}
	}
}

// Liveness: every number up to every mark read is sent, so nothing waiting on
// a mark waits on a batch that never comes; and every batch stays under the
// part size.
func TestEveryMarkIsReachedByABatchSent(t *testing.T) {
	for seed := int64(1); seed <= 200; seed++ {
		marks, sent, sizes := runMarks(t, seed)
		var lo, hi uint64
		for n := range sent {
			if lo == 0 || n < lo {
				lo = n
			}
			hi = max(hi, n)
		}
		for n := lo; n <= hi; n++ {
			if _, ok := sent[n]; !ok {
				t.Fatalf("seed %d: batch %d of %d..%d never sent", seed, n, lo, hi)
			}
		}
		for _, m := range marks {
			if m.mark > hi {
				t.Fatalf("seed %d: mark %d above the last batch sent, %d", seed, m.mark, hi)
			}
		}
		for _, s := range sizes {
			if s > 512+64 {
				t.Fatalf("seed %d: a delta of %d bytes", seed, s)
			}
		}
	}
}
