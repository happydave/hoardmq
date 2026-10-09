package routing

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// decodeRouteFrame reads a route-update frame the stub captured: its kind,
// a full set's number, or a delta's number from its first entry.
func decodeRouteFrame(t *testing.T, f sentFrame) (kind byte, number uint64, entries []Entry) {
	t.Helper()
	if f.class != transport.RouteUpdate || len(f.payload) == 0 {
		t.Fatalf("not a route update: %+v", f)
	}
	kind = f.payload[0]
	if kind == routeFull {
		var fs fullSync
		if err := json.Unmarshal(f.payload[1:], &fs); err != nil {
			t.Fatal(err)
		}
		return kind, fs.Batch, fs.Entries
	}
	if err := json.Unmarshal(f.payload[1:], &entries); err != nil {
		t.Fatal(err)
	}
	for i, e := range entries[1:] {
		if e.Batch != 0 {
			t.Fatalf("entry %d of a delta carries a batch number %d; only the first does", i+1, e.Batch)
		}
	}
	return kind, entries[0].Batch, entries
}

// TestRouteBatchesAreNumberedAndAFullSetCoversThem drives the flusher by
// hand: each delta batch takes the next number, a full set composed in the
// same flush carries the number of the batch it holds, and the mark counts a
// change still waiting for its batch.
func TestRouteBatchesAreNumberedAndAFullSetCoversThem(t *testing.T) {
	tr := newStubTransport("n3")
	p := New(Options{Node: "n2", Transport: tr, SyncInterval: time.Hour, Logger: quiet})
	seed := p.RouteMark()
	if seed == 0 {
		t.Fatal("the mark before any batch is zero, which is what an older sender's absent number reads as")
	}
	tr.setState("n3", transport.Up) // queues a full set for n3
	p.emit("default", "a", 1)
	if got := p.RouteMark(); got != seed+1 {
		t.Fatalf("mark with a change pending: %d, want %d", got, seed+1)
	}
	p.flushOnce()
	frames := tr.sentFrames()
	if len(frames) != 2 {
		t.Fatalf("frames %d after one flush, want a full set and a delta", len(frames))
	}
	kind, cover, entries := decodeRouteFrame(t, frames[0])
	if kind != routeFull || len(entries) != 1 {
		t.Fatalf("first frame kind %d with %d entries, want the full set holding a", kind, len(entries))
	}
	kind, first, _ := decodeRouteFrame(t, frames[1])
	if kind != routeDelta || first != seed+1 || cover != first {
		t.Fatalf("delta %d numbered %d, full set covering %d; want the delta numbered %d and the set covering it", kind, first, cover, seed+1)
	}
	if got := p.RouteMark(); got != first {
		t.Fatalf("mark with nothing pending: %d, want %d", got, first)
	}

	p.emit("default", "b", 1)
	p.emit("default", "c", 1)
	p.flushOnce()
	frames = tr.sentFrames()
	_, second, entries := decodeRouteFrame(t, frames[len(frames)-1])
	if second != first+1 || len(entries) != 2 {
		t.Fatalf("second batch numbered %d with %d entries, want %d with 2", second, len(entries), first+1)
	}
	if got := p.RouteMark(); got != second {
		t.Fatalf("mark %d after the second batch, want %d", got, second)
	}
}

func TestRouteProof(t *testing.T) {
	var r routeProof
	if r.holds(1) {
		t.Fatal("holds before anything arrived")
	}
	r.full(100)
	if !r.holds(100) || r.holds(101) {
		t.Fatal("a full set covering 100 must hold 100 and not 101")
	}
	if r.delta(100) || !r.holds(100) {
		t.Fatal("the batch a full set covers, arriving after it, broke the proof")
	}
	if r.delta(101) || !r.holds(101) {
		t.Fatal("the next batch did not advance the proof")
	}
	if !r.delta(103) {
		t.Fatal("a skipped batch was not a gap")
	}
	if r.holds(101) {
		t.Fatal("the proof survived a gap")
	}
	if r.delta(104) || r.holds(104) {
		t.Fatal("a batch after a gap advanced the proof, or was counted as a second gap")
	}
	r.full(110)
	if !r.holds(110) {
		t.Fatal("a full set did not restore the proof")
	}
	if !r.delta(0) || r.holds(110) {
		t.Fatal("an unnumbered delta (an older sender) kept the proof")
	}
	r.full(0)
	if r.holds(1) {
		t.Fatal("an unnumbered full set proved something")
	}
	if r.delta(1); r.holds(1) {
		t.Fatal("a delta after an unnumbered full set was taken as its successor")
	}
	r.full(120)
	if r.holds(0) {
		t.Fatal("a zero mark was held; a fact with no mark proves nothing")
	}
}

// TestAPeersRouteProofOutlivesItsLinkButNotAGap: what arrived stays
// received when the link goes; a skipped batch, on any link, stops the proof
// until the next full set, and is counted.
func TestAPeersRouteProofOutlivesItsLinkButNotAGap(t *testing.T) {
	f := &servedFixture{owned: map[string]int{}}
	p, tr := newServedPlane(t, f)
	tr.setState("n3", transport.Up)
	tr.deliverFull(t, "n3", nil, 200)
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/x", Version: 2, Count: 1}}, 201)
	if !p.routesHeld("n3", 201) {
		t.Fatal("contiguous batches did not prove n3's routes to 201")
	}
	tr.setState("n3", transport.Down)
	tr.setState("n3", transport.Up)
	if !p.routesHeld("n3", 201) {
		t.Fatal("a link going down undid what had arrived")
	}
	tr.deliverDelta(t, "n3", []Entry{{Namespace: "default", Filter: "t/y", Version: 3, Count: 1}}, 203)
	if p.routesHeld("n3", 201) {
		t.Fatal("the proof survived a gap")
	}
	if got := f.metrics.get("hoardmq_route_update_gaps_total{}"); got != 1 {
		t.Fatalf("gaps counted %v, want 1", got)
	}
	merged := p.Table().byOwner("n3")
	if len(merged) != 2 {
		t.Fatalf("n3's entries %+v, want t/x and t/y merged", merged)
	}
	for _, e := range merged {
		if e.Batch != 0 {
			t.Fatalf("a merged entry kept the batch number it rode: %+v", e)
		}
	}
	tr.deliverFull(t, "n3", []Entry{{Namespace: "default", Filter: "t/x", Version: 2, Count: 1}}, 210)
	if !p.routesHeld("n3", 210) {
		t.Fatal("a full set did not restore the proof")
	}
}
