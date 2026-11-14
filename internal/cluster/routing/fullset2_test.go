package routing

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// pacedTransport records, at each part a plane sends, how many frames sat in
// the peer's data queue, and the order of parts and deltas.
type pacedTransport struct {
	*transport.Transport
	mu       sync.Mutex
	maxDepth uint64
	events   []string // "part:<snapshot>:<last>" or "delta"
	partSeen chan struct{}
}

func (p *pacedTransport) Send(ctx context.Context, peer string, c transport.Class, payload []byte) error {
	if c == transport.RouteUpdate && len(payload) > 0 {
		ev := "delta"
		if payload[0] == routePart {
			for _, info := range p.Peers() {
				if info.Name == peer {
					p.mu.Lock()
					p.maxDepth = max(p.maxDepth, info.DataAdmitted-info.DataTaken)
					p.mu.Unlock()
				}
			}
			var sp setPart
			_ = json.Unmarshal(payload[1:], &sp)
			ev = "part:" + strconv.FormatUint(sp.Snapshot, 10) + ":" + map[bool]string{true: "last", false: "more"}[sp.Last]
			select {
			case p.partSeen <- struct{}{}:
			default:
			}
		}
		if payload[0] == routePart || payload[0] == routeDelta {
			p.mu.Lock()
			p.events = append(p.events, ev)
			p.mu.Unlock()
		}
	}
	return p.Transport.Send(ctx, peer, c, payload)
}

// A set goes out in rounds, and a peer's next round waits until the link's
// writer has taken the previous round's last part: the data queue never
// holds more than a round of it, and a delta emitted mid-set leaves before
// the set's last part. A busy link slows a set but does not stop it.
func TestSetRoundsArePacedByTheWriter(t *testing.T) {
	var pt *pacedTransport
	a := frameNode(t, "a", frameOpts{})
	b := frameNode(t, "b", frameOpts{tweak: func(p *Plane) { p.partSize = 1 << 10 }, wrap: func(tr *transport.Transport) Transport {
		pt = &pacedTransport{Transport: tr, partSeen: make(chan struct{}, 1)}
		return pt
	}})
	pairUp(t, a, b)
	growOwnSet(t, a, b, 0, 1500) // about 135 parts of 1 KiB: five rounds
	b.tr.SetFault("a", &transport.Fault{Delay: 3 * time.Millisecond, Classes: []transport.Class{transport.RouteUpdate}})

	// Wait for a set to start, then emit a delta.
	pt.mu.Lock()
	pt.events = nil
	pt.mu.Unlock()
	for range 2 {
		<-pt.partSeen
	}
	in := a.m.get("hoardmq_route_syncs_total", "direction", "in")
	b.plane.Observe("default", "mid/set", 1)
	waitFor(t, "the delta at a", 10*time.Second, func() bool {
		_, ok := a.plane.Table().Lookup("b", "default", "mid/set")
		return ok
	})
	waitFor(t, "the set in progress to complete", 10*time.Second, func() bool {
		return a.m.get("hoardmq_route_syncs_total", "direction", "in") > in
	})
	pt.mu.Lock()
	depth, events := pt.maxDepth, append([]string(nil), pt.events...)
	pt.mu.Unlock()
	if depth > roundParts {
		t.Errorf("the data queue held %d frames at a part's send, over a round of %d", depth, roundParts)
	}
	delta := -1
	for i, ev := range events {
		if ev == "delta" {
			delta = i
			break
		}
	}
	if delta <= 0 {
		t.Fatalf("no delta after the set's first part: %v", events)
	}
	snapshot := strings.Split(events[0], ":")[1]
	lastAfter := false
	for _, ev := range events[delta:] {
		if ev == "part:"+snapshot+":last" {
			lastAfter = true
		}
	}
	if !lastAfter {
		t.Errorf("the delta left after the set's last part: %v", events)
	}

	// A busy data link: the set still completes.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		load := make([]byte, 4<<10)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = b.tr.Send(context.Background(), "a", transport.ReplicationAppend, load)
		}
	}()
	defer func() { close(stop); wg.Wait() }()
	in = a.m.get("hoardmq_route_syncs_total", "direction", "in")
	waitFor(t, "a set completed under load", 10*time.Second, func() bool {
		return a.m.get("hoardmq_route_syncs_total", "direction", "in") > in
	})

	// A data link reset mid-set ends the set; a later one completes.
	<-pt.partSeen
	b.tr.Reset("a", transport.RoleData, "test")
	in = a.m.get("hoardmq_route_syncs_total", "direction", "in")
	waitFor(t, "a set after the reset", 15*time.Second, func() bool {
		return a.m.get("hoardmq_route_syncs_total", "direction", "in") > in
	})
}

// A refused full set is counted by kind and reason and logged at Warn once in
// a burst, the out-counter counts only sets admitted, and the receiver warns
// that the sender's routes are not proved.
func TestRefusedSetIsCountedAndWarned(t *testing.T) {
	a := frameNode(t, "a", frameOpts{maxVersion: transport.Version5})
	b := frameNode(t, "b", frameOpts{})
	pairUp(t, a, b)
	growOwnSet(t, a, b, 0, 1500)
	waitFor(t, "a first refused set", 5*time.Second, func() bool {
		return b.m.get("hoardmq_route_send_failures_total", "kind", "full", "reason", "too-large") >= 1
	})
	out := b.m.get("hoardmq_route_syncs_total", "direction", "out")
	waitFor(t, "three refused sets", 5*time.Second, func() bool {
		return b.m.get("hoardmq_route_send_failures_total", "kind", "full", "reason", "too-large") >= 3
	})
	if n := b.logs.count(`msg="route update not sent" component=routing peer=a kind=full reason=too-large`); n != 1 {
		t.Errorf("%d Warns for a burst of refused sets, want 1", n)
	}
	if n := b.m.get("hoardmq_route_syncs_total", "direction", "out"); n != out {
		t.Errorf("syncs out rose from %v to %v with every set refused", out, n)
	}
	waitFor(t, "a's Warn that b's routes are not proved", 5*time.Second, func() bool {
		return a.logs.count(`level=WARN msg="a peer's routes are not proved here`) > 0
	})
}

// A build that reads parts sends a peer below framing version 6 one-frame
// sets, and that peer merges nothing from a part or a held sent to it;
// between the newer nodes sets travel in parts. Once the older node runs the
// newer build, sets complete everywhere and tombstones prune.
func TestMixedVersionRouteSync(t *testing.T) {
	var members atomic.Value
	members.Store([]string{"a", "b", "c"})
	mem := func() []string { return members.Load().([]string) }
	a := frameNode(t, "a", frameOpts{maxVersion: transport.Version5, legacy: true})
	b := frameNode(t, "b", frameOpts{members: mem})
	c := frameNode(t, "c", frameOpts{members: mem})
	pairUp(t, a, b, c)
	growOwnSet(t, a, b, 0, 1500)

	waitFor(t, "b's large set refused to the older a", 5*time.Second, func() bool {
		return b.m.get("hoardmq_route_send_failures_total", "kind", "full", "reason", "too-large") >= 1
	})
	cIn := c.m.get("hoardmq_route_syncs_total", "direction", "in")
	waitFor(t, "b's set in parts at c", 5*time.Second, func() bool {
		return c.m.get("hoardmq_route_syncs_total", "direction", "in") > cIn && b.m.get("hoardmq_route_set_parts_total", "direction", "out") > 0
	})

	// A part and a held sent to the older node are read as bad deltas.
	part := partOf(t, 1<<40, 1, 0, true, "seam/1")
	held, _ := json.Marshal(setHeld{Snapshot: 1 << 40})
	_ = c.tr.Send(context.Background(), "a", transport.RouteUpdate, part)
	_ = c.tr.Send(context.Background(), "a", transport.RouteUpdate, append([]byte{routeHeld}, held...))
	waitFor(t, "a's bad route update Warns", 5*time.Second, func() bool {
		return a.logs.count(`msg="bad route update" component=routing peer=c`) >= 2
	})
	if _, ok := a.plane.Table().Lookup("b", "default", "seam/1"); ok {
		t.Fatal("the older node merged a part")
	}

	// b's tombstones wait for the older node, which never acknowledges.
	for i := 0; i < 100; i++ {
		b.plane.Observe("default", frameFilter(i), 0)
	}
	time.Sleep(1500 * time.Millisecond)
	if n := b.plane.Table().OwnTombstones(); n != 100 {
		t.Fatalf("b pruned with an older member: %d own tombstones left", n)
	}

	// a runs the newer build.
	a.stop()
	a2 := frameNode(t, "a", frameOpts{members: mem})
	pairUp(t, a2, b, c)
	waitFor(t, "the new a to know every owner", 5*time.Second, a2.plane.OwnersKnown)
	waitFor(t, "b's tombstones pruned", 5*time.Second, func() bool { return b.plane.Table().OwnTombstones() == 0 })
	if held := liveFrom(a2, "b", 1500); held != 1400 {
		t.Fatalf("the new a holds %d of b's live entries, want 1400", held)
	}
}

// Own tombstones go only once every member has acknowledged a set past them:
// a member held down holds them until it returns, and a member removed stops
// holding them.
func TestOwnTombstonesPrunedOnceEveryMemberHolds(t *testing.T) {
	var members atomic.Value
	members.Store([]string{"a", "b", "c"})
	mem := func() []string { return members.Load().([]string) }
	a := frameNode(t, "a", frameOpts{members: mem})
	b := frameNode(t, "b", frameOpts{members: mem})
	c := frameNode(t, "c", frameOpts{members: mem})
	pairUp(t, a, b, c)
	growOwnSet(t, a, b, 0, 200)

	for i := 0; i < 100; i++ {
		b.plane.Observe("default", frameFilter(i), 0)
	}
	waitFor(t, "the first tombstones pruned", 5*time.Second, func() bool { return b.plane.Table().OwnTombstones() == 0 })

	// c held down: nothing pruned until it returns.
	b.tr.Isolate("c", true)
	c.tr.Isolate("b", true)
	for i := 100; i < 150; i++ {
		b.plane.Observe("default", frameFilter(i), 0)
	}
	time.Sleep(1500 * time.Millisecond)
	if n := b.plane.Table().OwnTombstones(); n != 50 {
		t.Fatalf("pruned with a member down: %d own tombstones left, want 50", n)
	}
	b.tr.Isolate("c", false)
	c.tr.Isolate("b", false)
	waitFor(t, "pruned once c is back", 10*time.Second, func() bool { return b.plane.Table().OwnTombstones() == 0 })
	if held := liveFrom(c, "b", 200); held != 50 {
		t.Fatalf("c holds %d of b's live entries after its return, want 50", held)
	}

	// c removed from the membership: it no longer holds them.
	b.tr.Isolate("c", true)
	c.tr.Isolate("b", true)
	for i := 150; i < 200; i++ {
		b.plane.Observe("default", frameFilter(i), 0)
	}
	time.Sleep(1 * time.Second)
	if n := b.plane.Table().OwnTombstones(); n != 50 {
		t.Fatalf("pruned with a member down: %d left, want 50", n)
	}
	members.Store([]string{"a", "b"})
	waitFor(t, "pruned once c is removed", 5*time.Second, func() bool { return b.plane.Table().OwnTombstones() == 0 })
}

// An own entry no frame can carry is left out of route sync, counted and
// logged; everything else still syncs.
func TestUnsendableEntryIsLeftOut(t *testing.T) {
	a := frameNode(t, "a", frameOpts{})
	b := frameNode(t, "b", frameOpts{})
	pairUp(t, a, b)
	huge := "h/" + strings.Repeat("x", frameLimit)
	b.plane.Observe("default", huge, 1)
	b.plane.Observe("default", "ok/1", 1)
	waitFor(t, "the other entry at a", 5*time.Second, func() bool {
		_, ok := a.plane.Table().Lookup("b", "default", "ok/1")
		return ok
	})
	if n := b.m.get("hoardmq_route_entries_unsendable_total"); n != 1 {
		t.Fatalf("unsendable counted %v", n)
	}
	if b.logs.count("route entries no cluster frame can carry") != 1 {
		t.Fatal("no Warn for the unsendable entry")
	}
	in := a.m.get("hoardmq_route_syncs_total", "direction", "in")
	waitFor(t, "full sets from b go on", 5*time.Second, func() bool {
		return a.m.get("hoardmq_route_syncs_total", "direction", "in") >= in+2
	})
	if _, ok := a.plane.Table().Lookup("b", "default", huge); ok {
		t.Fatal("a holds the entry no frame can carry")
	}
}

// An entry versioned before a set is read and merged after it would be
// missing from a set whose snapshot is above its version, and a peer that had
// its delta while the set was in flight would withdraw it at the set's end.
// The version is taken under the table's lock in the step that merges it.
func TestSetNeverWithdrawsAnEntryMergedDuringIt(t *testing.T) {
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	var armed atomic.Bool
	entered := make(chan struct{}, 1)
	a := frameNode(t, "a", frameOpts{})
	b := frameNode(t, "b", frameOpts{sync: time.Hour, tweak: func(p *Plane) {
		p.partSize, p.roundParts = 1<<10, 1
		p.beforeOwnMerge = func() {
			if armed.CompareAndSwap(true, false) {
				entered <- struct{}{}
				<-hold
			}
		}
	}})
	pairUp(t, a, b)
	growOwnSet(t, a, b, 0, 1500)
	b.tr.SetFault("a", &transport.Fault{Delay: 3 * time.Millisecond, Classes: []transport.Class{transport.RouteUpdate}})

	armed.Store(true)
	go b.plane.Observe("default", "late/x", 1)
	<-entered
	in := a.m.get("hoardmq_route_syncs_total", "direction", "in")
	b.plane.queueFull("a")
	waitFor(t, "a set in progress at a", 5*time.Second, func() bool {
		a.plane.inMu.Lock()
		s := a.plane.inSets["b"]
		a.plane.inMu.Unlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.active
	})
	release()
	waitFor(t, "late/x at a", 5*time.Second, func() bool {
		_, ok := a.plane.Table().Lookup("b", "default", "late/x")
		return ok
	})
	waitFor(t, "the set completed", 10*time.Second, func() bool {
		return a.m.get("hoardmq_route_syncs_total", "direction", "in") > in
	})
	if e, ok := a.plane.Table().Lookup("b", "default", "late/x"); !ok || e.Tombstone {
		t.Fatalf("the set withdrew an entry merged during it: %+v", e)
	}
}
