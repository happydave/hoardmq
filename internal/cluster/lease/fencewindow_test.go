package lease

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// fenceCase stages one partition: the staged node's last renewal held
// between its result arriving and its record (the return trip), its service
// started later than the others' (which offsets its quarter-ttl tick from the
// leader's), and its fence made late.
type fenceCase struct {
	name                   string
	ttl, hold, phase, late time.Duration
}

// TestFenceComesBeforeTheGrant stages a partition that begins just after the
// leader applied a follower's renewal, and requires the follower's fence
// before any majority-side grant of one of its clients, by at least the
// declaration margin less the fence's lateness (and 20 ms for
// scheduling). The default cases run at a 1.5 s ttl; HOARDMQ_FENCEWINDOW=product
// adds the product's 30 s ttl cases, about 47 s each.
func TestFenceComesBeforeTheGrant(t *testing.T) {
	short := 1500 * time.Millisecond
	cases := []fenceCase{
		{name: "no hold", ttl: short},
		{name: "return held 200ms", ttl: short, hold: 200 * time.Millisecond},
		{name: "return held 500ms", ttl: short, hold: 500 * time.Millisecond},
		{name: "fence late by the margin", ttl: short, late: declarationMargin(short)},
	}
	if os.Getenv("HOARDMQ_FENCEWINDOW") == "product" {
		product := 30 * time.Second
		cases = append(cases,
			fenceCase{name: "product, tick offset 7s", ttl: product, phase: 7 * time.Second},
			fenceCase{name: "product, return held 8s", ttl: product, hold: 8 * time.Second},
			fenceCase{name: "product, tick offset 7s, fence late by the margin", ttl: product, phase: 7 * time.Second, late: declarationMargin(product)},
			// The leader's tick just past the staged node's record, the ttl and
			// the margin: the declaration comes as early as it can.
			fenceCase{name: "product, tick offset 6.5s, fence late by the margin", ttl: product, phase: 6500 * time.Millisecond, late: declarationMargin(product)},
		)
	}
	for _, fc := range cases {
		t.Run(fc.name, func(t *testing.T) { runFenceCase(t, fc) })
	}
}

func runFenceCase(t *testing.T, fc fenceCase) {
	tick := fc.ttl / 4
	const staged = "n3"
	var armed atomic.Bool
	var cutNs, returnedNs, leaderLoopNs, stagedLoopNs atomic.Int64
	var cut func()
	fixtureOptions = func(o *Options) {
		if o.Node != staged {
			return
		}
		o.RenewalGate = func() {
			if !armed.CompareAndSwap(true, false) {
				return
			}
			cutNs.Store(time.Now().UnixNano())
			cut()
			time.Sleep(fc.hold)
			returnedNs.Store(time.Now().UnixNano())
		}
		o.FenceGate = func() { time.Sleep(fc.late) }
	}
	fixtureService = func(node string, _ *Service) {
		if node == staged {
			time.Sleep(fc.phase)
		}
	}
	t.Cleanup(func() { fixtureOptions, fixtureService = nil, nil })

	c := newCluster(t, []string{"n1", "n2", staged}, fc.ttl, func(name string) string { return name + ".clients:1883" })
	cc := c.nodes[staged]
	lead := c.settled()
	if lead == cc {
		t.Fatalf("%s leads; the staged node must be a follower", staged)
	}
	other := c.nodes["n1"]
	if other == lead {
		other = c.nodes["n2"]
	}
	cut = func() {
		leaderLoopNs.Store(lead.svc.loopLastNs.Load())
		stagedLoopNs.Store(cc.svc.loopLastNs.Load())
		for _, n := range []*tnode{lead, other} {
			n.tr.Isolate(cc.name, true)
			cc.tr.Isolate(n.name, true)
		}
	}
	if _, err := acquire(t, cc, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	cc.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: 1}})

	armed.Store(true)
	waitFor(t, "the staged renewal's return", fc.ttl+fc.hold, func() bool { return returnedNs.Load() != 0 })

	var grantAt time.Time
	deadline := time.Now().Add(3*fc.ttl + 10*time.Second)
	for time.Now().Before(deadline) && (grantAt.IsZero() || cc.sessions.fenceCount() == 0) {
		if grantAt.IsZero() {
			if _, err := acquire(t, other, "x", nil, 300, false); err == nil {
				grantAt = time.Now()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if grantAt.IsZero() || cc.sessions.fenceCount() == 0 {
		t.Fatalf("by the deadline: granted %v, fenced %v", !grantAt.IsZero(), cc.sessions.fenceCount() != 0)
	}
	cc.sessions.mu.Lock()
	fencedAt := cc.sessions.fencedAt
	cc.sessions.mu.Unlock()

	applied := lead.svc.timeline.of(cc.name).lastAlive.Load()
	declared := lead.svc.timeline.of(cc.name).proposed.Load()
	record := cc.svc.lastAlive.Load()
	rel := func(ns int64) time.Duration { return time.Duration(ns - applied).Round(time.Millisecond) }
	grid := time.Duration(stagedLoopNs.Load()-leaderLoopNs.Load()) % tick
	if grid < 0 {
		grid += tick
	}
	margin := grantAt.Sub(fencedAt)
	want := declarationMargin(fc.ttl) - fc.late - 20*time.Millisecond
	t.Logf("fencewindow: ttl %s, tick %s, hold %s, phase %s, fence late %s; the staged node's tick %s after the leader's",
		fc.ttl, tick, fc.hold, fc.phase, fc.late, grid.Round(time.Millisecond))
	t.Logf("fencewindow: from the leader's apply of the last renewal: cut %s, the staged node's record %s (returned %s), fenced %s, declared %s, granted %s",
		rel(cutNs.Load()), rel(record), rel(returnedNs.Load()), rel(fencedAt.UnixNano()), rel(declared), rel(grantAt.UnixNano()))
	t.Logf("fencewindow verdict: grant minus fence %s, required at least %s", margin.Round(time.Millisecond), want)
	if !fencedAt.Before(grantAt) || margin < want {
		t.Fatalf("granted %s after the fence, want the fence first by at least %s", margin.Round(time.Millisecond), want)
	}
}

// TestDeclarationComesAtTheMarginNotATick stages a dead node whose last alive
// record applies so that the leader's next tick falls inside the declaration
// margin: the leader holds the follower's forwarded
// renewal until that moment, cuts the follower off, and lets it apply. The
// declaration must come at the margin's end, within 40 ms, not a tick later.
func TestDeclarationComesAtTheMarginNotATick(t *testing.T) {
	ttl := 3 * time.Second
	tick, margin := ttl/4, declarationMargin(ttl)
	const slack = 40 * time.Millisecond
	var armed atomic.Bool
	var place func()
	var victimName atomic.Value
	fixtureOptions = func(o *Options) {
		o.ForwardedGate = func(peer string, ops []Op) {
			if !armed.Load() || peer != victimName.Load() {
				return
			}
			for _, op := range ops {
				if op.Kind == OpAlive && armed.CompareAndSwap(true, false) {
					place()
					return
				}
			}
		}
	}
	t.Cleanup(func() { fixtureOptions = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, ttl, nil)
	lead := c.settled()
	victim := c.follower(lead)
	other := c.nodes["n1"]
	for _, n := range c.nodes {
		if n != lead && n != victim {
			other = n
		}
	}
	// The renewal applies half a margin before a leader tick less the ttl,
	// so that tick finds the victim past its ttl and inside the margin.
	place = func() {
		for _, n := range []*tnode{lead, other} {
			n.tr.Isolate(victim.name, true)
			victim.tr.Isolate(n.name, true)
		}
		at := time.Unix(0, lead.svc.loopLastNs.Load()).Add(-margin / 2)
		for at.Before(time.Now().Add(5 * time.Millisecond)) {
			at = at.Add(tick)
		}
		time.Sleep(time.Until(at))
	}
	victimName.Store(victim.name)
	armed.Store(true)
	waitFor(t, "the victim's declaration", 3*ttl, func() bool { return lead.svc.timeline.of(victim.name).proposed.Load() != 0 })
	applied := lead.svc.timeline.of(victim.name).lastAlive.Load()
	proposed := lead.svc.timeline.of(victim.name).proposed.Load()
	late := time.Duration(proposed-applied) - ttl
	t.Logf("declaration %s after the victim's last record and the ttl; margin %s, tick %s", late.Round(time.Millisecond), margin, tick)
	if late < margin-2*time.Millisecond || late > margin+slack {
		t.Fatalf("declared %s after the record and the ttl, want %s to %s (a tick late would be about %s)",
			late.Round(time.Millisecond), margin, margin+slack, (margin/2 + tick).Round(time.Millisecond))
	}
}
