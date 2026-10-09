package sim

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/lease"
	"github.com/happydave/hoardmq/internal/cluster/replication"
	"github.com/happydave/hoardmq/internal/cluster/routing"
	"github.com/happydave/hoardmq/internal/cluster/transport"
)

var dropCrash = time.Date(2026, 9, 24, 3, 0, 0, 0, time.UTC)

func at(sec float64) int64 {
	return dropCrash.Add(time.Duration(sec * float64(time.Second))).UnixNano()
}

// dropStats is two survivors of n1's crash, n3 leading after it. The map
// before the crash (1), the one that drops n1 (2), a later one that names it
// again (3) and one that drops it a second time (4) are all in the timelines,
// so the pick is the first map to drop it and not merely one that does.
func dropStats() map[string]broker.StormStats {
	survivor := func(declared, mapApplied, reconciled, adopted, proposed, forgot float64, held map[string]float64, leader bool) broker.StormStats {
		tl := lease.NodeLiveness{LastAliveAppliedNs: at(-3.2), DeclarationAppliedNs: at(declared)}
		if leader {
			tl.DeclarationProposedNs = at(29.40)
		}
		ev2 := replication.MapEvent{Version: 2, Basis: []string{"n2", "n3"}, AppliedNs: at(mapApplied), ReconciledNs: at(reconciled)}
		ev4 := replication.MapEvent{Version: 4, Basis: []string{"n2", "n3"}, AppliedNs: at(55), ReconciledNs: at(55.1)}
		if leader {
			ev2.Cause, ev2.DecidedNs, ev2.ProposedNs = "declaration", at(29.43), at(29.44)
			ev4.Cause, ev4.DecidedNs, ev4.ProposedNs = "tick", at(54.9), at(54.95)
		}
		heldNs := map[string]int64{}
		for why, s := range held {
			heldNs[why] = at(s)
		}
		return broker.StormStats{
			Peers: []transport.PeerInfo{{Name: "n1", State: transport.Down, DownForMs: 59_990}},
			Lease: &lease.LivenessStats{Timeline: map[string]lease.NodeLiveness{"n1": tl}},
			ReplicationMaps: &replication.MapTimeline{Events: []replication.MapEvent{
				{Version: 1, Basis: []string{"n1", "n2", "n3"}, AppliedNs: at(-100)},
				ev2,
				{Version: 3, Basis: []string{"n1", "n2", "n3"}, AppliedNs: at(50)},
				ev4,
			}},
			RoutingForgets: []routing.ForgetEvent{{Owner: "n1", HiddenNs: at(0.4), ServedHereNs: at(adopted), ProposedNs: at(proposed), ForgottenNs: at(forgot), LastHeldNs: heldNs}},
			OwnerKnowledge: []routing.Episode{{Start: time.Unix(0, at(-200)), End: time.Unix(0, at(-199))}},
		}
	}
	stats := map[string]broker.StormStats{
		"n2": survivor(29.43, 29.46, 29.70, 32.95, 33.00, 33.10, map[string]float64{"leases": 29.0, "adopting": 32.9}, false),
		"n3": survivor(29.42, 29.45, 29.60, 29.50, 29.55, 33.00, map[string]float64{"unsynced": 32.5}, true),
	}
	// n2 took sessions in transit and committed a cover; n3 took none.
	stats["n2"].RoutingForgets[0].CoveredNs = at(32.97)
	return stats
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func TestDropTimelineFromTheSurvivorsStats(t *testing.T) {
	d := newDropTimeline(dropCrash, dropCrash.Add(60*time.Second), "n1", "n3", dropStats(), dropCrash.Add(33500*time.Millisecond), true)
	if len(d.Missing) != 0 {
		t.Fatalf("missing %v with every figure present", d.Missing)
	}
	checks := []struct {
		name      string
		got, want float64
	}{
		{"peer down on n2", d.PeerDownSec["n2"], 0.01},
		{"last renewal", *d.LastRenewalSec, -3.2},
		{"declaration proposed", *d.DeclarationProposedSec, 29.40},
		{"declaration applied on n2", d.DeclarationAppliedSec["n2"], 29.43},
		{"map decided", *d.MapDecidedSec, 29.43},
		{"map applied on n2", d.MapAppliedSec["n2"], 29.46},
		{"map reconciled on n3", d.MapReconciledSec["n3"], 29.60},
		{"adoptions finished on n2", d.AdoptedSec["n2"], 32.95},
		{"fact committed on n3", d.ProposedSec["n3"], 29.55},
		{"cover committed on n2", d.CoverSec["n2"], 32.97},
		{"forget on n2", d.ForgetSec["n2"], 33.10},
		{"instant", *d.InstantSec, 33.5},
	}
	for _, c := range checks {
		if !near(c.got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, c.got, c.want)
		}
	}
	if _, ok := d.CoverSec["n3"]; ok {
		t.Errorf("a cover read for n3, which committed none: %v", d.CoverSec)
	}
	if d.MapVersion != 2 || d.MapCause != "declaration" {
		t.Errorf("map %d on a %q check; want the first map leaving n1 out (2), decided on the declaration", d.MapVersion, d.MapCause)
	}
	if d.ForgetLastHeld["n2"] != "adopting" || d.ForgetLastHeld["n3"] != "unsynced" {
		t.Errorf("last held %v; want n2 adopting, n3 unsynced", d.ForgetLastHeld)
	}
	if got := dropOrder(&d); got != "dropped on every survivor 4.04 s before the instant" {
		t.Errorf("drop order %q", got)
	}
	if got := servedLine(&d); got != "last adoptions finished 0.55 s before the instant; last forget 0.40 s before the instant, 0.15 s after them" {
		t.Errorf("served reading %q", got)
	}
	late := d
	late.InstantSec = new(float64)
	*late.InstantSec = 33.0
	if got := servedLine(&late); got != "last adoptions finished 0.05 s before the instant; last forget 0.10 s **after** the instant, 0.15 s after them" {
		t.Errorf("served reading with the forget after the instant %q", got)
	}
}

// TestDropTimelineNamesWhatIsMissing: an absent figure is named and left out,
// never read as zero seconds after the crash.
func TestDropTimelineNamesWhatIsMissing(t *testing.T) {
	stats := dropStats()
	n2 := stats["n2"]
	n2.ReplicationMaps = &replication.MapTimeline{Events: []replication.MapEvent{{Version: 1, Basis: []string{"n1", "n2", "n3"}, AppliedNs: at(-100)}}}
	n2.RoutingForgets = []routing.ForgetEvent{{Owner: "n1", HiddenNs: at(0.4)}}
	stats["n2"] = n2
	d := newDropTimeline(dropCrash, dropCrash.Add(60*time.Second), "n1", "", stats, time.Time{}, false)
	for _, want := range []string{"the post-crash leader's lease timeline", "n2: map 2 not applied", "n2: its adoptions not seen finished for n1", "n2: n1's routes not forgotten", "no recovery instant"} {
		if !strings.Contains(strings.Join(d.Missing, "\n"), want) {
			t.Errorf("missing %v does not name %q", d.Missing, want)
		}
	}
	if d.LastRenewalSec != nil || d.DeclarationProposedSec != nil || d.InstantSec != nil {
		t.Errorf("an unread figure was filled in: %+v", d)
	}
	if _, ok := d.MapAppliedSec["n2"]; ok {
		t.Error("n2's map apply was filled in")
	}
	if got := dropOrder(&d); got != "not dropped on every survivor in the window" {
		t.Errorf("drop order %q with the map applied on one survivor of two", got)
	}

	none := newDropTimeline(dropCrash, dropCrash.Add(60*time.Second), "n1", "n3", map[string]broker.StormStats{"n2": {}, "n3": {}}, dropCrash, true)
	if none.MapVersion != 0 || !strings.Contains(strings.Join(none.Missing, "\n"), "no map leaving n1 out applied after the crash") {
		t.Errorf("stats with no timelines: map %d, missing %v", none.MapVersion, none.Missing)
	}
	if _, ok := none.DegradedSec["n2"]; ok || !strings.Contains(strings.Join(none.Missing, "\n"), "n2: no owner-knowledge record") {
		t.Errorf("stats with no owner-knowledge record: degraded %v, missing %v", none.DegradedSec, none.Missing)
	}
}

// TestDropAndRefusalSourceSections: the two report-only sections render from
// run.json, a run from before the timeline existed reads as not recorded, and
// the source split counts only what rose after the instant.
func TestDropAndRefusalSourceSections(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, v any) {
		d := filepath.Join(dir, name)
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "run.json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ins := balanced()
	for i := range ins[:2] {
		ins[i].Probe.RefusedAfterRecovery = map[string]int64{"0x83": 90}
	}
	drop := newDropTimeline(dropCrash, dropCrash.Add(60*time.Second), "n1", "n3", dropStats(), dropCrash.Add(33500*time.Millisecond), true)
	with := map[string]any{}
	raw, _ := json.Marshal(artifactFor(ins[0]))
	if err := json.Unmarshal(raw, &with); err != nil {
		t.Fatal(err)
	}
	with["victim"] = "n1"
	with["drop_timeline"] = drop
	with["metrics_at_instant"] = map[string]map[string]float64{
		"n2": {
			`hoardmq_replication_barrier_failures_total{peer="n1",reason="refused"}`:  10,
			`hoardmq_replication_barrier_failures_total{peer="n3",reason="unscoped"}`: 0,
			`hoardmq_replication_barrier_failures_total{peer="n3",reason="timeout"}`:  9,
			`hoardmq_forward_failures_total{reason="subscriber-node-down"}`:           1,
			`hoardmq_lease_declarations_total`:                                        0,
		},
	}
	with["metrics_after"] = map[string]map[string]float64{
		"n2": {
			`hoardmq_replication_barrier_failures_total{peer="n1",reason="refused"}`:  15,
			`hoardmq_replication_barrier_failures_total{peer="n3",reason="unscoped"}`: 7,
			`hoardmq_replication_barrier_failures_total{peer="n3",reason="timeout"}`:  4,
			`hoardmq_forward_failures_total{reason="subscriber-node-down"}`:           3,
			`hoardmq_lease_declarations_total`:                                        1,
			`hoardmq_lease_proposals_total{op="served"}`:                              1,
			`hoardmq_lease_proposals_total{op="acquire"}`:                             40,
		},
		"n3": {
			`hoardmq_lease_proposals_total{op="served"}`: 2,
		},
	}
	write("r1", with)
	write("r2", artifactFor(ins[1]))
	var out strings.Builder
	JudgeRunFiles(&out, []string{dir})
	s := out.String()
	for _, want := range []string{
		"## The crashed node's drop from the shard map, and the forget, against the recovery instant",
		"| r1 | n3 | -3.20 | 29.40 | n2 29.43, n3 29.42 | 29.43 (declaration) | n2 29.46, n3 29.45 | n2 33.10 (adopting), n3 33.00 (unsynced) | 33.50 | dropped on every survivor 4.04 s before the instant |",
		"| r2 | — | — | — | — | — | — | — | — | not recorded in this run.json |",
		"## Refusals after the recovery instant, by source",
		"| r1 | 90 | 5 (refused 5) | 7 (unscoped 7) | 2 (subscriber-node-down 2) |",
		"## Adoptions finished, served facts, and the forget, against the recovery instant",
		"| r1 | n2 32.95, n3 29.50 | n2 33.00, n3 29.55 | n2 33.10 (adopting), n3 33.00 (unsynced) | 33.50 | 3 (n2 1, n3 2) | last adoptions finished 0.55 s before the instant; last forget 0.40 s before the instant, 0.15 s after them |",
		"| r2 | 90 | not split",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("output lacks %q:\n%s", want, s)
		}
	}
}

// TestDegradedSecondsOverlapTheWindow (WI 1571): only the part of an
// episode between the crash mark and the read counts, an open one runs to
// the read, and a full list starting after the mark is named as cut.
func TestDegradedSecondsOverlapTheWindow(t *testing.T) {
	crash, read := dropCrash, dropCrash.Add(60*time.Second)
	ep := func(start, end float64) routing.Episode {
		e := routing.Episode{Start: time.Unix(0, at(start))}
		if end != 0 {
			e.End = time.Unix(0, at(end))
		}
		return e
	}
	cases := []struct {
		name     string
		episodes []routing.Episode
		want     float64
	}{
		{"none", []routing.Episode{}, 0},
		{"before the crash only", []routing.Episode{ep(-10, -5)}, 0},
		{"across the crash", []routing.Episode{ep(-10, 2.5)}, 2.5},
		{"inside, then open at the read", []routing.Episode{ep(1, 3), ep(50, 0)}, 12},
	}
	for _, c := range cases {
		sec, cut := unknownWithin(c.episodes, crash, read)
		if !near(sec, c.want) || cut {
			t.Errorf("%s: %v s (cut %v), want %v", c.name, sec, cut, c.want)
		}
	}
	full := make([]routing.Episode, routing.MaxEpisodes)
	for i := range full {
		full[i] = ep(float64(i+1), float64(i+1)+0.5)
	}
	if _, cut := unknownWithin(full, crash, read); !cut {
		t.Error("a full list starting after the crash is not reported cut")
	}

	stats := dropStats()
	n3 := stats["n3"]
	n3.OwnerKnowledge = []routing.Episode{ep(0.4, 1.4)}
	stats["n3"] = n3
	d := newDropTimeline(crash, read, "n1", "n3", stats, crash.Add(33500*time.Millisecond), true)
	if !near(d.DegradedSec["n3"], 1) || d.DegradedSec["n2"] != 0 {
		t.Errorf("degraded seconds %v; want n3 1, n2 0", d.DegradedSec)
	}
}
