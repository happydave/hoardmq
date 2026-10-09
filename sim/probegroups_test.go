package sim

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

func byAddress(addr string) string { return "node@" + addr }

// G1: each class of group outcome, from synthetic deliveries.
func TestTallyGroupClassifies(t *testing.T) {
	acked := func(seqs ...uint64) map[uint64]bool {
		m := map[uint64]bool{}
		for _, s := range seqs {
			m[s] = true
		}
		return m
	}
	d := func(member string, seq uint64, mod func(*Delivery)) Delivery {
		x := Delivery{Client: member, Node: member + ":1", Seq: seq}
		if mod != nil {
			mod(&x)
		}
		return x
	}
	same := func(x *Delivery) { x.SameExchange = true }
	after := func(x *Delivery) { x.AfterComplete = true }
	cases := []struct {
		name                                  string
		qos                                   byte
		acked                                 map[uint64]bool
		deliveries                            []Delivery
		undelivered, duplicates               int
		repeated, secondMember, deliveredOnly int
	}{
		{"one member has it", 1, acked(1), []Delivery{d("m0", 1, nil)}, 0, 0, 0, 0, 0},
		{"one member has it at QoS 2", 2, acked(1), []Delivery{d("m2", 1, nil)}, 0, 0, 0, 0, 0},
		{"no member has it", 1, acked(1, 2), []Delivery{d("m0", 1, nil)}, 1, 0, 0, 0, 0},
		{"QoS 2 on two members", 2, acked(1), []Delivery{d("m0", 1, nil), d("m1", 1, nil)}, 0, 1, 0, 0, 0},
		{"QoS 2 retransmitted in its exchange", 2, acked(1), []Delivery{d("m0", 1, nil), d("m0", 1, same)}, 0, 0, 0, 0, 0},
		{"QoS 2 after its exchange completed", 2, acked(1), []Delivery{d("m0", 1, nil), d("m0", 1, after)}, 0, 1, 0, 0, 0},
		{"QoS 1 twice on one member", 1, acked(1), []Delivery{d("m0", 1, nil), d("m0", 1, nil)}, 0, 0, 1, 0, 0},
		{"QoS 1 on two members", 1, acked(1), []Delivery{d("m0", 1, nil), d("m3", 1, nil)}, 0, 0, 1, 1, 0},
		{"a refused publish delivered", 1, acked(2), []Delivery{d("m0", 1, nil), d("m1", 2, nil)}, 0, 0, 0, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tallyGroup(tc.qos, tc.acked, tc.deliveries, byAddress)
			if len(got.Undelivered) != tc.undelivered || len(got.Duplicates) != tc.duplicates ||
				got.Repeated != tc.repeated || got.SecondMember != tc.secondMember || got.DeliveredUnacked != tc.deliveredOnly {
				t.Fatalf("undelivered %v, duplicates %v, repeated %d, second member %d, delivered unacked %d; want %d, %d, %d, %d, %d",
					got.Undelivered, got.Duplicates, got.Repeated, got.SecondMember, got.DeliveredUnacked,
					tc.undelivered, tc.duplicates, tc.repeated, tc.secondMember, tc.deliveredOnly)
			}
			if got.Acked != len(tc.acked) {
				t.Fatalf("acked %d, want %d", got.Acked, len(tc.acked))
			}
			if total := sumByNode(got.ByNode); total != len(tc.deliveries) {
				t.Fatalf("by node %v sums to %d, want %d deliveries", got.ByNode, total, len(tc.deliveries))
			}
		})
	}
}

func sumByNode(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// relaysHeld sums hoardmq_group_gives_relayed_total{outcome="held"} over the
// running nodes.
func relaysHeld(t *testing.T, c *Cluster) float64 {
	t.Helper()
	var n float64
	for _, nd := range c.Running() {
		snap, ok := nd.Server().Registry().Metrics().(interface{ Snapshot() []module.Metric })
		if !ok {
			t.Fatalf("%s: no metrics snapshot", nd.Name)
		}
		n += pickStormMetrics(snap.Snapshot())[`hoardmq_group_gives_relayed_total{outcome="held"}`]
	}
	return n
}

// G2: on a live cluster with no fault, the groups have members on every node,
// every acknowledged message reaches exactly one member, each group's
// messages land on more than one member node, and the owners relay gives
// made to a third node.
func TestProbeGroupsSpanNodesAndRelay(t *testing.T) {
	c := New(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	p, err := newProbe(ctx, 1, 2, 100*time.Millisecond, c)
	defer p.close()
	if err != nil {
		t.Fatal(err)
	}
	p.drain = 2 * time.Second
	p.start(ctx)
	time.Sleep(3 * time.Second)
	rec := p.result(time.Now(), time.Time{})
	g := rec.Groups
	if raw, err := json.Marshal(g); err == nil {
		t.Logf("groups: %s", raw)
	}
	if g == nil || g.Groups != 2 || g.MembersPerGroup != 4 {
		t.Fatalf("groups record %+v, want 2 groups of 4", g)
	}
	names := []string{c.Nodes[0].Name, c.Nodes[1].Name, c.Nodes[2].Name}
	for k, s := range g.Start {
		if !sameCounts(s.Members, probeGroupWant(k, names)) {
			t.Errorf("%s: members at the start %v, want %v", s.Key, s.Members, probeGroupWant(k, names))
		}
		if s.Owner != names[k%3] || s.PublisherNode != names[(k+1)%3] || s.QoS != probeGroupQoS(k) {
			t.Errorf("%s: owner %s, publisher on %s, QoS %d", s.Key, s.Owner, s.PublisherNode, s.QoS)
		}
	}
	if g.Acked == 0 {
		t.Fatalf("no group publish was acknowledged: %+v", g)
	}
	if g.AckedNotDelivered != 0 || g.QoS2Duplicates != 0 || g.QoS1SecondMember != 0 {
		t.Fatalf("undelivered %d %v, QoS 2 duplicates %d %v, QoS 1 on a second member %d",
			g.AckedNotDelivered, g.Examples, g.QoS2Duplicates, g.DuplicateExamples, g.QoS1SecondMember)
	}
	for _, f := range g.PerGroup {
		if f.Acked == 0 || len(f.DeliveredByNode) < 2 {
			t.Errorf("%s: %d acknowledged, delivered by node %v: want deliveries on two member nodes or more", f.Key, f.Acked, f.DeliveredByNode)
		}
	}
	if held := relaysHeld(t, c); held < 1 {
		t.Fatalf("no give to a third node was relayed held (%v)", held)
	}
	p.close()
	for _, pg := range p.groups {
		for _, cl := range append([]*Client{pg.pub}, pg.members...) {
			if cl.Online() {
				t.Errorf("%s is still online after the probe closed", cl.ID)
			}
		}
	}
}

// G3: a group whose members do not span the nodes is not published into; the
// build fails naming the group and what each node's table showed.
func TestProbeGroupRouteWaitNamesWhatItSaw(t *testing.T) {
	c := New(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	g := &probeGroup{k: 0, qos: 1}
	for j, addr := range c.Addresses()[:2] {
		m := NewClient("probe-g00-m"+string(rune('0'+j)), []string{addr}, 60, int64(j))
		if err := m.Run(ctx); err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		if err := m.Subscribe(ctx, probeGroupFilter(0), 1); err != nil {
			t.Fatal(err)
		}
	}
	names := []string{c.Nodes[0].Name, c.Nodes[1].Name, c.Nodes[2].Name}
	err := waitGroupRoutes(ctx, c, []*probeGroup{g}, names, time.Second)
	if err == nil {
		t.Fatal("a group on two nodes passed the wait")
	}
	if !strings.Contains(err.Error(), probeGroupFilter(0)) || !strings.Contains(err.Error(), "sees map[") {
		t.Fatalf("the error does not name the group and what was seen: %v", err)
	}
}

// G4: the report row carries the groups' clause, and leaves it out when no
// group ran.
func TestProbeLineCarriesTheGroups(t *testing.T) {
	base := probeRecord{Pairs: 2, RateMs: 1000, Attempts: 10, Acked: 10}
	if got := probeLine(&base); strings.Contains(got, "groups") {
		t.Fatalf("a probe without groups mentions them: %q", got)
	}
	withGroups := base
	withGroups.Groups = &groupsRecord{Groups: 6, MembersPerGroup: 4, Attempts: 60, Acked: 58,
		Start:                []groupStart{{QoS: 1}, {QoS: 2}},
		Refused:              map[string]int64{"0x83": 2},
		RefusedAfterRecovery: map[string]int64{"0x83": 1}, RefusedWithMembersBack: 1,
		AckedNotDelivered: 1, QoS2Duplicates: 1}
	got := probeLine(&withGroups)
	for _, want := range []string{"6 shared groups (1 at QoS 1, 1 at QoS 2)", "2 refused 0x83", "**1 after the recovery instant",
		"1 acknowledged and delivered to no member", "1 QoS 2 duplicates"} {
		if !strings.Contains(got, want) {
			t.Errorf("row lacks %q: %q", want, got)
		}
	}
}

// G5: the gating rows add the groups to the pairs, so a run whose pairs pass
// and whose groups lost or refused fails.
func TestBoundsCountTheGroups(t *testing.T) {
	in := inBounds()
	in.Probe.Groups = &groupsRecord{Acked: 600, RefusedAfterRecovery: map[string]int64{}}
	v := judgeRun(in)
	for _, name := range []string{rowUndelivered, rowRefusals} {
		if r := mustRow(t, v, name); r.Status != boundPass {
			t.Fatalf("%s: %s %q with nothing lost or refused", name, r.Status, r.Figure)
		}
	}
	if r := mustRow(t, v, rowUndelivered); r.Figure != "0 of 6500 acknowledged (pairs 0 of 5900, groups 0 of 600)" {
		t.Errorf("undelivered figure %q", r.Figure)
	}

	lost := inBounds()
	lost.Probe.Groups = &groupsRecord{Acked: 600, AckedNotDelivered: 1}
	r := mustRow(t, judgeRun(lost), rowUndelivered)
	if r.Status != boundFail || r.Figure != "1 of 6500 acknowledged (pairs 0 of 5900, groups 1 of 600)" {
		t.Errorf("a group loss: row %s %q, want fail", r.Status, r.Figure)
	}

	refused := inBounds()
	refused.Probe.Groups = &groupsRecord{Acked: 600, RefusedAfterRecovery: map[string]int64{"0x83": 2}, ErrorsAfterRecovery: 1}
	r = mustRow(t, judgeRun(refused), rowRefusals)
	if r.Status != boundFail || r.Figure != "2 (pairs 0, groups 2) (and 1 with no acknowledgement)" {
		t.Errorf("a group refusal after the instant: row %s %q, want fail", r.Status, r.Figure)
	}
}

// G6: failoverbounds prints the groups section for a run with groups, with
// the survivors' group forwards and relays over the run, and says when a run
// had none.
func TestJudgeRunFilesPrintsTheGroups(t *testing.T) {
	dir := t.TempDir()
	runs := balanced()[:2]
	runs[0].Probe.Groups = &groupsRecord{Groups: 6, Attempts: 600, Acked: 590, AckedNotDelivered: 1,
		Examples: []string{"probe-g01-p seq 7 on $share/pg01/probe/group/01"},
		Start:    []groupStart{{QoS: 1}, {QoS: 2}}, QoS2Duplicates: 0, QoS1Repeated: 3, QoS1SecondMember: 1,
		DeliveredByNode: map[string]int{"n2": 300, "n3": 289}}
	for i, in := range runs {
		art := artifactFor(in)
		if i == 0 {
			art.MetricsBefore = map[string]map[string]float64{"n2": {`hoardmq_group_gives_relayed_total{outcome="held"}`: 10}}
			art.MetricsAfter = map[string]map[string]float64{
				"n2": {`hoardmq_group_gives_relayed_total{outcome="held"}`: 50, `hoardmq_forwards_total{kind="group-give"}`: 70},
				"n3": {`hoardmq_group_gives_relayed_total{outcome="refused"}`: 2, `hoardmq_forwards_total{kind="group-decide"}`: 40},
			}
		}
		d := filepath.Join(dir, "r"+string(rune('1'+i)))
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(art)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "run.json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var out strings.Builder
	JudgeRunFiles(&out, []string{dir})
	text := out.String()
	for _, want := range []string{"## Shared groups",
		"| r1 | 6 (QoS 1: 1, QoS 2: 1) | 600 | 590 | 1 | 0 (0) | 0 | 3 (1) | n2 300, n3 289 | 40, 70, 40 / 2 |",
		"| r2 | no groups in this run |", "r1: delivered to no member: probe-g01-p seq 7"} {
		if !strings.Contains(text, want) {
			t.Errorf("output lacks %q:\n%s", want, text)
		}
	}
}

// newTestGroupProbe builds a probe with one group and no broker behind it:
// the clients exist so their identifiers and delivery records can be driven.
func newTestGroupProbe(qos byte) *probe {
	p := newTestProbeNoT(0)
	g := &probeGroup{k: 1, qos: qos, pub: NewClient("probe-g01-p", []string{"127.0.0.1:1"}, 60, 1),
		start: groupStart{Key: probeGroupFilter(1), QoS: qos}}
	for j := 0; j < 2; j++ {
		g.members = append(g.members, NewClient("probe-g01-m"+string(rune('0'+j)), []string{"127.0.0.1:1"}, 60, int64(j)))
	}
	p.groups = []*probeGroup{g}
	p.groupCounts.acked[1] = map[uint64]bool{}
	return p
}

func newTestProbeNoT(pairs int) *probe {
	return &probe{pairs: pairs, rate: time.Second, stop: make(chan struct{}),
		acked: map[string]map[uint64]bool{}, refused: map[string]int64{},
		groupCounts: groupCounts{acked: map[int]map[uint64]bool{}, refused: map[string]int64{}},
		nodeName:    func(a string) string { return a }}
}

// G7: a group publish's outcomes are recorded as a pair's are, No Matching
// Subscribers included among the acknowledged, and the record splits the
// refusals at the instant and names an undelivered message.
func TestProbeGroupsRecordAndSplit(t *testing.T) {
	crash := time.Now()
	instant := crash.Add(10 * time.Second)
	p := newTestGroupProbe(1)
	p.recordGroup(1, 1, packet.Success, nil, true, crash.Add(time.Second))
	p.recordGroup(1, 2, packet.NoMatchingSubscribers, nil, true, crash.Add(2*time.Second))
	p.recordGroup(1, 3, packet.ImplementationSpecificError, errors.New("refused"), false, crash.Add(3*time.Second))
	p.recordGroup(1, 4, packet.ImplementationSpecificError, errors.New("refused"), true, crash.Add(12*time.Second))
	p.recordGroup(1, 5, 0, errors.New("connection ended"), true, crash.Add(13*time.Second))
	p.recordGroup(1, 6, packet.Success, nil, true, crash.Add(14*time.Second))
	p.groups[0].members[0].deliveries = []Delivery{{Client: "probe-g01-m0", Publisher: "probe-g01-p", Seq: 1}}
	p.groups[0].members[1].deliveries = []Delivery{{Client: "probe-g01-m1", Publisher: "probe-g01-p", Seq: 2},
		{Client: "probe-g01-m1", Publisher: "someone-else", Seq: 6}}
	g := p.result(crash, instant).Groups
	if g == nil {
		t.Fatal("no groups record")
	}
	code := packet.ImplementationSpecificError.String()
	if g.Attempts != 6 || g.Acked != 3 || g.NoSubscribers != 1 || g.Refused[code] != 2 || g.Errors != 1 {
		t.Fatalf("attempts %d acked %d no subscribers %d refused %v errors %d; want 6, 3, 1, 2, 1",
			g.Attempts, g.Acked, g.NoSubscribers, g.Refused, g.Errors)
	}
	if g.RefusedAfterRecovery[code] != 1 || g.ErrorsAfterRecovery != 1 || g.RefusedWithMembersBack != 1 {
		t.Fatalf("after the instant: refused %v errors %d with members back %d; want 1, 1, 1",
			g.RefusedAfterRecovery, g.ErrorsAfterRecovery, g.RefusedWithMembersBack)
	}
	if g.LastRefusalSec < 11.9 || g.LastRefusalSec > 12.1 {
		t.Fatalf("last refusal at %v, want about 12", g.LastRefusalSec)
	}
	if g.AckedNotDelivered != 1 || len(g.Examples) != 1 || !strings.Contains(g.Examples[0], "seq 6") {
		t.Fatalf("undelivered %d %v; want seq 6 alone (another publisher's seq 6 is not the group's)", g.AckedNotDelivered, g.Examples)
	}
	if len(g.PerGroup) != 1 || g.PerGroup[0].AckedNotDelivered != 1 || g.PerGroup[0].Key != probeGroupFilter(1) {
		t.Fatalf("per group %+v", g.PerGroup)
	}
}

// G8: the start-time socket limit counts the groups' clients, and only when
// the probe runs.
func TestProbeClientsCountsTheGroups(t *testing.T) {
	if got := probeClients(100, 6, 3); got != 230 {
		t.Fatalf("100 pairs and 6 groups on 3 nodes: %d clients, want 230", got)
	}
	if got := probeClients(0, 6, 3); got != 0 {
		t.Fatalf("no probe: %d clients, want 0", got)
	}
}

// G9: HOARDMQ_SIM_PROBE_GROUPS sets the group count; zero disables them and a
// value that does not parse is the default.
func TestProbeGroupCountReadsItsVariable(t *testing.T) {
	for v, want := range map[string]int{"": probeDefaultGroups, "0": 0, "3": 3, "x": probeDefaultGroups, "-2": probeDefaultGroups} {
		t.Setenv("HOARDMQ_SIM_PROBE_GROUPS", v)
		if got := probeGroupCount(); got != want {
			t.Errorf("%q: %d groups, want %d", v, got, want)
		}
	}
}

// G10: a group's members are back only when every one of them is recovered.
func TestProbeGroupMembersBack(t *testing.T) {
	g := newTestGroupProbe(1).groups[0]
	set := func(c *Client, back bool) {
		c.online.Store(back)
		c.recovered.Store(back)
	}
	set(g.members[0], true)
	set(g.members[1], true)
	if !g.membersBack() {
		t.Fatal("every member recovered, and the group reads not back")
	}
	set(g.members[1], false)
	if g.membersBack() {
		t.Fatal("a member not recovered, and the group reads back")
	}
}

// G11: the pairs' figures count only the pairs when groups ran beside them,
// so a record from a build with groups reads as one from a build without.
func TestProbePairsFiguresExcludeTheGroups(t *testing.T) {
	crash := time.Now()
	p := newTestGroupProbe(1)
	pub := NewClient("probe-p0000", []string{"127.0.0.1:1"}, 60, 9)
	sub := NewClient("probe-s0000", []string{"127.0.0.1:1"}, 60, 10)
	p.pairs, p.pubs, p.subs = 1, []*Client{pub}, []*Client{sub}
	p.acked[pub.ID] = map[uint64]bool{1: true}
	p.attempts = 1
	sub.deliveries = []Delivery{{Client: sub.ID, Publisher: pub.ID, Seq: 1, Topic: probeTopic(0)}}
	p.recordGroup(1, 1, packet.Success, nil, true, crash.Add(time.Second))
	p.recordGroup(1, 2, packet.Success, nil, true, crash.Add(2*time.Second))
	p.recordGroup(1, 3, packet.ImplementationSpecificError, errors.New("refused"), true, crash.Add(12*time.Second))
	rec := p.result(crash, crash.Add(10*time.Second))
	if rec.Attempts != 1 || rec.Acked != 1 || rec.AckedNotDelivered != 0 || len(rec.Refused) != 0 || len(rec.RefusedAfterRecovery) != 0 {
		t.Fatalf("pairs: attempts %d acked %d undelivered %d refused %v after %v; want 1, 1, 0, none, none",
			rec.Attempts, rec.Acked, rec.AckedNotDelivered, rec.Refused, rec.RefusedAfterRecovery)
	}
	if g := rec.Groups; g == nil || g.Acked != 2 || g.AckedNotDelivered != 2 || sumCounts(g.RefusedAfterRecovery) != 1 {
		t.Fatalf("groups: %+v; want 2 acknowledged, 2 undelivered, 1 refusal after the instant", g)
	}
}
