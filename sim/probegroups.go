package sim

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// The probe's shared groups (WI 1962). Each group has a member on every node
// and a second on one of them, which makes that node the group's dispatch
// owner, and one publisher on another node, so the owner's round robin gives
// to the third node about one message in three: WI 1896's relay. After a
// node is lost the group's members and ownership move with the survivors.
//
// A group message is delivered when one member has it. The pairs' figures are
// kept apart from the groups' and the gating rows add the two.

const (
	probeDefaultGroups = 6
	// probeGroupRouteWait bounds the wait for every node's routing table to
	// show each group's members where they connected.
	probeGroupRouteWait = 30 * time.Second
)

// probeGroupCount is how many groups the probe runs: the default, or
// HOARDMQ_SIM_PROBE_GROUPS. Zero disables them.
func probeGroupCount() int {
	if v := os.Getenv("HOARDMQ_SIM_PROBE_GROUPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return probeDefaultGroups
}

// probeClients is how many clients the probe opens beside the population.
func probeClients(pairs, groups, nodes int) int {
	if pairs == 0 {
		return 0
	}
	return 2*pairs + groups*(nodes+2)
}

func probeGroupName(k int) string   { return fmt.Sprintf("pg%02d", k) }
func probeGroupTopic(k int) string  { return fmt.Sprintf("probe/group/%02d", k) }
func probeGroupFilter(k int) string { return "$share/" + probeGroupName(k) + "/" + probeGroupTopic(k) }

// probeGroupQoS alternates, so each owner node has a group at each QoS.
func probeGroupQoS(k int) byte {
	if k%2 == 0 {
		return 1
	}
	return 2
}

// probeGroupWant is the member count per node group k has once every member
// is subscribed: one on each node, and node k mod n holds two.
func probeGroupWant(k int, names []string) map[string]int {
	want := map[string]int{}
	for _, n := range names {
		want[n] = 1
	}
	want[names[k%len(names)]]++
	return want
}

// probeGroup is one shared group with its members and publisher.
type probeGroup struct {
	k       int
	qos     byte
	members []*Client
	pub     *Client
	start   groupStart
}

// groupStart is a group as the routing table showed it when publishing began.
type groupStart struct {
	Key           string         `json:"key"`
	QoS           byte           `json:"qos"`
	Members       map[string]int `json:"members"`
	Owner         string         `json:"owner"`
	PublisherNode string         `json:"publisher_node"`
}

// membersBack reports whether every member of the group is recovered.
func (g *probeGroup) membersBack() bool {
	for _, m := range g.members {
		if !m.Recovered() {
			return false
		}
	}
	return true
}

// groupCounts is what the groups' publishers saw, under the probe's mutex.
type groupCounts struct {
	attempts int64
	acked    map[int]map[uint64]bool // group -> acknowledged sequences
	noSubs   int64
	refused  map[string]int64
	errs     int64
	bad      []probeEvent
}

// groupsRecord is the groups' part of the probe's record. Its figures are
// read against Attempts and the start records, as the pairs' are.
type groupsRecord struct {
	Groups               int              `json:"groups"`
	MembersPerGroup      int              `json:"members_per_group"`
	Start                []groupStart     `json:"start"`
	Attempts             int64            `json:"attempts"`
	Acked                int64            `json:"acked"`
	NoSubscribers        int64            `json:"no_subscribers"`
	Refused              map[string]int64 `json:"refused"`
	Errors               int64            `json:"errors"`
	LastRefusalSec       float64          `json:"last_refusal_sec"`
	RefusedAfterRecovery map[string]int64 `json:"refused_after_recovery"`
	ErrorsAfterRecovery  int64            `json:"errors_after_recovery"`
	// RefusedWithMembersBack counts the refusals after the recovery instant
	// whose group had every member recovered at the time.
	RefusedWithMembersBack int64 `json:"refused_with_members_back"`
	MembersRecovered       int   `json:"members_recovered"`

	AckedNotDelivered int      `json:"acked_not_delivered"`
	Examples          []string `json:"examples,omitempty"`
	// QoS2Duplicates counts QoS 2 messages received as more than one exchange,
	// on one member or across members.
	QoS2Duplicates    int      `json:"qos2_duplicates"`
	DuplicateExamples []string `json:"duplicate_examples,omitempty"`
	// QoS1Repeated counts QoS 1 messages received more than once, and
	// QoS1SecondMember those of them received by more than one member.
	QoS1Repeated     int `json:"qos1_repeated"`
	QoS1SecondMember int `json:"qos1_second_member"`
	// DeliveredUnacked counts messages received whose publish was not
	// acknowledged with Success.
	DeliveredUnacked int            `json:"delivered_unacked"`
	DeliveredByNode  map[string]int `json:"delivered_by_node"`
	// PerGroup is the same figures for each group, so a loss is placed in
	// its group.
	PerGroup []groupFigures `json:"per_group"`
}

// groupFigures is one group's share of the record.
type groupFigures struct {
	Key               string         `json:"key"`
	Acked             int            `json:"acked"`
	AckedNotDelivered int            `json:"acked_not_delivered"`
	QoS2Duplicates    int            `json:"qos2_duplicates"`
	QoS1Repeated      int            `json:"qos1_repeated"`
	QoS1SecondMember  int            `json:"qos1_second_member"`
	DeliveredByNode   map[string]int `json:"delivered_by_node"`
}

// newProbeGroups connects n groups' members and publishers, then waits until
// every running node's routing table shows each group's members where they
// connected. The groups are returned whatever happens, so the caller can
// close what was connected.
func newProbeGroups(ctx context.Context, n int, c *Cluster) ([]*probeGroup, error) {
	addrs := c.Addresses()
	names := make([]string, len(c.Nodes))
	for i, nd := range c.Nodes {
		names[i] = nd.Name
	}
	order := func(first int) []string {
		out := append([]string(nil), addrs[first%len(addrs):]...)
		return append(out, addrs[:first%len(addrs)]...)
	}
	var groups []*probeGroup
	for k := 0; k < n; k++ {
		g := &probeGroup{k: k, qos: probeGroupQoS(k)}
		groups = append(groups, g)
		firsts := make([]int, 0, len(addrs)+1)
		for j := range addrs {
			firsts = append(firsts, j)
		}
		firsts = append(firsts, k%len(addrs))
		for j, first := range firsts {
			m := NewClient(fmt.Sprintf("probe-g%02d-m%d", k, j), order(first), 3600, int64(3_000_000+100*k+j))
			g.members = append(g.members, m)
			if err := m.Run(ctx); err != nil {
				return groups, fmt.Errorf("%s: %w", m.ID, err)
			}
			if err := m.Subscribe(ctx, probeGroupFilter(k), g.qos); err != nil {
				return groups, fmt.Errorf("%s: %w", m.ID, err)
			}
		}
		pubFirst := (k + 1) % len(addrs)
		g.pub = NewClient(fmt.Sprintf("probe-g%02d-p", k), order(pubFirst), 3600, int64(3_000_000+100*k+99))
		if err := g.pub.Run(ctx); err != nil {
			return groups, fmt.Errorf("%s: %w", g.pub.ID, err)
		}
		g.start = groupStart{Key: probeGroupFilter(k), QoS: g.qos, PublisherNode: names[pubFirst]}
	}
	if err := waitGroupRoutes(ctx, c, groups, names, probeGroupRouteWait); err != nil {
		return groups, err
	}
	return groups, nil
}

// waitGroupRoutes waits until every running node's table shows each group
// with the member counts probeGroupWant gives, and records the counts the
// first running node shows and the owner they elect.
func waitGroupRoutes(ctx context.Context, c *Cluster, groups []*probeGroup, names []string, bound time.Duration) error {
	deadline := time.Now().Add(bound)
	for {
		seen, ok := groupRoutesSeen(c, groups, names)
		if ok {
			for _, g := range groups {
				for _, n := range c.Running() {
					g.start.Members = seen[n.Name][g.k]
					break
				}
				g.start.Owner = ownerOf(g.start.Members)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("probe groups: the routing tables did not show every group's members within %s: %s", bound, describeGroupRoutes(seen, groups, names))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("probe groups: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// groupRoutesSeen reads each running node's member counts for every group,
// and whether all of them match.
func groupRoutesSeen(c *Cluster, groups []*probeGroup, names []string) (map[string]map[int]map[string]int, bool) {
	seen := map[string]map[int]map[string]int{}
	ok := true
	for _, n := range c.Running() {
		srv := n.Server()
		if srv == nil || srv.Routes() == nil {
			ok = false
			continue
		}
		seen[n.Name] = map[int]map[string]int{}
		for _, g := range groups {
			_, matched := srv.Routes().Table().Match("default", probeGroupTopic(g.k))
			var members map[string]int
			for _, mg := range matched {
				if mg.Key == probeGroupFilter(g.k) {
					members = map[string]int{}
					for node, count := range mg.Members {
						if count > 0 {
							members[node] = count
						}
					}
				}
			}
			seen[n.Name][g.k] = members
			if !sameCounts(members, probeGroupWant(g.k, names)) {
				ok = false
			}
		}
	}
	return seen, ok
}

func sameCounts(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// ownerOf elects as the routing table does: most members, ties to the
// lowest name.
func ownerOf(members map[string]int) string {
	best, bestCount := "", -1
	for node, count := range members {
		if count > bestCount || (count == bestCount && node < best) {
			best, bestCount = node, count
		}
	}
	return best
}

func describeGroupRoutes(seen map[string]map[int]map[string]int, groups []*probeGroup, names []string) string {
	nodes := make([]string, 0, len(seen))
	for n := range seen {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	out := ""
	for _, g := range groups {
		out += fmt.Sprintf("%s want %v:", probeGroupFilter(g.k), probeGroupWant(g.k, names))
		for _, n := range nodes {
			out += fmt.Sprintf(" %s sees %v", n, seen[n][g.k])
		}
		out += "; "
	}
	return out
}

// publishGroup sends one message into a group and records the outcome as
// publish does for a pair.
func (p *probe) publishGroup(ctx context.Context, g *probeGroup, seq uint64) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	code, err := g.pub.Publish(ctx, probeGroupTopic(g.k), g.qos, seq)
	p.recordGroup(g.k, seq, code, err, g.membersBack(), time.Now())
}

// recordGroup counts one group publish's outcome.
func (p *probe) recordGroup(k int, seq uint64, code packet.ReasonCode, err error, back bool, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	gc := &p.groupCounts
	gc.attempts++
	p.noteOutcome(true, code, err, at)
	switch {
	case err != nil && code >= 0x80:
		gc.refused[code.String()]++
		gc.bad = append(gc.bad, probeEvent{at: at, code: code.String(), subBack: back})
	case err != nil:
		gc.errs++
		gc.bad = append(gc.bad, probeEvent{at: at, subBack: back})
	case code == packet.NoMatchingSubscribers:
		// An acknowledgement all the same, and a group decided on another
		// node is answered with it while a member holds the message.
		gc.noSubs++
		gc.acked[k][seq] = true
	default:
		gc.acked[k][seq] = true
	}
}

// groupTally is what one group's members received, against what its
// publisher had acknowledged.
type groupTally struct {
	Acked            int
	Undelivered      []uint64
	Duplicates       []uint64
	Repeated         int
	SecondMember     int
	DeliveredUnacked int
	ByNode           map[string]int
}

// tallyGroup classifies a group's messages from its members' deliveries. A
// message is delivered when one member has it. At QoS 2 an exchange is a
// delivery not marked SameExchange, and a message received as more than one
// exchange is a duplicate. At QoS 1 a repeat is counted and judged by nothing.
func tallyGroup(qos byte, acked map[uint64]bool, deliveries []Delivery, nodeName func(string) string) groupTally {
	t := groupTally{Acked: len(acked), ByNode: map[string]int{}}
	type seen struct {
		deliveries, exchanges int
		members               map[string]bool
	}
	bySeq := map[uint64]*seen{}
	for _, d := range deliveries {
		s := bySeq[d.Seq]
		if s == nil {
			s = &seen{members: map[string]bool{}}
			bySeq[d.Seq] = s
		}
		s.deliveries++
		if !d.SameExchange {
			s.exchanges++
		}
		s.members[d.Client] = true
		t.ByNode[nodeName(d.Node)]++
	}
	seqs := make([]uint64, 0, len(bySeq))
	for seq := range bySeq {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, seq := range seqs {
		s := bySeq[seq]
		if !acked[seq] {
			t.DeliveredUnacked++
		}
		if qos == 2 {
			if s.exchanges > 1 {
				t.Duplicates = append(t.Duplicates, seq)
			}
			continue
		}
		if s.deliveries > 1 {
			t.Repeated++
		}
		if len(s.members) > 1 {
			t.SecondMember++
		}
	}
	ackedSeqs := make([]uint64, 0, len(acked))
	for seq := range acked {
		ackedSeqs = append(ackedSeqs, seq)
	}
	sort.Slice(ackedSeqs, func(i, j int) bool { return ackedSeqs[i] < ackedSeqs[j] })
	for _, seq := range ackedSeqs {
		if bySeq[seq] == nil {
			t.Undelivered = append(t.Undelivered, seq)
		}
	}
	return t
}

// groupsResult builds the groups' record. Caller holds p.mu, after the drain.
func (p *probe) groupsResult(crashAt, instant time.Time) *groupsRecord {
	if len(p.groups) == 0 {
		return nil
	}
	gc := &p.groupCounts
	rec := &groupsRecord{Groups: len(p.groups), MembersPerGroup: len(p.groups[0].members),
		Attempts: gc.attempts, NoSubscribers: gc.noSubs, Errors: gc.errs,
		Refused: map[string]int64{}, RefusedAfterRecovery: map[string]int64{}, LastRefusalSec: -1,
		DeliveredByNode: map[string]int{}}
	for k, v := range gc.refused {
		rec.Refused[k] = v
	}
	split := splitAtInstant(gc.bad, crashAt, instant)
	rec.LastRefusalSec, rec.RefusedAfterRecovery, rec.ErrorsAfterRecovery, rec.RefusedWithMembersBack =
		split.lastRefusalSec, split.refusedAfter, split.errorsAfter, split.refusedWithBack
	for _, g := range p.groups {
		rec.Start = append(rec.Start, g.start)
		var deliveries []Delivery
		for _, m := range g.members {
			if m.Recovered() {
				rec.MembersRecovered++
			}
			for _, d := range m.TakeDeliveries() {
				if d.Publisher == g.pub.ID {
					deliveries = append(deliveries, d)
				}
			}
		}
		t := tallyGroup(g.qos, gc.acked[g.k], deliveries, p.nodeName)
		rec.Acked += int64(t.Acked)
		rec.AckedNotDelivered += len(t.Undelivered)
		for _, seq := range t.Undelivered {
			if len(rec.Examples) < 10 {
				rec.Examples = append(rec.Examples, fmt.Sprintf("%s seq %d on %s", g.pub.ID, seq, probeGroupFilter(g.k)))
			}
		}
		rec.QoS2Duplicates += len(t.Duplicates)
		for _, seq := range t.Duplicates {
			if len(rec.DuplicateExamples) < 10 {
				rec.DuplicateExamples = append(rec.DuplicateExamples, fmt.Sprintf("%s seq %d on %s", g.pub.ID, seq, probeGroupFilter(g.k)))
			}
		}
		rec.QoS1Repeated += t.Repeated
		rec.QoS1SecondMember += t.SecondMember
		rec.DeliveredUnacked += t.DeliveredUnacked
		for n, v := range t.ByNode {
			rec.DeliveredByNode[n] += v
		}
		rec.PerGroup = append(rec.PerGroup, groupFigures{Key: probeGroupFilter(g.k), Acked: t.Acked,
			AckedNotDelivered: len(t.Undelivered), QoS2Duplicates: len(t.Duplicates), QoS1Repeated: t.Repeated,
			QoS1SecondMember: t.SecondMember, DeliveredByNode: t.ByNode})
	}
	return rec
}

// groupsLine is the report's clause for the groups.
func groupsLine(g *groupsRecord) string {
	if g == nil {
		return ""
	}
	q1, q2 := 0, 0
	for _, s := range g.Start {
		if s.QoS == 2 {
			q2++
		} else {
			q1++
		}
	}
	out := fmt.Sprintf("; groups: %d shared groups (%d at QoS 1, %d at QoS 2) of %d members across the nodes, one publish each a second: %d attempts, %d acknowledged, %d with no subscriber, %d failed without an acknowledgement",
		g.Groups, q1, q2, g.MembersPerGroup, g.Attempts, g.Acked, g.NoSubscribers, g.Errors)
	if len(g.Refused) == 0 {
		out += ", no refusals"
	} else {
		for _, code := range sortedKeys(g.Refused) {
			out += fmt.Sprintf(", %d refused %s", g.Refused[code], code)
		}
	}
	after := g.ErrorsAfterRecovery
	for _, n := range g.RefusedAfterRecovery {
		after += n
	}
	if after == 0 {
		out += ", none once recovered"
	} else {
		out += fmt.Sprintf(", **%d after the recovery instant, %d of them refusals with every member back**", after, g.RefusedWithMembersBack)
	}
	return out + fmt.Sprintf(", %d acknowledged and delivered to no member, %d QoS 2 duplicates, %d QoS 1 repeats (%d on a second member)",
		g.AckedNotDelivered, g.QoS2Duplicates, g.QoS1Repeated, g.QoS1SecondMember)
}
