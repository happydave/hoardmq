package sim

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// The failover measurement's publish probe (WI 1618).
//
// The measurement publishes nothing, so a cluster in which every client is
// back and every QoS 1 publish is refused read as recovered: WIs 1624 and
// 1625 were both invisible to it. The probe is a small cohort of
// publisher/subscriber pairs that keeps publishing through the crash, the
// storm and the hold, and reports what the broker answered — by reason code —
// and whether what it acknowledged arrived.
//
// It is deliberately small and outside the recovered population: the headline
// must stay comparable with the batches already published, and a probe large
// enough to move the measurement would be measuring itself.

const (
	probeDefaultPairs = 100
	probeDefaultRate  = time.Second
	// probeDrain is how long the probe waits, after it stops publishing, for
	// what the broker acknowledged to arrive. A message acknowledged in the
	// last moments of the hold is in flight, not lost.
	probeDrain = 5 * time.Second
)

// probePairs is how many publisher/subscriber pairs to run: the default, or
// HOARDMQ_SIM_PROBE. Zero disables the probe.
func probePairs() int {
	if v := os.Getenv("HOARDMQ_SIM_PROBE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return probeDefaultPairs
}

// probeRecord is what the probe saw. Attempts is the denominator every other
// figure here is read against: a probe that published nothing reports no
// clean bill, it reports that it published nothing.
type probeRecord struct {
	Pairs    int     `json:"pairs"`
	RateMs   int64   `json:"rate_ms"`
	FromSec  float64 `json:"from_sec"`  // when publishing began, relative to the crash
	UntilSec float64 `json:"until_sec"` // when it stopped, on the same scale
	DrainSec float64 `json:"drain_sec"`

	Attempts int64 `json:"attempts"`
	// Acked is acknowledged with Success: the broker took the message and
	// owes its delivery. NoSubscribers is 0x10, an acknowledgement that
	// nothing was subscribed — with the partner subscribed throughout, one is
	// a statement about the broker's routing and not about the workload.
	Acked         int64            `json:"acked"`
	NoSubscribers int64            `json:"no_subscribers"`
	Refused       map[string]int64 `json:"refused"`
	Errors        int64            `json:"errors"`
	// A storm refuses: the broker is choosing to refuse rather than to
	// acknowledge what it cannot place, which is the product's stance. The
	// gating question is whether it is *still* refusing once the cluster has
	// recovered, so every refusal and every unacknowledged publish is also
	// counted from the recovery instant onwards, and the last one is dated.
	RefusedAfterRecovery map[string]int64 `json:"refused_after_recovery"`
	ErrorsAfterRecovery  int64            `json:"errors_after_recovery"`
	LastRefusalSec       float64          `json:"last_refusal_sec"`
	// RefusedWithSubscriberBack counts the refusals after the recovery
	// instant whose own partner was recovered at the time: the probe cohort
	// is outside the measured population, so without this a refusal to a
	// subscriber that had not come back yet would read as the cluster
	// refusing a recovered topic.
	RefusedWithSubscriberBack int64 `json:"refused_with_subscriber_back"`
	// SubsRecovered is how many of the pairs' subscribers were recovered when
	// the probe was read, out of Pairs.
	SubsRecovered int `json:"subs_recovered"`
	// LastSubRecoveredSec is when the last of them became recovered, relative
	// to the crash; negative when one never did.
	LastSubRecoveredSec float64 `json:"last_sub_recovered_sec"`
	// AckedNotDelivered is the gate the owner named: a message the broker
	// acknowledged and the subscriber never received, counted after the
	// drain.
	AckedNotDelivered int      `json:"acked_not_delivered"`
	Examples          []string `json:"examples,omitempty"`
	// Losses attributes up to maxLosses of those messages: the node that
	// answered each, and its pair's connection events and deliveries around
	// it. NoSubscriberMarks dates and places the No Matching Subscribers
	// answers the same way.
	Losses            []probeLoss `json:"losses,omitempty"`
	NoSubscriberMarks []probeMark `json:"no_subscriber_marks,omitempty"`
	// Groups is the shared groups' record, absent when none ran. Its
	// figures are never folded into the pairs' above.
	Groups *groupsRecord `json:"groups,omitempty"`
}

// probe is the cohort itself.
type probe struct {
	pairs int
	rate  time.Duration
	drain time.Duration
	pubs  []*Client
	subs  []*Client
	from  time.Time
	until time.Time

	stop chan struct{}
	wg   sync.WaitGroup
	// subBackAt is when each pair's subscriber was last seen to become
	// recovered, sampled by the watch goroutine.
	subBackAt []time.Time

	mu       sync.Mutex
	attempts int64
	acked    map[string]map[uint64]bool // publisher -> acknowledged sequences
	noSubs   int64
	refused  map[string]int64
	errs     int64
	// bad is every refusal and every unacknowledged publish, with its time,
	// so the record can be split at the recovery instant once that is known.
	// Bounded: a hundred pairs publishing once a second for ten minutes is
	// sixty thousand attempts, of which only the failures land here.
	bad []probeEvent

	groups      []*probeGroup
	groupCounts groupCounts
	// keepOutcomes records every publish's outcome with its time, so a run
	// can count them by its own windows (the rolling restart's steps).
	keepOutcomes bool
	outcomes     []probeOutcome
	// nodeName names the node at a client address.
	nodeName func(string) string
	// ackedAt is where and when each pair publish was answered Success or
	// No Matching Subscribers, and events every pair client's connection
	// events, drained from the clients as the run goes: what a lost
	// message is attributed with.
	ackedAt map[string]map[uint64]ackMark
	events  map[string][]Event
}

// ackMark is one publish's answer: when, from which node, and whether it
// was No Matching Subscribers.
type ackMark struct {
	at     time.Time
	node   string
	noSubs bool
}

// maxProbeEvents bounds the connection events kept per probe client.
const maxProbeEvents = 512

// probeEvent is one publish that was not acknowledged with Success.
type probeEvent struct {
	at   time.Time
	code string // empty for a publish that got no acknowledgement at all
	// subBack is whether this publisher's own partner, or for a group every
	// member, was recovered at the moment of the outcome. The probe's pairs are outside the measured
	// population, so they can still be recovering after it has recovered —
	// and a refusal to a topic whose only subscriber is not back yet says
	// something different from one where it is.
	subBack bool
}

// newProbe connects the pairs and subscribes each subscriber to its
// partner's topic, waiting for the SUBACK: a probe whose subscriber is not
// subscribed would report every publish as having no subscribers. It then
// builds the shared groups.
func newProbe(ctx context.Context, pairs, groups int, rate time.Duration, c *Cluster) (*probe, error) {
	addrs := c.Addresses()
	p := &probe{pairs: pairs, rate: rate, drain: probeDrain, stop: make(chan struct{}),
		acked: map[string]map[uint64]bool{}, refused: map[string]int64{},
		ackedAt: map[string]map[uint64]ackMark{}, events: map[string][]Event{},
		groupCounts: groupCounts{acked: map[int]map[uint64]bool{}, refused: map[string]int64{}},
		nodeName: func(addr string) string {
			if n := c.NodeAt(addr); n != nil {
				return n.Name
			}
			return addr
		}}
	for i := 0; i < pairs; i++ {
		order := append([]string(nil), addrs[i%len(addrs):]...)
		order = append(order, addrs[:i%len(addrs)]...)
		sub := NewClient(fmt.Sprintf("probe-s%04d", i), order, 3600, int64(1_000_000+i))
		pub := NewClient(fmt.Sprintf("probe-p%04d", i), order, 3600, int64(2_000_000+i))
		if err := sub.Run(ctx); err != nil {
			return p, err
		}
		if err := sub.Subscribe(ctx, probeTopic(i), 1); err != nil {
			return p, err
		}
		if err := pub.Run(ctx); err != nil {
			return p, err
		}
		p.subs, p.pubs = append(p.subs, sub), append(p.pubs, pub)
		p.acked[pub.ID] = map[uint64]bool{}
		p.ackedAt[pub.ID] = map[uint64]ackMark{}
	}
	if groups > 0 {
		built, err := newProbeGroups(ctx, groups, c)
		p.groups = built
		for _, g := range built {
			p.groupCounts.acked[g.k] = map[uint64]bool{}
		}
		if err != nil {
			return p, err
		}
	}
	return p, nil
}

func probeTopic(i int) string { return fmt.Sprintf("probe/%04d", i) }

// start begins publishing, one goroutine per publisher, and watches the
// pairs' subscribers so a refusal can be read against whether its own
// subscriber was back.
func (p *probe) start(ctx context.Context) {
	p.from = time.Now()
	p.subBackAt = make([]time.Time, len(p.subs))
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		was := make([]bool, len(p.subs))
		for {
			select {
			case <-p.stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
			}
			for i, sub := range p.subs {
				back := sub.Recovered()
				if back != was[i] {
					p.mu.Lock()
					if back {
						p.subBackAt[i] = time.Now()
					} else {
						p.subBackAt[i] = time.Time{}
					}
					p.mu.Unlock()
				}
				was[i] = back
			}
			p.collectEvents()
		}
	}()
	for i, pub := range p.pubs {
		p.wg.Add(1)
		go func(i int, pub *Client) {
			defer p.wg.Done()
			t := time.NewTicker(p.rate)
			defer t.Stop()
			var seq uint64
			for {
				select {
				case <-p.stop:
					return
				case <-ctx.Done():
					return
				case <-t.C:
				}
				seq++
				p.publish(ctx, i, pub, seq)
			}
		}(i, pub)
	}
	for _, g := range p.groups {
		p.wg.Add(1)
		go func(g *probeGroup) {
			defer p.wg.Done()
			t := time.NewTicker(p.rate)
			defer t.Stop()
			var seq uint64
			for {
				select {
				case <-p.stop:
					return
				case <-ctx.Done():
					return
				case <-t.C:
				}
				seq++
				p.publishGroup(ctx, g, seq)
			}
		}(g)
	}
}

// collectEvents moves every pair client's connection events into the probe,
// so the clients' own cap does not drop the ones a loss is read against.
func (p *probe) collectEvents() {
	clients := append(append([]*Client(nil), p.pubs...), p.subs...)
	for _, c := range clients {
		evs := c.TakeEvents()
		if len(evs) == 0 {
			continue
		}
		p.mu.Lock()
		if room := maxProbeEvents - len(p.events[c.ID]); room > 0 {
			p.events[c.ID] = append(p.events[c.ID], evs[:min(room, len(evs))]...)
		}
		p.mu.Unlock()
	}
}

// publish sends one message and records what came back. Every outcome is
// counted: a refusal by its reason code, an acknowledgement by its kind, a
// failure with no acknowledgement at all.
func (p *probe) publish(ctx context.Context, i int, pub *Client, seq uint64) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	code, err := pub.Publish(ctx, probeTopic(i), 1, seq)
	node := p.nodeName(pub.Node())
	back := p.subs[i].Recovered()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	p.noteOutcome(false, code, err, time.Now())
	switch {
	case err != nil && code >= 0x80:
		p.refused[code.String()]++
		p.bad = append(p.bad, probeEvent{at: time.Now(), code: code.String(), subBack: back})
	case err != nil:
		p.errs++
		p.bad = append(p.bad, probeEvent{at: time.Now(), subBack: back})
	case code == packet.NoMatchingSubscribers:
		p.noSubs++
		p.ackedAt[pub.ID][seq] = ackMark{at: time.Now(), node: node, noSubs: true}
	default:
		p.acked[pub.ID][seq] = true
		p.ackedAt[pub.ID][seq] = ackMark{at: time.Now(), node: node}
	}
}

// maxLosses bounds the losses and marks a probe record attributes; lossWindow
// is how far either side of a lost message's answer its pair's events and
// deliveries are kept.
const (
	maxLosses  = 50
	lossWindow = 30 * time.Second
)

// probeMark is one publish's answer, dated against the run's zero.
type probeMark struct {
	Publisher string  `json:"publisher"`
	Seq       uint64  `json:"seq"`
	Node      string  `json:"node"`
	AtSec     float64 `json:"at_sec"`
}

// probeLoss is one message answered Success that its subscriber never
// received, with what its pair saw around the answer.
type probeLoss struct {
	probeMark
	Topic      string          `json:"topic"`
	Subscriber string          `json:"subscriber"`
	SubEvents  []lossEvent     `json:"sub_events"`
	PubEvents  []lossEvent     `json:"pub_events"`
	Delivered  []lossDelivered `json:"delivered"`
}

// lossEvent is a connection event of a pair client.
type lossEvent struct {
	Kind    string  `json:"kind"`
	Code    string  `json:"code,omitempty"`
	Node    string  `json:"node"`
	Present bool    `json:"present,omitempty"`
	AtSec   float64 `json:"at_sec"`
}

// lossDelivered is a delivery to the subscriber near a loss.
type lossDelivered struct {
	Seq   uint64  `json:"seq"`
	Node  string  `json:"node"`
	Dup   bool    `json:"dup,omitempty"`
	AtSec float64 `json:"at_sec"`
}

// lossEvents keeps a client's events within lossWindow of at.
func lossEvents(evs []Event, at, zero time.Time, nodeName func(string) string) []lossEvent {
	out := []lossEvent{}
	for _, e := range evs {
		if e.At.Before(at.Add(-lossWindow)) || e.At.After(at.Add(lossWindow)) {
			continue
		}
		le := lossEvent{Kind: e.Kind, Node: nodeName(e.Node), Present: e.Present, AtSec: e.At.Sub(zero).Seconds()}
		if e.Code != 0 {
			le.Code = e.Code.String()
		}
		out = append(out, le)
	}
	return out
}

// probeOutcome is one publish's outcome, for counting by window.
type probeOutcome struct {
	at    time.Time
	group bool
	// code is the refusal's reason code, "" when the publish was answered
	// with an acknowledgement or failed without one; failed says which.
	code   string
	failed bool
}

// noteOutcome keeps one outcome when the run asked for them. Caller holds p.mu.
func (p *probe) noteOutcome(group bool, code packet.ReasonCode, err error, at time.Time) {
	if !p.keepOutcomes {
		return
	}
	o := probeOutcome{at: at, group: group}
	switch {
	case err != nil && code >= 0x80:
		o.code = code.String()
	case err != nil:
		o.failed = true
	}
	p.outcomes = append(p.outcomes, o)
}

// result stops the probe, waits the drain for what was acknowledged to
// arrive, and reports. A message the broker acknowledged with Success and
// the partner never received is the loss the owner's second gating figure is
// about.
// result takes the recovery instant so the figures can be split at it; a
// zero instant means the run never recovered, and then everything the probe
// saw is "before" it.
func (p *probe) result(crashAt, instant time.Time) probeRecord {
	close(p.stop)
	p.wg.Wait()
	p.until = time.Now()
	time.Sleep(p.drain)
	p.collectEvents()

	p.mu.Lock()
	defer p.mu.Unlock()
	rec := probeRecord{Pairs: p.pairs, RateMs: p.rate.Milliseconds(),
		FromSec: p.from.Sub(crashAt).Seconds(), UntilSec: p.until.Sub(crashAt).Seconds(),
		DrainSec: p.drain.Seconds(), Attempts: p.attempts, NoSubscribers: p.noSubs,
		Refused: map[string]int64{}, RefusedAfterRecovery: map[string]int64{}, Errors: p.errs,
		LastRefusalSec: -1}
	for k, v := range p.refused {
		rec.Refused[k] = v
	}
	split := splitAtInstant(p.bad, crashAt, instant)
	rec.LastRefusalSec, rec.RefusedAfterRecovery, rec.ErrorsAfterRecovery, rec.RefusedWithSubscriberBack =
		split.lastRefusalSec, split.refusedAfter, split.errorsAfter, split.refusedWithBack
	rec.LastSubRecoveredSec = -1
	for i, sub := range p.subs {
		if sub.Recovered() {
			rec.SubsRecovered++
		}
		if i < len(p.subBackAt) && !p.subBackAt[i].IsZero() {
			if d := p.subBackAt[i].Sub(crashAt).Seconds(); d > rec.LastSubRecoveredSec {
				rec.LastSubRecoveredSec = d
			}
		}
	}
	for i, sub := range p.subs {
		got := map[uint64]bool{}
		deliveries := sub.TakeDeliveries()
		for _, d := range deliveries {
			got[d.Seq] = true
		}
		pubID := p.pubs[i].ID
		var lost []uint64
		for seq := range p.acked[pubID] {
			rec.Acked++
			if !got[seq] {
				rec.AckedNotDelivered++
				lost = append(lost, seq)
				if len(rec.Examples) < 10 {
					rec.Examples = append(rec.Examples, fmt.Sprintf("%s seq %d on %s", pubID, seq, probeTopic(i)))
				}
			}
		}
		sort.Slice(lost, func(a, b int) bool { return lost[a] < lost[b] })
		for _, seq := range lost {
			if len(rec.Losses) >= maxLosses {
				break
			}
			m := p.ackedAt[pubID][seq]
			l := probeLoss{probeMark: probeMark{Publisher: pubID, Seq: seq, Node: m.node, AtSec: m.at.Sub(crashAt).Seconds()},
				Topic: probeTopic(i), Subscriber: sub.ID,
				SubEvents: lossEvents(p.events[sub.ID], m.at, crashAt, p.nodeName),
				PubEvents: lossEvents(p.events[pubID], m.at, crashAt, p.nodeName), Delivered: []lossDelivered{}}
			for _, d := range deliveries {
				if d.Seq+5 >= seq && d.Seq <= seq+5 {
					l.Delivered = append(l.Delivered, lossDelivered{Seq: d.Seq, Node: p.nodeName(d.Node), Dup: d.Dup, AtSec: d.At.Sub(crashAt).Seconds()})
				}
			}
			rec.Losses = append(rec.Losses, l)
		}
		for seq, m := range p.ackedAt[pubID] {
			if m.noSubs && len(rec.NoSubscriberMarks) < maxLosses {
				rec.NoSubscriberMarks = append(rec.NoSubscriberMarks, probeMark{Publisher: pubID, Seq: seq, Node: m.node, AtSec: m.at.Sub(crashAt).Seconds()})
			}
		}
	}
	rec.Groups = p.groupsResult(crashAt, instant)
	return rec
}

// instantSplit is a set of probe events split at the recovery instant.
type instantSplit struct {
	lastRefusalSec  float64
	refusedAfter    map[string]int64
	errorsAfter     int64
	refusedWithBack int64
}

// splitAtInstant dates the last refusal against the crash and counts what
// came at or after the instant; a zero instant puts everything before it.
func splitAtInstant(bad []probeEvent, crashAt, instant time.Time) instantSplit {
	s := instantSplit{lastRefusalSec: -1, refusedAfter: map[string]int64{}}
	for _, e := range bad {
		if e.code != "" && e.at.Sub(crashAt).Seconds() > s.lastRefusalSec {
			s.lastRefusalSec = e.at.Sub(crashAt).Seconds()
		}
		if instant.IsZero() || e.at.Before(instant) {
			continue
		}
		if e.code == "" {
			s.errorsAfter++
			continue
		}
		s.refusedAfter[e.code]++
		if e.subBack {
			s.refusedWithBack++
		}
	}
	return s
}

func (p *probe) close() {
	all := append(append([]*Client(nil), p.pubs...), p.subs...)
	for _, g := range p.groups {
		all = append(all, g.members...)
		if g.pub != nil {
			all = append(all, g.pub)
		}
	}
	for _, c := range all {
		c.Cancel()
	}
	for _, c := range all {
		c.Close()
	}
}

// probeLine is the report's row. A probe that published nothing says so: the
// figures below it would otherwise read as a clean bill over no evidence.
func probeLine(rec *probeRecord) string {
	if rec == nil {
		return "not run"
	}
	if rec.Attempts == 0 {
		return fmt.Sprintf("%d pairs ran and published nothing — no publish figure from this run", rec.Pairs)
	}
	out := fmt.Sprintf("%d pairs, one QoS 1 publish each every %s from %.0fs to %.0fs: %d attempts, %d acknowledged, %d with no subscriber, %d failed without an acknowledgement",
		rec.Pairs, time.Duration(rec.RateMs)*time.Millisecond, rec.FromSec, rec.UntilSec, rec.Attempts, rec.Acked, rec.NoSubscribers, rec.Errors)
	if len(rec.Refused) == 0 {
		out += "; no refusals"
	} else {
		for _, code := range sortedKeys(rec.Refused) {
			out += fmt.Sprintf("; %d refused %s", rec.Refused[code], code)
		}
		out += fmt.Sprintf(" (last at %.1fs)", rec.LastRefusalSec)
	}
	after := rec.ErrorsAfterRecovery
	for _, n := range rec.RefusedAfterRecovery {
		after += n
	}
	if after == 0 {
		out += "; none once recovered"
	} else {
		out += fmt.Sprintf("; **%d after the recovery instant, %d of them refusals whose own subscriber was back**",
			after, rec.RefusedWithSubscriberBack)
	}
	out += fmt.Sprintf("; %d of %d probe subscribers recovered, the last at %.1fs",
		rec.SubsRecovered, rec.Pairs, rec.LastSubRecoveredSec)
	out += fmt.Sprintf("; %d acknowledged and never delivered after a %.0fs drain", rec.AckedNotDelivered, rec.DrainSec)
	return out + groupsLine(rec.Groups)
}

// sortedKeys keeps a map-backed row stable between runs, so two reports can
// be diffed.
func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
