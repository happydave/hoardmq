package sim

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// Property names, as reported.
const (
	PropOneOwner    = "one-live-owner"
	PropNoDuplicate = "no-double-delivery"
	PropOrdering    = "ordering"
	PropNoAckedLoss = "no-acknowledged-loss"
	PropMinority    = "minority-refuses"
	PropFailClosed  = "checksum-fails-closed"
	// PropRecovers: once every fault is healed and every node is running,
	// each node still accepts a QoS 1 publish. Four defects this month left a
	// node refusing every QoS 1/2 publish indefinitely and the corpus passed
	// each time, because a refusal never enters the ledger's publications and
	// so no other property can see it (WI 1502).
	PropRecovers = "replication-recovers"
	// PropNoRefusalWithoutFaults: a run that injected no faults refused no
	// QoS 1 or 2 publish. no-acknowledged-loss is a statement about
	// acknowledged publishes only, so a build that refuses more of its
	// traffic satisfies it more easily. With no fault injected there is
	// nothing for a refusal to blame, so zero is the design's own number
	// rather than a threshold this property chose (WI 1308).
	PropNoRefusalWithoutFaults = "no-refusal-without-faults"
	// PropCountedOnce: the harness recorded each publish as accepted or as
	// refused, never as both. This is a property over the harness's own
	// accounting rather than over the broker, and it is here because the
	// harness is one of the suspects whenever it reports a defect: the
	// original reading of WI 1308 was that a refusal was being counted as an
	// acknowledgement, and had it been true, the suite would have noticed only
	// on a faulted seed and only as something else: a false acknowledged loss,
	// or a double delivery through the re-homing exception, which reads that
	// ledger. Never as an accounting error.
	PropCountedOnce = "publish-counted-once"
	// PropSubscribed: every SUBSCRIBE in the run was granted. A refused
	// subscribe is the delivery side's blind spot — a subscriber that was
	// never subscribed produces no publication and no missing delivery, so
	// no-acknowledged-loss and no-double-delivery cannot fail for it. The
	// harness counts the refusals since WI 1500; this is what reads them
	// (WI 1509).
	PropSubscribed = "subscribe-granted"
	// PropNoServedEmpty: no node served a fresh session to a client whose
	// lease showed it had one (hoardmq_sessions_rebuilt_empty_total). The
	// broker serves it when no copy exists anywhere, and no corpus schedule
	// is meant to remove every copy of a session.
	PropNoServedEmpty = "no-session-served-empty"
)

// Violation is one property failure with its evidence.
type Violation struct {
	Property string
	Detail   string
}

func (v Violation) String() string { return v.Property + ": " + v.Detail }

// OwnerSample records a client identifier seen online on more than one node
// at one instant.
type OwnerSample struct {
	At     time.Time
	Client string
	Nodes  []string
}

// Probe is a connect attempt against a node during a partition: the
// minority must refuse with Server Unavailable or Use Another Server.
type Probe struct {
	At   time.Time
	Node string
	Code packet.ReasonCode
	Ref  string
	Err  error
}

// FailClosed records a restart over a corrupted log: the node must refuse.
type FailClosed struct {
	Node    string
	Log     string
	Refused bool
	Err     error
}

// Evidence is everything the checker reads.
type Evidence struct {
	Ledger     *Ledger
	Owners     []OwnerSample
	Probes     []Probe
	FailClosed []FailClosed
	// ResendLag and ResendWindow bound the replica-resend exception; they
	// derive from the cluster's configured timing (see Run) so they cannot
	// drift from it. Zero disables the exception: every repeat is judged.
	ResendLag    time.Duration
	ResendWindow time.Duration
	// Routes is every node's view of the workload's topics over the run, so
	// an acknowledged loss carries the routing state that produced it
	// (WI 1354).
	Routes []RouteSample
	// Liveness is one probe per running node, taken after every fault has
	// been healed and the cluster has settled: can this node still accept a
	// publish? Separate from Probes, which are taken during a partition for
	// minority-refuses.
	Liveness []Probe
	// BarrierFailures is each node's replication barrier failures by peer and
	// reason, read when the probes are taken, so a violation names the stream.
	BarrierFailures map[string]string
	// Unapplied counts the replicated records that changed nothing on a
	// follower, by reason and by reason.type, and the owners' repairs of a
	// refused key by outcome.
	Unapplied map[string]int
	// RefusedKeys are the keys peers refused records for, watched from the
	// heal (nil when nothing was watched).
	RefusedKeys []KeyBlock
	// Recovery is each node's refusing time after the heal (nil when the
	// cluster was not whole), carried into a liveness violation and the stats.
	Recovery []NodeRecovery
	// ServedEmpty is each running node's served-empty count by reason, read
	// at the end of the run (nil when not read).
	ServedEmpty map[string]map[string]int
	// FaultFree says the run injected no faults, which is what makes a
	// refused publish a violation rather than the design working. It is set
	// from the schedule rather than inferred from the ledger, and its zero
	// value judges nothing: a checker driven over a synthetic ledger has no
	// run behind it and must not be held to a whole run's rule.
	FaultFree bool
}

// retransmitted reports whether a delivery is a re-send of one the subscriber
// was already given, which its application sees once: the DUP flag, or a QoS 2
// PUBLISH under an identifier whose exchange is still open. A rebuilt session
// re-sends a queued entry under its reserved identifier with DUP clear, since
// the new owner cannot know the old one had sent it. Both delivery properties
// read this, so they cannot disagree about the same delivery.
func retransmitted(d Delivery) bool {
	return d.Dup || (d.QoS == 2 && d.SameExchange && !d.AfterComplete)
}

type msgKey struct {
	sub, pub, topic string
	seq             uint64
}

// countedOnceViolations reports a publish the harness put in both the
// accepted and the refused ledger. The workload does not retry an attempt —
// it counts it and moves on — so a client's sequence number appears once, in
// one of the two. Callers hold the ledger's lock.
func countedOnceViolations(l *Ledger) []Violation {
	if len(l.Failures) == 0 || len(l.Publications) == 0 {
		return nil
	}
	type pubKey struct {
		client string
		seq    uint64
	}
	accepted := make(map[pubKey]bool, len(l.Publications))
	for _, p := range l.Publications {
		accepted[pubKey{p.Publisher, p.Seq}] = true
	}
	both := make([]string, 0)
	for _, f := range l.Failures {
		if accepted[pubKey{f.Client, f.Seq}] {
			both = append(both, fmt.Sprintf("%s seq %d (%s)", f.Client, f.Seq, orUnknown(f.Node)))
		}
	}
	if len(both) == 0 {
		return nil
	}
	sort.Strings(both)
	return []Violation{{PropCountedOnce, fmt.Sprintf(
		"%d publishes are in both the accepted and the refused ledger, so a refusal was counted as an acknowledgement: %s",
		len(both), strings.Join(head(both, 10), ", "))}}
}

// refusalViolations judges the refusals of a run that injected no faults.
//
// Non-vacuity: a fault-free run that accepted no QoS 1 or 2 publish has not
// shown that it would have noticed a refusal, and is a violation in itself.
// Callers hold the ledger's lock.
func refusalViolations(l *Ledger, faultFree bool) []Violation {
	if !faultFree {
		return nil
	}
	accepted := 0
	for _, p := range l.Publications {
		if p.QoS > 0 {
			accepted++
		}
	}
	byNode := map[string]int{}
	byReason := map[string]int{}
	refused := 0
	for _, f := range l.Failures {
		if f.QoS == 0 {
			continue
		}
		refused++
		byNode[orUnknown(f.Node)]++
		if f.Code == 0 {
			byReason["the send itself failed"]++
			continue
		}
		byReason[f.Code.String()]++
	}
	if accepted == 0 {
		return []Violation{{PropNoRefusalWithoutFaults, fmt.Sprintf(
			"no QoS 1 or 2 publish was accepted, so a refusal could not have been noticed (%d refused)", refused)}}
	}
	if refused == 0 {
		return nil
	}
	return []Violation{{PropNoRefusalWithoutFaults, fmt.Sprintf(
		"%d of %d QoS 1/2 publishes refused with no fault injected; by node %s; by reason %s",
		refused, refused+accepted, countsByKey(byNode), countsByKey(byReason))}}
}

// countsByKey renders a count map in key order, so a violation reads the same
// way twice.
func countsByKey(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

// Check evaluates every property and returns the violations.
func Check(e Evidence) []Violation {
	var out []Violation
	// Replication recovers: every node that was running at the end could
	// still accept a publish. Checked before the ledger is locked because it
	// reads none of it.
	out = append(out, livenessViolations(e.Liveness, e.BarrierFailures, e.Recovery)...)
	l := e.Ledger
	l.mu.Lock()
	defer l.mu.Unlock()

	// Every subscribe granted. Read here rather than beside the liveness
	// check because it needs the ledger's events and subscriptions.
	out = append(out, subscribedViolations(len(l.Subscriptions), subscribeRefusals(l.Events))...)

	// Refusals: counted once, whatever the run did, and judged where the run
	// had no fault to blame them on.
	out = append(out, countedOnceViolations(l)...)
	out = append(out, refusalViolations(l, e.FaultFree)...)
	out = append(out, servedEmptyViolations(e.ServedEmpty)...)

	// One live owner: no sample saw a client on two nodes.
	for _, s := range e.Owners {
		out = append(out, Violation{PropOneOwner, fmt.Sprintf("%s online on %s at %s", s.Client, strings.Join(s.Nodes, " and "), s.At.Format(time.RFC3339Nano))})
	}

	// Duplicates: one delivery without DUP per message per subscriber; any
	// further delivery must carry DUP. One exception, stated: a QoS 1
	// message re-sent by a new owner that rebuilt the session from a replica
	// (the subscriber came back on another node while its previous node was
	// unreachable). The replica holds the message's enqueue — replication
	// waits for that before the publish is acknowledged — but the
	// subscriber's own PUBACK is replicated asynchronously, because there is
	// nothing to hold while a client acknowledgement travels the other way;
	// so the new owner may deliver again what the client already
	// acknowledged, as a first delivery.
	//
	// QoS 1 permits this: at-least-once, and the DUP flag describes a
	// sender re-transmitting from its own state, which the new owner is not.
	//
	// QoS 2 is **not** excused. The broker replicates the PUBREC state
	// before its PUBREL, and holds the PUBREL when it cannot, so a client
	// never completes an exchange the replicas do not know about and a
	// rebuild's re-send is recognised as the same exchange. A QoS 2 repeat
	// without DUP is therefore a violation of exactly-once, always. That holds
	// within an exchange the broker accepted: an error PUBREC ends the exchange
	// after the message may already have been routed, so a publisher that sends
	// it again is publishing anew, and scenarios must not do so (WI 1486).
	resend := replicaResends(l, e.ResendLag, e.ResendWindow)
	nonDup := map[msgKey]int{}
	dups := map[msgKey]int{}
	for _, d := range l.Deliveries {
		if d.QoS == 0 {
			continue
		}
		k := msgKey{d.Client, d.Publisher, d.Topic, d.Seq}
		if d.QoS == 2 && d.AfterComplete {
			// Exactly-once as the subscriber experiences it: the application
			// already had this message and the exchange was finished. A DUP
			// flag does not make a second delivery legal.
			out = append(out, Violation{PropNoDuplicate, fmt.Sprintf("%s received %s/%s seq %d again after completing its QoS 2 exchange:%s", k.sub, k.pub, k.topic, k.seq, history(l, k))})
			continue
		}
		if retransmitted(d) {
			dups[k]++
		} else if resend[k] {
			continue
		} else {
			nonDup[k]++
		}
	}
	for k, n := range nonDup {
		if n > 1 {
			out = append(out, Violation{PropNoDuplicate, fmt.Sprintf("%s received %s/%s seq %d %d times without DUP:%s", k.sub, k.pub, k.topic, k.seq, n, history(l, k))})
		}
	}
	for k := range dups {
		if nonDup[k] == 0 {
			// A redelivery flagged DUP with no original is fine for the
			// subscriber (the first attempt was lost in flight); noted, not
			// a violation.
			continue
		}
	}

	// Ordering per (subscriber, publisher, topic) over deliveries without
	// DUP.
	type streamKey struct{ sub, pub, topic string }
	last := map[streamKey]uint64{}
	for _, d := range l.Deliveries {
		if retransmitted(d) || d.Publisher == "" || resend[msgKey{d.Client, d.Publisher, d.Topic, d.Seq}] {
			continue
		}
		k := streamKey{d.Client, d.Publisher, d.Topic}
		if prev, ok := last[k]; ok && d.Seq <= prev {
			detail := fmt.Sprintf("%s received %s/%s seq %d after seq %d: seq %d%s", d.Client, d.Publisher, d.Topic, d.Seq, prev, d.Seq, history(l, msgKey{d.Client, d.Publisher, d.Topic, d.Seq}))
			if prev != d.Seq {
				detail += fmt.Sprintf("; seq %d%s", prev, history(l, msgKey{d.Client, d.Publisher, d.Topic, prev}))
			}
			out = append(out, Violation{PropOrdering, detail})
		}
		last[k] = d.Seq
	}

	// Acknowledged loss: every acknowledged QoS 1/2 publish reaches every
	// durable subscriber that held the topic before the acknowledgement. The
	// subject is the *accepted* publishes — a refused one is in the ledger's
	// failures and in nothing this iterates — so what makes the property
	// non-vacuous is that the run accepted some, which TestCorpus asserts
	// over the corpus and no-refusal-without-faults asserts for a run with no
	// fault to blame (WI 1308).
	delivered := map[msgKey]bool{}
	for _, d := range l.Deliveries {
		delivered[msgKey{d.Client, d.Publisher, d.Topic, d.Seq}] = true
	}
	type gap struct {
		client   string
		seqs     []uint64
		first    time.Time
		noMatch  int // acknowledged with No Matching Subscribers
		firstAck packet.ReasonCode
		firstOn  string // the node that answered the first one
		topic    string
	}
	missing := map[string]*gap{}
	for _, p := range l.Publications {
		if p.QoS == 0 {
			continue
		}
		for _, s := range l.Subscriptions {
			if s.Filter != p.Topic || !s.Since.Before(p.AckedAt) {
				continue
			}
			if delivered[msgKey{s.Client, p.Publisher, p.Topic, p.Seq}] {
				continue
			}
			k := s.Client + " <- " + p.Publisher + "/" + p.Topic
			g := missing[k]
			if g == nil {
				g = &gap{client: s.Client, first: p.AckedAt, firstAck: p.Code, firstOn: p.Node, topic: p.Topic}
				missing[k] = g
			}
			if p.AckedAt.Before(g.first) {
				g.first, g.firstAck, g.firstOn = p.AckedAt, p.Code, p.Node
			}
			if p.Code == packet.NoMatchingSubscribers {
				g.noMatch++
			}
			g.seqs = append(g.seqs, p.Seq)
		}
	}
	keys := make([]string, 0, len(missing))
	for k := range missing {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		g := missing[k]
		sort.Slice(g.seqs, func(i, j int) bool { return g.seqs[i] < g.seqs[j] })
		on := g.firstOn
		if on == "" {
			on = "an unrecorded node"
		}
		out = append(out, Violation{PropNoAckedLoss, fmt.Sprintf("%s: %d acknowledged messages never delivered (first %v); %d of them were answered No Matching Subscribers; the first was acknowledged %s by %s at %s, around which %s saw %s; routes for %s were %s",
			k, len(g.seqs), head(g.seqs, 10), g.noMatch, g.firstAck, on, g.first.Format("15:04:05.000"), g.client, nearEvents(l.Events, g.client, g.first, 2*time.Second),
			g.topic, routesAround(e.Routes, g.topic, g.first, 750*time.Millisecond))})
	}

	// Minority: probes during a partition must be refused with 0x88 or 0x9C.
	for _, p := range e.Probes {
		if p.Err != nil {
			continue // a refused TCP connection is a refusal too
		}
		if p.Code != packet.ServerUnavailable && p.Code != packet.UseAnotherServer {
			out = append(out, Violation{PropMinority, fmt.Sprintf("%s answered a new connect during its partition with %s (reference %q)", p.Node, p.Code, p.Ref)})
		}
	}

	// Fail closed: a restart over a corrupted record must be refused.
	for _, f := range e.FailClosed {
		if !f.Refused {
			out = append(out, Violation{PropFailClosed, fmt.Sprintf("%s started over a corrupted %s log", f.Node, f.Log)})
		}
	}
	return out
}

// noExpiryReason is the lease service's reason for a session served empty
// whose lease carried no session expiry: such a session ended with its
// connection, so it is recorded but is no violation.
const noExpiryReason = "no-expiry"

// servedEmptyViolations reports each node that served a session empty, with
// its counts by reason, leaving out sessions that had no expiry.
func servedEmptyViolations(counts map[string]map[string]int) []Violation {
	nodes := make([]string, 0, len(counts))
	for n := range counts {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	var out []Violation
	for _, n := range nodes {
		total := 0
		for r, v := range counts[n] {
			if r != noExpiryReason {
				total += v
			}
		}
		if total > 0 {
			out = append(out, Violation{PropNoServedEmpty, fmt.Sprintf("%s served %d sessions empty although their leases showed one existed (by reason: %s)", n, total, countsByKey(counts[n]))})
		}
	}
	return out
}

// nearEvents renders a client's connection events within a window either
// side of an instant, so a loss can be read against what the client's
// connection was doing at the time.
func nearEvents(events []Event, client string, at time.Time, window time.Duration) string {
	var out []string
	for _, e := range events {
		if e.Client != client || e.At.Before(at.Add(-window)) || e.At.After(at.Add(window)) {
			continue
		}
		s := fmt.Sprintf("%s %s on %s", e.At.Format("15:04:05.000"), e.Kind, e.Node)
		if e.Kind == "connack" {
			s += fmt.Sprintf(" (%s, session present %v)", e.Code, e.Present)
		} else if e.Code != 0 {
			s += " (" + e.Code.String() + ")"
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return "no connection event"
	}
	return strings.Join(out, ", ")
}

// head is the first n of a list, so a violation names examples without
// printing a run's whole ledger.
func head[T any](s []T, n int) []T {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// Stats summarises the ledger for the report.
func Stats(e Evidence) map[string]int {
	l := e.Ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	st := map[string]int{
		"replicaResends": len(replicaResends(l, e.ResendLag, e.ResendWindow)),
		"publications":   len(l.Publications),
		"deliveries":     len(l.Deliveries),
		"publishErrors":  len(l.Errors),
		"probes":         len(e.Probes),
		"livenessProbes": len(e.Liveness),
		"ownerSamples":   len(e.Owners),
		"failClosed":     len(e.FailClosed),
	}
	for k, v := range recoveryStats(e.Recovery) {
		st[k] = v
	}
	for k, v := range e.Unapplied {
		st["unapplied."+k] = v
	}
	for _, byReason := range e.ServedEmpty {
		for r, v := range byReason {
			st["servedEmpty."+r] += v
		}
	}
	for k, v := range refusedKeyStats(e.RefusedKeys) {
		st[k] = v
	}
	// Refusals and the clients they landed on, the same pair the failover
	// artifact carries: one client refused repeatedly and many clients refused
	// once are different findings under one total (WI 1509).
	if total, clients := refusalsByClient(subscribeRefusals(l.Events)); total > 0 {
		st["subRefusals"], st["subRefusedClients"] = total, clients
	}
	// Which node refused, not just how many refusals there were: one node
	// refusing everything and every node refusing a little are different
	// findings under the same total (WI 1502).
	for _, f := range l.Failures {
		st["publishErrors."+orUnknown(f.Node)]++
	}
	seen := map[string]bool{}
	for _, ev := range l.Events {
		st["event."+ev.Kind]++
		if ev.Kind == "disconnect" && ev.Code == packet.UseAnotherServer {
			st["redirects"]++
		}
		if ev.Kind == "connack" && ev.Code == packet.Success {
			// A durable client told its session is absent on a reconnect lost
			// its subscriptions and its queue.
			if seen[ev.Client] && !ev.Present {
				st["sessionLost"]++
			}
			seen[ev.Client] = true
		}
	}
	return st
}

// replicaResends classifies the QoS 1 and 2 deliveries a subscriber received
// again because its session was rebuilt from a replica that had not yet
// recorded that the client had the message (the client's PUBACK, or the
// PUBREC state and the packet identifier the owner assigned — replicated
// after the fact, and not at all while the owner is partitioned). The
// exception is narrow and attributable, never a category: the subscriber
// must have come back involuntarily at time t — an accepted CONNACK with
// Session Present after a dropped connection or a server DISCONNECT (a
// fence, a takeover, a redirect), on any node, since a lease adopted and
// pulled back rebuilds the session even on the node the client returns to;
// the message's first delivery must fall in (t − lag, t); and the repeat
// must arrive from the node that accepted the client within window after t.
// lag is how long a client's acknowledgement can take to reach the follower
// (a partitioned owner serves for up to a ttl with nothing reaching them)
// and window how long a rebuilt queue takes to drain on connect; both derive
// from the cluster's timing (see Run), and zero disables the exception. Each
// excused message therefore has exactly one first delivery inside a bounded
// window before one hop, so a regression that duplicated freely would be
// excused for at most the messages in flight at each hop and reported for
// the rest. Keyed by delivery; the ledger must hold the caller's lock.
func replicaResends(l *Ledger, lag, window time.Duration) map[msgKey]bool {
	// QoS 1 only: see the note in Check. QoS 2 exactness is held by the
	// broker itself and a repeat there is always a violation.
	out := map[msgKey]bool{}
	if lag <= 0 || window <= 0 {
		return out
	}
	type hop struct {
		at   time.Time
		node string
	}
	hops := map[string][]hop{}
	lost := map[string]bool{} // the client's last connection ended involuntarily
	events := append([]Event(nil), l.Events...)
	sort.Slice(events, func(i, j int) bool { return events[i].At.Before(events[j].At) })
	for _, e := range events {
		switch e.Kind {
		case "dropped", "disconnect":
			lost[e.Client] = true
		case "connack":
			if e.Code != 0 {
				continue
			}
			if lost[e.Client] && e.Present {
				hops[e.Client] = append(hops[e.Client], hop{e.At, e.Node})
			}
			lost[e.Client] = false
		}
	}
	if len(hops) == 0 {
		return out
	}
	first := map[msgKey]Delivery{}
	for _, d := range l.Deliveries {
		if d.Dup || d.QoS != 1 {
			continue
		}
		k := msgKey{d.Client, d.Publisher, d.Topic, d.Seq}
		if f, ok := first[k]; !ok || d.At.Before(f.At) {
			first[k] = d
		}
	}
	for _, d := range l.Deliveries {
		if d.Dup || d.QoS != 1 {
			continue
		}
		k := msgKey{d.Client, d.Publisher, d.Topic, d.Seq}
		f := first[k]
		if !d.At.After(f.At) {
			continue // the first delivery itself
		}
		for _, h := range hops[d.Client] {
			if f.At.After(h.at.Add(-lag)) && f.At.Before(h.at) &&
				d.At.After(h.at) && d.At.Before(h.at.Add(window)) && d.Node == h.node {
				out[k] = true
				break
			}
		}
	}
	return out
}

// history lists every delivery a subscriber saw of one message, for the
// duplicate violations to quote.
func history(l *Ledger, k msgKey) string {
	out := ""
	for _, o := range l.Deliveries {
		if o.Client == k.sub && o.Publisher == k.pub && o.Topic == k.topic && o.Seq == k.seq {
			out += fmt.Sprintf(" [%s id=%d qos=%d dup=%v node=%s same=%v after=%v]", o.At.Format("15:04:05.000"), o.PacketID, o.QoS, o.Dup, o.Node, o.SameExchange, o.AfterComplete)
		}
	}
	return out
}
