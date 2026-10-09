package sim

import (
	"fmt"
	"net"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// ProbePublish connects once with a throwaway identifier, publishes one QoS 1
// message to the given topic, and returns the acknowledgement's reason code.
//
// It is the smallest question that distinguishes a node which can make
// progress from one which cannot: a node whose replication stream is holed, or
// whose barrier cannot pass, answers a QoS 1 publish with an error code and
// keeps doing so — the outcome of every one of the four defects WI 1502 was
// filed from. Success and No Matching Subscribers both mean the node is
// working; the topic is deliberately one nobody subscribes to, so the second
// is the expected healthy answer.
func ProbePublish(addr, id, topic string) (packet.ReasonCode, error) {
	nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return 0, err
	}
	defer func() { _ = nc.Close() }()
	enc := &packet.Encoder{Version: packet.V5}
	dec := packet.NewDecoder(nc)
	dec.Version = packet.V5
	if err := nc.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return 0, err
	}
	if err := enc.Encode(nc, withCredentials(&packet.Connect{ProtocolLevel: packet.V5, ClientID: id, CleanStart: true})); err != nil {
		return 0, err
	}
	p, err := dec.Read()
	if err != nil {
		return 0, err
	}
	ack, ok := p.(*packet.Connack)
	if !ok {
		return 0, fmt.Errorf("expected CONNACK, got %T", p)
	}
	if ack.ReasonCode != packet.Success {
		return ack.ReasonCode, fmt.Errorf("connect refused: %s", ack.ReasonCode)
	}
	if err := enc.Encode(nc, &packet.Publish{Topic: topic, QoS: 1, PacketID: 1, Payload: []byte("liveness")}); err != nil {
		return 0, err
	}
	p, err = dec.Read()
	if err != nil {
		return 0, err
	}
	pub, ok := p.(*packet.Puback)
	if !ok {
		return 0, fmt.Errorf("expected PUBACK, got %T", p)
	}
	return pub.ReasonCode, nil
}

// livenessProbes asks each node whether it can still accept a QoS 1 publish.
// It is called once, after every fault has been healed, every stopped node
// restarted and the cluster settled, so a refusal at that point has no
// legitimate cause left.
//
// **It returns nothing unless every node is running**, and that is the whole
// difficulty of this property rather than a detail. With a replica down, the
// survivors refuse a QoS 1 publish with 0x83 *correctly* — the barrier cannot
// reach the missing peer — so probing a cluster that is not whole reports a
// violation for a node that is behaving exactly as designed. A liveness
// property that fires on correct behaviour would be turned off within a week,
// so when the cluster is not whole the property is not evaluated at all, and
// `livenessProbes` in the run's statistics reads 0 to say so.
// The bound comes from the cluster's own acknowledgement timeout rather than a
// constant: a barrier that cannot pass retries, a holed stream waits a
// cooldown of one acknowledgement timeout before resyncing, and a resync
// itself takes time. Eight of them is comfortably longer than one repair and
// far shorter than a run, and it moves with the product's configuration
// instead of drifting from it.
func livenessBound(c *Cluster) time.Duration {
	d := 8 * c.AckTimeout
	if d < 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

func livenessProbes(c *Cluster, topic string) []Probe {
	for _, n := range c.Nodes {
		if !n.Running() {
			return nil
		}
	}
	deadline := time.Now().Add(livenessBound(c))
	var out []Probe
	for _, n := range c.Nodes {
		// Retried until the bound, because the property is that a stream which
		// cannot make progress *does not stay that way* — not that it is
		// already fine the instant the last fault heals. A single probe fails
		// on a healthy build when a fault heals near the end of the window,
		// which is how this was found: the first version took one reading and
		// reported a node that was midway through a legitimate resync.
		var last Probe
		for {
			code, err := ProbePublish(n.TCP, "liveness-"+n.Name, topic)
			last = Probe{At: time.Now(), Node: n.Name, Code: code, Err: err}
			if err == nil && !code.IsError() {
				break
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		out = append(out, last)
	}
	return out
}

// livenessViolations reports every node that could not accept a publish once
// the cluster was whole again.
//
// The reason code is carried into the violation, because "it refused" and
// "which refusal" are different findings: 0x83 is a barrier that could not
// pass, while a fence or a lost quorum is another property's business and
// should not be silently folded in here.
func livenessViolations(probes []Probe, counters map[string]string, rec []NodeRecovery) []Violation {
	return livenessFindingsWith(probes, counters, rec)
}

// livenessFindings is the decision without a recovery record, as the tests
// that drive it directly call it.
func livenessFindings(probes []Probe, counters map[string]string) []Violation {
	return livenessFindingsWith(probes, counters, nil)
}

// livenessFindingsWith also carries each named node's refusing time since
// the heal, so the violation says how long and how much, not only that.
func livenessFindingsWith(probes []Probe, counters map[string]string, rec []NodeRecovery) []Violation {
	var out []Violation
	for _, p := range probes {
		switch {
		case p.Err != nil && p.Code == 0:
			// The probe never got an answer: the node did not accept a
			// connection or the exchange broke. Reported, because a node that
			// cannot be reached after everything has healed is the same
			// failure from the caller's side.
			out = append(out, Violation{PropRecovers, fmt.Sprintf(
				"%s could not answer a publish after the cluster was whole again: %v%s%s", p.Node, p.Err, barrierNote(counters, p.Node), recoveryNote(rec, p.Node))})
		case p.Code.IsError():
			out = append(out, Violation{PropRecovers, fmt.Sprintf(
				"%s refused a QoS 1 publish with %s after the cluster was whole again%s%s", p.Node, p.Code, barrierNote(counters, p.Node), recoveryNote(rec, p.Node))})
		}
	}
	return out
}

// barrierNote appends a node's replication barrier failures by peer and
// reason, so the violation says which stream could not make progress rather
// than leaving it to be reconstructed by hand as WI 1494's investigation had
// to.
func barrierNote(counters map[string]string, node string) string {
	if counters == nil {
		return ""
	}
	if s := counters[node]; s != "" {
		return "; barrier failures " + s
	}
	return ""
}
