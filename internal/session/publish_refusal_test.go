package session

import (
	"errors"
	"testing"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
)

// refusingForwarder stands for a routing plane that cannot answer for a
// publish — because a subscriber's node is down, or because an owner's
// routes are unknown to this node (WI 1351) — or, with sent above zero and
// no error, one that forwarded the message to other nodes.
type refusingForwarder struct {
	sent int
	err  error
}

func (f refusingForwarder) Forward(*message.Message) (int, error) { return f.sent, f.err }

// TestRefusedForwardBeatsNoMatchingSubscribers pins the order of two
// outcomes decided in the same function. When no local subscriber matched
// and the forward was refused, the client must see 0x83 and not 0x10: "no
// subscriber matched" is a claim this node is not entitled to make while it
// cannot account for another owner's routes. The QoS 0 case is included
// because it has no acknowledgement to be wrong about.
func TestRefusedForwardBeatsNoMatchingSubscribers(t *testing.T) {
	m, _ := newManager(t, func(o *Options) {
		o.Forwarder = refusingForwarder{err: errors.New("routing: routes unknown")}
	})
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)

	if code := publish(m, pub, "t", 1, 1, "x"); code != packet.ImplementationSpecificError {
		t.Fatalf("a refused forward must answer 0x83, not %s", code)
	}
	if code := publish(m, pub, "t", 2, 2, "x"); code != packet.ImplementationSpecificError {
		t.Fatalf("QoS 2 must answer 0x83 too, not %s", code)
	}
	if code := publish(m, pub, "t", 0, 0, "x"); code != packet.Success {
		t.Fatalf("QoS 0 carries no acknowledgement to refuse: %s", code)
	}
}

// TestAcceptedForwardStillReportsNoMatchingSubscribers is the control for
// the test above: with the same absence of local subscribers and a
// forwarder that accepts, 0x10 is the right answer and must survive.
func TestAcceptedForwardStillReportsNoMatchingSubscribers(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.Forwarder = refusingForwarder{err: nil} })
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	if code := publish(m, pub, "t", 1, 1, "x"); code != packet.NoMatchingSubscribers {
		t.Fatalf("an accepted forward with no local match must answer 0x10, not %s", code)
	}
}

// TestForwardedMatchIsNotNoMatchingSubscribers: with no local subscriber and
// a forward sent to another node, the publish reached a subscriber and must
// be acknowledged Success, not 0x10 (WI 1487).
func TestForwardedMatchIsNotNoMatchingSubscribers(t *testing.T) {
	m, _ := newManager(t, func(o *Options) { o.Forwarder = refusingForwarder{sent: 1} })
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	if code := publish(m, pub, "t", 1, 1, "x"); code != packet.Success {
		t.Fatalf("QoS 1 with a forwarded match must answer Success, not %s", code)
	}
	if code := publish(m, pub, "t", 2, 2, "x"); code != packet.Success {
		t.Fatalf("QoS 2 with a forwarded match must answer Success, not %s", code)
	}
}

// TestRefusedForwardBeatsAForwardedMatch: a forward that was sent but
// refused still answers 0x83; the count cannot turn a refusal into Success.
func TestRefusedForwardBeatsAForwardedMatch(t *testing.T) {
	m, _ := newManager(t, func(o *Options) {
		o.Forwarder = refusingForwarder{sent: 1, err: errors.New("routing: a subscriber's node is down")}
	})
	pub := newFake("pub", packet.V5, 100)
	connectV5(m, pub, true, 0, nil)
	if code := publish(m, pub, "t", 1, 1, "x"); code != packet.ImplementationSpecificError {
		t.Fatalf("a refused forward must answer 0x83 whatever it sent, not %s", code)
	}
}
