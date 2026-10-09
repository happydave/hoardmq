package sim

import (
	"strings"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// deliveriesOf builds one subscriber's stream from one publisher; each step
// is a sequence number with the flags the client recorded for it.
type step struct {
	seq              uint64
	qos              byte
	id               uint16
	dup, same, after bool
}

func ledgerOf(steps ...step) *Ledger {
	l := &Ledger{}
	at := time.Unix(1_000, 0)
	for i, s := range steps {
		l.Deliveries = append(l.Deliveries, Delivery{Client: "sub", Node: "n1", Topic: "t", QoS: s.qos, Dup: s.dup, PacketID: s.id,
			Publisher: "pub", Seq: s.seq, At: at.Add(time.Duration(i) * time.Millisecond), SameExchange: s.same, AfterComplete: s.after})
	}
	return l
}

func violationsOf(l *Ledger) map[string][]string {
	out := map[string][]string{}
	for _, v := range Check(Evidence{Ledger: l}) {
		out[v.Property] = append(out[v.Property], v.Detail)
	}
	return out
}

// TestDeliveryPropertiesAgree drives the checker over a synthetic ledger: the
// two delivery properties must read a retransmission the same way, and must
// still catch a reordering and every repeat that is not one.
func TestDeliveryPropertiesAgree(t *testing.T) {
	cases := []struct {
		name          string
		steps         []step
		ordering, dup bool
	}{
		{"in order", []step{{seq: 1, qos: 1, id: 1}, {seq: 2, qos: 1, id: 2}, {seq: 3, qos: 1, id: 3}}, false, false},
		{"reordered", []step{{seq: 1, qos: 1, id: 1}, {seq: 3, qos: 1, id: 3}, {seq: 2, qos: 1, id: 2}}, true, false},
		{"QoS 2 re-sent under its open identifier, DUP clear", []step{{seq: 5, qos: 2, id: 9}, {seq: 5, qos: 2, id: 9, same: true}}, false, false},
		{"QoS 2 delivered again after its exchange completed", []step{{seq: 5, qos: 2, id: 9}, {seq: 5, qos: 2, id: 10, after: true}}, true, true},
		{"QoS 1 delivered twice without DUP", []step{{seq: 5, qos: 1, id: 81}, {seq: 5, qos: 1, id: 83}}, true, true},
		{"QoS 1 re-sent with DUP", []step{{seq: 5, qos: 1, id: 9}, {seq: 5, qos: 1, id: 9, dup: true}}, false, false},
	}
	for _, tc := range cases {
		v := violationsOf(ledgerOf(tc.steps...))
		if got := len(v[PropOrdering]) > 0; got != tc.ordering {
			t.Errorf("%s: ordering fired %v, want %v (%v)", tc.name, got, tc.ordering, v[PropOrdering])
		}
		if got := len(v[PropNoDuplicate]) > 0; got != tc.dup {
			t.Errorf("%s: no-double-delivery fired %v, want %v (%v)", tc.name, got, tc.dup, v[PropNoDuplicate])
		}
	}
}

// TestOrderingViolationCarriesTheDeliveries: the violation names the packet
// identifiers and flags of what it is about, so a sighting says which kind of
// repeat it was without a re-run.
func TestOrderingViolationCarriesTheDeliveries(t *testing.T) {
	v := violationsOf(ledgerOf(step{seq: 5, qos: 1, id: 81}, step{seq: 5, qos: 1, id: 83}))[PropOrdering]
	if len(v) != 1 || !strings.Contains(v[0], "id=81") || !strings.Contains(v[0], "id=83") || !strings.Contains(v[0], "qos=1") || !strings.Contains(v[0], "dup=false") {
		t.Fatalf("a repeated sequence: %v", v)
	}
	v = violationsOf(ledgerOf(step{seq: 1, qos: 1, id: 1}, step{seq: 3, qos: 1, id: 3}, step{seq: 2, qos: 1, id: 2}))[PropOrdering]
	if len(v) != 1 || !strings.Contains(v[0], "seq 2 [") || !strings.Contains(v[0], "id=2") || !strings.Contains(v[0], "seq 3 [") || !strings.Contains(v[0], "id=3") {
		t.Fatalf("a reordering: %v", v)
	}
}

// TestOneLiveOwnerIsNotVacuous: "no client was seen on two nodes" is also
// what a sampler that never looked reports.
func TestOneLiveOwnerIsNotVacuous(t *testing.T) {
	if v := ownerSamplingVacuous(0, 0); v == nil || v.Property != PropOneOwner {
		t.Fatalf("a sampler that never sampled: %v", v)
	}
	if v := ownerSamplingVacuous(40, 0); v == nil || v.Property != PropOneOwner {
		t.Fatalf("a sampler that saw nobody online: %v", v)
	}
	if v := ownerSamplingVacuous(40, 3); v != nil {
		t.Fatalf("a sampler that watched three clients: %v", v)
	}
}

// TestRefusedPublishIsNotAnAcknowledgedOne is WI 1308's first and third
// criteria. The two cases are each other's control: a checker that could not
// tell a refusal from an acknowledgement would fail one of them.
func TestRefusedPublishIsNotAnAcknowledgedOne(t *testing.T) {
	at := time.Unix(1_000, 0)
	held := []Subscription{{Client: "sub", Filter: "t", QoS: 1, Since: at.Add(-time.Second)}}

	refused := &Ledger{Subscriptions: held}
	refused.addErr(PublishFailure{Client: "pub", Node: "n1", Seq: 1, QoS: 1, Code: packet.ImplementationSpecificError, Err: "publish refused"})
	if v := violationsOf(refused); len(v[PropNoAckedLoss]) > 0 {
		t.Errorf("a refused publish that was never delivered reported an acknowledged loss: %v", v[PropNoAckedLoss])
	}

	accepted := &Ledger{Subscriptions: held}
	accepted.addPub(Publication{Publisher: "pub", Topic: "t", QoS: 1, Seq: 1, Code: packet.Success, AckedAt: at, Node: "n1"})
	if v := violationsOf(accepted); len(v[PropNoAckedLoss]) == 0 {
		t.Error("an acknowledged publish that was never delivered reported no acknowledged loss, so the property has no teeth")
	}
}

// TestPublishCountedOnce: the harness is one of the suspects whenever it
// reports a defect, and this is the accounting the original reading of WI
// 1308 accused it of getting wrong. The presence case is asserted first — a
// ledger where the same publish is in both — so the absence case cannot pass
// by the property being dead.
func TestPublishCountedOnce(t *testing.T) {
	both := &Ledger{}
	both.addPub(Publication{Publisher: "pub", Topic: "t", QoS: 1, Seq: 7, Code: packet.Success, Node: "n1"})
	both.addErr(PublishFailure{Client: "pub", Node: "n1", Seq: 7, QoS: 1, Code: packet.ImplementationSpecificError, Err: "publish refused"})
	v := violationsOf(both)
	if len(v[PropCountedOnce]) == 0 {
		t.Fatalf("a publish in both ledgers reported nothing: %v", v)
	}
	if !strings.Contains(v[PropCountedOnce][0], "pub seq 7 (n1)") {
		t.Errorf("the violation does not name the publish: %q", v[PropCountedOnce][0])
	}

	apart := &Ledger{}
	apart.addPub(Publication{Publisher: "pub", Topic: "t", QoS: 1, Seq: 7, Code: packet.Success, Node: "n1"})
	apart.addErr(PublishFailure{Client: "pub", Node: "n1", Seq: 8, QoS: 1, Code: packet.ImplementationSpecificError, Err: "publish refused"})
	if v := violationsOf(apart); len(v[PropCountedOnce]) > 0 {
		t.Errorf("one accepted and one refused publish reported double counting: %v", v[PropCountedOnce])
	}
}

// TestNoRefusalWithoutFaults covers the property's four readings: a faulted
// run is not judged, a fault-free run with a refusal is, a fault-free run
// with none is not, and a fault-free run that accepted nothing is a violation
// whether or not it refused anything — which is the property's non-vacuity.
func TestNoRefusalWithoutFaults(t *testing.T) {
	accepted := func() *Ledger {
		l := &Ledger{}
		l.addPub(Publication{Publisher: "pub", Topic: "t", QoS: 1, Seq: 1, Code: packet.Success, Node: "n1"})
		return l
	}
	withRefusal := func() *Ledger {
		l := accepted()
		l.addErr(PublishFailure{Client: "pub", Node: "n2", Seq: 2, QoS: 1, Code: packet.ImplementationSpecificError, Err: "publish refused"})
		return l
	}
	for _, c := range []struct {
		name      string
		ledger    *Ledger
		faultFree bool
		want      string // a substring the violation must carry, or "" for none
	}{
		{"a faulted run's refusals are not judged", withRefusal(), false, ""},
		{"a fault-free run with no refusal passes", accepted(), true, ""},
		{"a fault-free run with a refusal fails, naming the node and the reason", withRefusal(), true,
			"1 of 2 QoS 1/2 publishes refused with no fault injected; by node n2=1; by reason Implementation specific error"},
		{"a fault-free run that accepted nothing is not silent", &Ledger{}, true,
			"no QoS 1 or 2 publish was accepted"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			for _, v := range Check(Evidence{Ledger: c.ledger, FaultFree: c.faultFree}) {
				if v.Property == PropNoRefusalWithoutFaults {
					got = append(got, v.Detail)
				}
			}
			switch {
			case c.want == "" && len(got) > 0:
				t.Errorf("%s fired: %v", PropNoRefusalWithoutFaults, got)
			case c.want != "" && len(got) == 0:
				t.Errorf("%s did not fire; want a violation carrying %q", PropNoRefusalWithoutFaults, c.want)
			case c.want != "" && !strings.Contains(got[0], c.want):
				t.Errorf("%s = %q, want it to carry %q", PropNoRefusalWithoutFaults, got[0], c.want)
			}
		})
	}
}

// TestRefusalsReachTheRunStatisticsByNode is WI 1308's second criterion: a
// change in what a build refuses has to be visible in the corpus, not only on
// a cluster, and by node — one node refusing everything and every node
// refusing a little are different findings under the same total (WI 1502).
func TestRefusalsReachTheRunStatisticsByNode(t *testing.T) {
	l := &Ledger{}
	l.addPub(Publication{Publisher: "pub", Topic: "t", QoS: 1, Seq: 1, Code: packet.Success, Node: "n1"})
	l.addErr(PublishFailure{Client: "pub", Node: "n2", Seq: 2, QoS: 1, Code: packet.ImplementationSpecificError, Err: "publish refused"})
	l.addErr(PublishFailure{Client: "pub", Node: "n2", Seq: 3, QoS: 1, Code: packet.ImplementationSpecificError, Err: "publish refused"})
	st := Stats(Evidence{Ledger: l})
	if st["publishErrors"] != 2 || st["publishErrors.n2"] != 2 {
		t.Errorf("statistics %v, want 2 publish errors, both attributed to n2", st)
	}
	if st["publications"] != 1 {
		t.Errorf("statistics %v: publications counts accepted publishes, and one was accepted", st)
	}
}
