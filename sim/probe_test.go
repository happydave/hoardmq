package sim

import (
	"strings"
	"testing"
	"time"
)

// newTestProbe builds a probe with no broker behind it: the pairs' clients
// exist so their identifiers and delivery records can be driven directly.
func newTestProbe(t *testing.T, pairs int) *probe {
	t.Helper()
	p := &probe{pairs: pairs, rate: time.Second, drain: 0, stop: make(chan struct{}),
		acked: map[string]map[uint64]bool{}, refused: map[string]int64{},
		ackedAt: map[string]map[uint64]ackMark{}, events: map[string][]Event{},
		nodeName: func(addr string) string { return addr }}
	for i := 0; i < pairs; i++ {
		pub := NewClient("probe-p", []string{"127.0.0.1:1"}, 60, int64(i))
		sub := NewClient("probe-s", []string{"127.0.0.1:1"}, 60, int64(i))
		p.pubs, p.subs = append(p.pubs, pub), append(p.subs, sub)
		p.acked[pub.ID] = map[uint64]bool{}
		p.ackedAt[pub.ID] = map[uint64]ackMark{}
	}
	return p
}

// A message the broker acknowledged and the subscriber never received is the
// loss the owner's second gating figure is about, and the probe names it.
func TestProbeCountsAnAckedMessageNeverDelivered(t *testing.T) {
	crash := time.Now()
	p := newTestProbe(t, 1)
	p.attempts = 2
	p.acked["probe-p"][1] = true
	p.acked["probe-p"][2] = true
	// Only the first was delivered.
	p.subs[0].deliveries = []Delivery{{Client: "probe-s", Publisher: "probe-p", Seq: 1, Topic: probeTopic(0)}}
	rec := p.result(crash, crash.Add(time.Second))
	if rec.Acked != 2 {
		t.Fatalf("acked %d, want 2", rec.Acked)
	}
	if rec.AckedNotDelivered != 1 {
		t.Fatalf("acked_not_delivered %d, want 1", rec.AckedNotDelivered)
	}
	if len(rec.Examples) != 1 || !strings.Contains(rec.Examples[0], "seq 2") {
		t.Fatalf("the lost message is not named: %v", rec.Examples)
	}
}

// A lost message is attributed: the node that answered it, when, and the
// pair's connection events and deliveries around it (WI 2259).
func TestProbeAttributesALostMessage(t *testing.T) {
	crash := time.Now()
	p := newTestProbe(t, 1)
	p.attempts = 3
	for seq := uint64(1); seq <= 3; seq++ {
		p.acked["probe-p"][seq] = true
		p.ackedAt["probe-p"][seq] = ackMark{at: crash.Add(time.Duration(seq) * time.Second), node: "n2"}
	}
	p.ackedAt["probe-p"][4] = ackMark{at: crash.Add(4 * time.Second), node: "n3", noSubs: true}
	p.events["probe-s"] = []Event{
		{Kind: "dropped", Node: "n1", At: crash.Add(1500 * time.Millisecond)},
		{Kind: "connack", Node: "n3", Present: true, At: crash.Add(2500 * time.Millisecond)},
		{Kind: "connack", Node: "n3", At: crash.Add(time.Hour)},
	}
	p.subs[0].deliveries = []Delivery{{Publisher: "probe-p", Seq: 1, Node: "n1", At: crash.Add(time.Second)},
		{Publisher: "probe-p", Seq: 3, Node: "n3", At: crash.Add(3 * time.Second)}}
	rec := p.result(crash, crash.Add(time.Second))
	if len(rec.Losses) != 1 {
		t.Fatalf("losses %+v, want seq 2 alone", rec.Losses)
	}
	l := rec.Losses[0]
	if l.Seq != 2 || l.Node != "n2" || l.AtSec != 2 || l.Subscriber != "probe-s" || l.Topic != probeTopic(0) {
		t.Fatalf("loss %+v, want seq 2 answered by n2 at 2 s", l)
	}
	if len(l.SubEvents) != 2 || l.SubEvents[1].Node != "n3" || !l.SubEvents[1].Present {
		t.Fatalf("subscriber events %+v, want the drop and the CONNACK inside the window, not the one an hour on", l.SubEvents)
	}
	if len(l.Delivered) != 2 || l.Delivered[1].Seq != 3 || l.Delivered[1].Node != "n3" {
		t.Fatalf("deliveries %+v, want seqs 1 and 3 with their nodes", l.Delivered)
	}
	if len(rec.NoSubscriberMarks) != 1 || rec.NoSubscriberMarks[0].Seq != 4 || rec.NoSubscriberMarks[0].Node != "n3" {
		t.Fatalf("no-subscriber marks %+v, want seq 4 by n3", rec.NoSubscriberMarks)
	}
}

// The same probe with everything delivered reports no loss, so the figure
// above is not something the probe reports whatever happens.
func TestProbeReportsNoLossWhenEverythingArrived(t *testing.T) {
	crash := time.Now()
	p := newTestProbe(t, 1)
	p.attempts = 2
	p.acked["probe-p"][1] = true
	p.acked["probe-p"][2] = true
	p.subs[0].deliveries = []Delivery{
		{Publisher: "probe-p", Seq: 1}, {Publisher: "probe-p", Seq: 2}}
	if rec := p.result(crash, crash.Add(time.Second)); rec.AckedNotDelivered != 0 {
		t.Fatalf("acked_not_delivered %d over two delivered messages", rec.AckedNotDelivered)
	}
}

// Refusals are split at the recovery instant: a storm refuses by design, and
// the question the gate asks is whether the cluster is still refusing once it
// has recovered.
func TestProbeSplitsRefusalsAtTheInstant(t *testing.T) {
	crash := time.Now()
	instant := crash.Add(10 * time.Second)
	p := newTestProbe(t, 1)
	p.attempts = 4
	p.refused["0x83"] = 3
	p.errs = 1
	p.bad = []probeEvent{
		{at: crash.Add(2 * time.Second), code: "0x83"},
		{at: crash.Add(9 * time.Second), code: "0x83"},
		{at: crash.Add(12 * time.Second), code: "0x83"},
		{at: crash.Add(13 * time.Second)},
	}
	rec := p.result(crash, instant)
	if rec.Refused["0x83"] != 3 {
		t.Fatalf("refused %v, want 3 in total", rec.Refused)
	}
	if rec.RefusedAfterRecovery["0x83"] != 1 || rec.ErrorsAfterRecovery != 1 {
		t.Fatalf("after the instant: %v refused, %d errors; want 1 and 1", rec.RefusedAfterRecovery, rec.ErrorsAfterRecovery)
	}
	if rec.LastRefusalSec < 11.9 || rec.LastRefusalSec > 12.1 {
		t.Fatalf("last refusal at %v, want about 12", rec.LastRefusalSec)
	}
}

// A run that never recovered has no instant to split at, and everything the
// probe saw is before it rather than after it.
func TestProbeWithNoRecoveryReportsNothingAfterIt(t *testing.T) {
	crash := time.Now()
	p := newTestProbe(t, 1)
	p.attempts = 1
	p.refused["0x83"] = 1
	p.bad = []probeEvent{{at: crash.Add(time.Second), code: "0x83"}}
	rec := p.result(crash, time.Time{})
	if len(rec.RefusedAfterRecovery) != 0 {
		t.Fatalf("refusals after an instant that does not exist: %v", rec.RefusedAfterRecovery)
	}
}

// A probe that published nothing reports that, not a clean bill. A property
// that passes over an idle system verifies nothing.
func TestProbeLineIsNotVacuous(t *testing.T) {
	if got := probeLine(nil); got != "not run" {
		t.Fatalf("no probe: %q", got)
	}
	got := probeLine(&probeRecord{Pairs: 100})
	if !strings.Contains(got, "published nothing") {
		t.Fatalf("a probe with no attempts reads as %q", got)
	}
	full := probeLine(&probeRecord{Pairs: 2, RateMs: 1000, Attempts: 10, Acked: 9,
		Refused: map[string]int64{"0x83": 1}, LastRefusalSec: 4})
	if !strings.Contains(full, "1 refused 0x83") || !strings.Contains(full, "none once recovered") {
		t.Fatalf("the row does not carry its refusals: %q", full)
	}
}
