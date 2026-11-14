package routing

import (
	"errors"
	"testing"
	"time"
)

// A held withdrawal's timer can fire just as Observe stops it: Stop then
// reports false and the callback runs anyway, after whatever Observe and the
// calls behind it did. If the client has returned, or left again, by then, the
// late callback must change nothing: a newer hold's timer and moving state
// stay, and no withdrawal is written.
//
// The tests call the late callback themselves, with its hold, at the point the
// scheduler can put it.

// staleHeldRace runs the sequence on a plane whose grace will not elapse
// under test: the filter advertised, a hold, a return, a second hold, and,
// with late, the first hold's callback run after all of them.
func staleHeldRace(t *testing.T, late bool) *Plane {
	t.Helper()
	p := movingPlane(t, time.Hour)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	first := heldFor(p, "t/0")
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	if late {
		p.emitHeld("default", "t/0", first)
	}
	return p
}

// heldFor is the hold registered for a filter now.
func heldFor(p *Plane, filter string) *heldWithdrawal {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.held[routeID{"default", filter}]
}

func staleHeldCases(t *testing.T, body func(t *testing.T, p *Plane)) {
	for _, c := range []struct {
		name string
		late bool
	}{{"the first hold's callback runs late", true}, {"no late callback (control)", false}} {
		t.Run(c.name, func(t *testing.T) { body(t, staleHeldRace(t, c.late)) })
	}
}

// TestALateHeldCallbackLeavesTheNewerHoldInPlace: the second hold's timer is
// still the one a later Observe would stop, and its move still answers for
// the filter.
func TestALateHeldCallbackLeavesTheNewerHoldInPlace(t *testing.T) {
	staleHeldCases(t, func(t *testing.T, p *Plane) {
		id := routeID{"default", "t/0"}
		p.mu.Lock()
		held, moving := p.held[id] != nil, p.moving[id]
		p.mu.Unlock()
		if !held {
			t.Error("the second hold's timer is no longer tracked: a return inside its grace cannot stop it")
		}
		if moving.IsZero() {
			t.Error("the second hold's move was ended by the first hold's callback")
		}
		if !p.table.MovingMatch("default", "t/0") {
			t.Error("the moving index no longer answers for the filter inside the second hold's grace")
		}
	})
}

// TestAPublishHereIsRefusedThroughoutTheNewerHoldsGrace: the client's session
// has left this node again and its new owner's route has not arrived, so a
// QoS 1 publish made here reaches no session. It must be refused, not
// acknowledged as delivered with nowhere to go.
func TestAPublishHereIsRefusedThroughoutTheNewerHoldsGrace(t *testing.T) {
	staleHeldCases(t, func(t *testing.T, p *Plane) {
		own := p.table.Own()
		if len(own) != 1 || own[0].Tombstone {
			t.Fatalf("the premise: the route is still advertised during the second hold's grace: %+v", own)
		}
		if sent, err := p.Forward(qos1On("t/0")); !errors.Is(err, ErrRoutesUnavailable) {
			t.Fatalf("a QoS 1 publish here for a session that has left was answered (%d sent, %v), want a refusal", sent, err)
		}
	})
}

// TestALateHeldCallbackAfterAReturnWritesNothing: the client came back and
// the hold was stopped too late; nothing holds the filter now. The late
// callback writes no withdrawal, sends nothing, and is counted superseded.
func TestALateHeldCallbackAfterAReturnWritesNothing(t *testing.T) {
	m := newLabelMetrics()
	p := heldPlane(t, time.Hour, m)
	p.Observe("default", "t/0", 1)
	handOver(p, "t/0")
	first := heldFor(p, "t/0")
	p.Observe("default", "t/0", 1)
	sent := m.get("hoardmq_route_updates_total", "direction", "out")
	before := p.version.Load()
	p.emitHeld("default", "t/0", first)

	if got := p.table.OwnCount("default", "t/0"); got != 1 {
		t.Fatalf("the returned session's filter is advertised with count %d, want 1: %+v", got, p.table.Own())
	}
	if got := p.version.Load(); got != before {
		t.Fatalf("a late callback merged an own entry (version %d to %d)", before, got)
	}
	if got := m.get("hoardmq_route_updates_total", "direction", "out"); got != sent {
		t.Fatalf("a late callback sent %v route updates, want none", got-sent)
	}
	if got := m.get("hoardmq_route_withdrawals_superseded_total"); got != 1 {
		t.Fatalf("superseded withdrawals counted %v, want 1", got)
	}
}

func TestHeldIsCurrent(t *testing.T) {
	a, b := &heldWithdrawal{}, &heldWithdrawal{}
	for _, c := range []struct {
		name             string
		registered, mine *heldWithdrawal
		want             bool
	}{
		{"its own hold registered", a, a, true},
		{"another hold registered", b, a, false},
		{"none registered", nil, a, false},
	} {
		if got := heldIsCurrent(c.registered, c.mine); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}
