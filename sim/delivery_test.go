package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/packet"
)

// TestClassifyDelivery: one case a class, and one a clause of the order. A
// refused publish is never a loss, a client told Session Present 0 is not
// counted again, and only a client that reconnected since the mark is a lost
// session.
func TestClassifyDelivery(t *testing.T) {
	resumed := deliveryFacts{Accepted: true, Code: packet.NoMatchingSubscribers, Recovered: true, Resumed: true}
	with := func(f func(*deliveryFacts)) deliveryFacts {
		d := resumed
		f(&d)
		return d
	}
	for _, tc := range []struct {
		name          string
		facts         deliveryFacts
		class, reason string
		caught        bool
	}{
		{"delivered, resumed", with(func(d *deliveryFacts) { d.Received, d.Code = true, packet.Success }), classDelivered, "", false},
		{"refused on every pass, resumed", with(func(d *deliveryFacts) { d.Accepted, d.Code = false, packet.ImplementationSpecificError }), classNeverAccepted, "Implementation specific error", false},
		{"no acknowledgement, resumed", with(func(d *deliveryFacts) { d.Accepted, d.Code, d.NoAck = false, 0, true }), classNeverAccepted, "none", false},
		{"not online", with(func(d *deliveryFacts) { d.Recovered = false }), classNotOnline, "", false},
		{"told Session Present 0", with(func(d *deliveryFacts) { d.Told = true }), classTold, "", false},
		{"resumed, no matching subscribers", resumed, classLost, reasonNoSubscriber, true},
		{"resumed, accepted and not delivered", with(func(d *deliveryFacts) { d.Code = packet.Success }), classLost, reasonUndelivered, true},
		{"held, no matching subscribers", with(func(d *deliveryFacts) { d.Resumed = false }), classHeld, reasonNoSubscriber, true},
	} {
		class, reason := classifyDelivery(tc.facts)
		if class != tc.class || reason != tc.reason {
			t.Errorf("%s: class %q reason %q, want %q %q", tc.name, class, reason, tc.class, tc.reason)
		}
		if got := controlCaught(class); got != tc.caught {
			t.Errorf("%s: a control in class %q caught %v, want %v", tc.name, class, got, tc.caught)
		}
	}
}

// TestDeliverySample: a sample is distinct indexes in range, fixed by its
// seed; no sample, or one as large as the population, is every client.
func TestDeliverySample(t *testing.T) {
	a := deliverySample(300, 100, 7)
	if len(a) != 100 {
		t.Fatalf("sample of 100 from 300: %d indexes", len(a))
	}
	seen := map[int]bool{}
	for _, i := range a {
		if i < 0 || i >= 300 || seen[i] {
			t.Fatalf("index %d out of range or repeated", i)
		}
		seen[i] = true
	}
	if b := deliverySample(300, 100, 7); fmt.Sprint(b) != fmt.Sprint(a) {
		t.Fatal("the same seed drew another sample")
	}
	if c := deliverySample(300, 100, 8); fmt.Sprint(c) == fmt.Sprint(a) {
		t.Fatal("another seed drew the same sample")
	}
	for _, sample := range []int{0, 300} {
		all := deliverySample(300, sample, 7)
		sort.Ints(all)
		if len(all) != 300 || all[0] != 0 || all[299] != 299 {
			t.Fatalf("sample %d: %d indexes, want every one of 300", sample, len(all))
		}
	}
}

// TestPublishPasses: a refused or unanswered publish is tried again in a
// later pass, an accepted one is not, the passes are bounded, and every
// refusal is counted by its code.
func TestPublishPasses(t *testing.T) {
	var mu sync.Mutex
	tries := map[int]int{}
	pauses := 0
	out := publishPasses(4, 2, func(_, k int) (packet.ReasonCode, error) {
		mu.Lock()
		tries[k]++
		n := tries[k]
		mu.Unlock()
		switch {
		case k == 1 && n < 3:
			return packet.ImplementationSpecificError, errors.New("refused")
		case k == 2:
			return packet.QuotaExceeded, errors.New("refused")
		case k == 3:
			return 0, errors.New("connection ended before the acknowledgement")
		}
		return packet.Success, nil
	}, func() bool { pauses++; return true })
	if !out.accepted[0] || tries[0] != 1 {
		t.Fatalf("client 0 accepted %v after %d tries, want accepted at the first", out.accepted[0], tries[0])
	}
	if !out.accepted[1] || tries[1] != 3 || out.code[1] != packet.Success {
		t.Fatalf("client 1 accepted %v after %d tries, code %v; want accepted at the third", out.accepted[1], tries[1], out.code[1])
	}
	if out.accepted[2] || out.code[2] != packet.QuotaExceeded || out.noAck[2] || tries[2] != deliveryPasses {
		t.Fatalf("client 2: accepted %v, code %v, no answer %v, %d tries; want refused with its code on all %d passes", out.accepted[2], out.code[2], out.noAck[2], tries[2], deliveryPasses)
	}
	if out.accepted[3] || !out.noAck[3] {
		t.Fatalf("client 3: accepted %v, no answer %v; want unanswered", out.accepted[3], out.noAck[3])
	}
	want := int64(1 + 3 + 2*deliveryPasses)
	if out.passes != deliveryPasses || pauses != deliveryPasses-1 || out.published != want {
		t.Fatalf("passes %d, pauses %d, published %d; want %d, %d, %d", out.passes, pauses, out.published, deliveryPasses, deliveryPasses-1, want)
	}
	if out.refusals["Implementation specific error"] != 2 || out.refusals["Quota exceeded"] != deliveryPasses || out.refusals["none"] != deliveryPasses {
		t.Fatalf("refusals by code %v", out.refusals)
	}
	stopped := publishPasses(1, 1, func(_, _ int) (packet.ReasonCode, error) { return 0, errors.New("no answer") }, func() bool { return false })
	if stopped.passes != 1 {
		t.Fatalf("a pause that stops: %d passes, want 1", stopped.passes)
	}
}

// TestDeliveryProblems: each clause of the check's own verdict refuses on its
// value alone; losses are findings and pass.
func TestDeliveryProblems(t *testing.T) {
	good := deliveryResult{Checked: 100, Delivered: 88, LostSessions: 2, ControlsAsked: 10, ControlsRemoved: 10, ControlsCaught: 10}
	if p := deliveryProblems(good); len(p) != 0 {
		t.Fatalf("a result with two losses and every control caught: %v", p)
	}
	for _, tc := range []struct {
		name string
		res  deliveryResult
		want string
	}{
		{"no control removed", deliveryResult{Checked: 100, Delivered: 100, ControlsAsked: 10}, "no negative control"},
		{"a control missed", deliveryResult{Checked: 100, Delivered: 97, ControlsAsked: 3, ControlsRemoved: 3, ControlsCaught: 2,
			Controls: []deliveryClient{{ID: "f000001", Class: classLost}, {ID: "f000002", Class: classDelivered}, {ID: "f000003", Class: classHeld}}}, "1 of 3 negative controls not counted lost: f000002 delivered"},
		{"nothing delivered", deliveryResult{Checked: 100, ControlsAsked: 10, ControlsRemoved: 10, ControlsCaught: 10}, "no checked client received"},
	} {
		p := deliveryProblems(tc.res)
		if len(p) != 1 || !strings.Contains(p[0], tc.want) {
			t.Errorf("%s: %v, want one problem naming %q", tc.name, p, tc.want)
		}
	}
}

// TestNameDeliveryNodes: every named client in the record carries its node's
// name from its address.
func TestNameDeliveryNodes(t *testing.T) {
	res := deliveryResult{Lost: []deliveryClient{{ID: "a", Node: "10.0.0.1:7061"}}, Held: []deliveryClient{{ID: "b", Node: "10.0.0.1:7064"}},
		Controls: []deliveryClient{{ID: "c", Node: "10.0.0.1:7067"}}}
	nameDeliveryNodes(&res, map[string]string{"10.0.0.1:7061": "n1", "10.0.0.1:7064": "n2", "10.0.0.1:7067": "n3"})
	if res.Lost[0].NodeName != "n1" || res.Held[0].NodeName != "n2" || res.Controls[0].NodeName != "n3" {
		t.Fatalf("names %q %q %q, want n1 n2 n3", res.Lost[0].NodeName, res.Held[0].NodeName, res.Controls[0].NodeName)
	}
	if r := scaleReport(&scaleRecord{Config: scaleConfig{DeliveryCheck: "all"}, Delivery: &res}); !strings.Contains(r, "- lost: a on n1 (10.0.0.1:7061)") {
		t.Fatalf("the report does not name the lost client with its node:\n%s", r)
	}
}

// TestScaleDeliveryCheckSetting: off by default, so a run without the setting
// is unchanged; all, or a sample no larger than the clients.
func TestScaleDeliveryCheckSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layout.json")
	data, _ := json.Marshal(goodLayout())
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{scaleLayoutEnv: path, scaleClientsEnv: "100000"}
	get := func(k string) string { return env[k] }
	if c, _, err := readScaleConfig(get); err != nil || c.DeliveryCheck != "off" || c.deliveryOn() {
		t.Fatalf("default delivery check %q (on %v), %v", c.DeliveryCheck, c.deliveryOn(), err)
	}
	for _, tc := range []struct {
		clients, value, check string
		sample                int
	}{{"100000", "off", "off", 0}, {"100000", "all", "all", 0}, {"1000000", "300000", "300000", 300000}, {"100000", "100000", "100000", 100000}} {
		env[scaleClientsEnv], env[scaleDeliveryEnv] = tc.clients, tc.value
		c, _, err := readScaleConfig(get)
		if err != nil || c.DeliveryCheck != tc.check || c.DeliverySample != tc.sample || c.deliveryOn() != (tc.check != "off") {
			t.Fatalf("%s at %s: %q sample %d on %v, %v", tc.value, tc.clients, c.DeliveryCheck, c.DeliverySample, c.deliveryOn(), err)
		}
	}
	env[scaleClientsEnv] = "100000"
	for _, bad := range []string{"0", "-1", "abc", "100001"} {
		env[scaleDeliveryEnv] = bad
		if _, _, err := readScaleConfig(get); err == nil || !strings.Contains(err.Error(), scaleDeliveryEnv) {
			t.Fatalf("delivery check %q: %v, want a refusal naming the variable", bad, err)
		}
	}
}

// TestDriverDeliveryCheck runs a small storm through the driver, then the
// delivery check: every control, made to lose its subscription while it still
// holds the filter, is counted lost with the node that served it, and every
// other client receives its message. A second deliver is refused.
func TestDriverDeliveryCheck(t *testing.T) {
	const clients, controls = 300, 5
	c := New(t, Options{})
	d := startPipedDriver(t, c, clients, testDriverOptions())
	t.Cleanup(d.pop.close)
	if _, err := d.pop.hello(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var completed atomic.Int64
	if failed, err := d.pop.fill(ctx, &completed); err != nil || failed != 0 {
		t.Fatalf("fill: %d failed, %v", failed, err)
	}
	perNode, err := d.pop.perNode()
	if err != nil {
		t.Fatal(err)
	}
	victim := c.Nodes[0]
	for _, n := range c.Nodes {
		if perNode[n.TCP] > perNode[victim.TCP] {
			victim = n
		}
	}
	if err := d.pop.mark(); err != nil {
		t.Fatal(err)
	}
	c.Crash(victim)
	if err := d.pop.crashed(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	w := newHoldWatch(clients, time.Second, deadline)
	for time.Now().Before(deadline) {
		s, err := d.pop.sample()
		if err != nil {
			t.Fatal(err)
		}
		if w.observe(s.At, s.Recovered) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, reason := w.verdict(); reason != recoveryReached {
		t.Fatalf("recovery not seen before the check: %q", reason)
	}
	if _, err := d.pop.finish(); err != nil {
		t.Fatal(err)
	}

	res, err := d.pop.deliver(ctx, deliveryRequest{Seed: 3, Controls: controls, WaitMs: 20_000})
	if err != nil {
		t.Fatal(err)
	}
	running := map[string]bool{}
	for _, n := range c.Running() {
		running[n.TCP] = true
	}
	if res.Checked != clients || res.ControlsRemoved != controls || res.ControlsCaught != controls || len(res.Controls) != controls {
		t.Fatalf("checked %d, controls removed %d, caught %d, listed %v; want %d, %d, %d", res.Checked, res.ControlsRemoved, res.ControlsCaught, res.Controls, clients, controls, controls)
	}
	// Client i dials node i mod 3 first, so the crashed node's clients are
	// the ones that reconnected: every control must be one of them.
	addrs := c.Addresses()
	victimIdx := -1
	for i, a := range addrs {
		if a == victim.TCP {
			victimIdx = i
		}
	}
	for _, ctl := range res.Controls {
		if ctl.Class != classLost || !running[ctl.Node] || (ctl.Reason != reasonNoSubscriber && ctl.Reason != reasonUndelivered) {
			t.Fatalf("control %+v: want a lost session on a running node with a loss reason", ctl)
		}
		var i int
		if _, err := fmt.Sscanf(ctl.ID, "f%06d", &i); err != nil || i%len(addrs) != victimIdx {
			t.Fatalf("control %s was not a client of the crashed node (index %d of %v): only a resumed client is a control", ctl.ID, victimIdx, addrs)
		}
	}
	if res.Delivered != clients-controls || res.LostSessions != 0 || res.HeldNotDelivered != 0 || res.Told != 0 || res.NotOnline != 0 || len(res.NeverAccepted) != 0 {
		t.Fatalf("delivered %d of %d, lost %d %v, held %d %v, told %d, not online %d, never accepted %v",
			res.Delivered, clients-controls, res.LostSessions, res.Lost, res.HeldNotDelivered, res.Held, res.Told, res.NotOnline, res.NeverAccepted)
	}
	if p := deliveryProblems(res); len(p) != 0 {
		t.Fatalf("a check that caught every control: %v", p)
	}
	// After the check, the driver answers for any client by name, as often as
	// asked: a control was resumed with Session Present 1 on a running node,
	// its first CONNACK the fill's.
	for range 2 {
		recs, err := d.pop.connacks([]string{res.Controls[0].ID, "nobody"})
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 2 || recs[1].ID != "nobody" || recs[1].Found {
			t.Fatalf("records %+v: want the control's and nobody not found", recs)
		}
		c := recs[0]
		if !c.Found || c.ID != res.Controls[0].ID || c.Seen < 2 || len(c.Last) < 2 {
			t.Fatalf("the control's record %+v: want found with the fill's and the resume's CONNACKs", c)
		}
		last := c.Last[len(c.Last)-1]
		if last.Code != "Success" || !last.SessionPresent || !running[last.Node] || (c.Seen <= connackRing && c.Last[0].Node != victim.TCP) {
			t.Fatalf("the control's %d CONNACKs %+v: want its last Success with Session Present on a running node, and the fill's from the crashed %s while kept", c.Seen, c.Last, victim.TCP)
		}
	}
	if _, err := d.pop.deliver(ctx, deliveryRequest{Controls: 1, WaitMs: 1000}); err == nil || !strings.Contains(err.Error(), "unexpected message \"deliver\" while waiting for \"close\"") {
		t.Fatalf("a second deliver: %v, want a refusal naming it", err)
	}
}
