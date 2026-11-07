package replication

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
)

// misroutedSetup: under v1 n1 leads shard 0 and n2 follows; n1 and n2 apply
// v2, under which n2 leads and n1 follows, while n3 stays on v1 and so
// forwards the shard's retained writes to n1. It returns v2 and the topic.
func misroutedSetup(t *testing.T, h *harness) (shardmap.Map, string) {
	t.Helper()
	allAlive(h)
	h.applyMap(moveMap(h, 1, sh("n1", "n2"), sh("n2", "n3")))
	waitSettled(h, 1)
	v2 := moveMap(h, 2, sh("n2", "n1"), sh("n2", "n3"))
	for _, n := range []string{"n1", "n2"} {
		if _, err := h.nodes[n].table.Apply(0, shardmap.Encode(v2)); err != nil {
			t.Fatal(err)
		}
	}
	h.waitFor("n1 and n2 to reconcile v2", 5*time.Second, func() bool {
		return h.nodes["n1"].rep.Reconciled() == 2 && h.nodes["n2"].rep.Reconciled() == 2 &&
			h.nodes["n1"].rep.RetainedMovesPending() == 0 && h.nodes["n2"].rep.RetainedMovesPending() == 0
	})
	return v2, topicIn(2, 0)
}

// TestMisroutedRetainedWrite: n3's client publish, forwarded to n1, which no
// longer leads the shard, is refused, and nothing is left on n1. The control
// installs it as before: acknowledged, held on n1 only, served by nobody.
func TestMisroutedRetainedWrite(t *testing.T) {
	for _, c := range []struct {
		name    string
		control bool
	}{{"refused", false}, {"control: installed", true}} {
		t.Run(c.name, func(t *testing.T) {
			h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, func(o *Options) {
				quickAck(o)
				o.InstallMisrouted = c.control
			})
			_, tp := misroutedSetup(t, h)
			err := writeRetained(h, "n3", tp, "x")
			atLeader := h.nodes["n2"].rep.QueryRetained("default", tp, time.Now())
			t.Logf("n3's write on v1: err=%v reason=%q; n1 own %q, n2 own %q; a query at n2 finds %d",
				err, barrierReason(err), ownHolds(h, "n1", tp), ownHolds(h, "n2", tp), len(atLeader))
			if c.control {
				if err != nil || ownHolds(h, "n1", tp) != "x" || len(atLeader) != 0 {
					t.Fatal("control: want the write acknowledged, held on n1 only and served by nobody")
				}
				return
			}
			if err == nil {
				t.Fatal("the misrouted write was acknowledged")
			}
			if got := ownHolds(h, "n1", tp); got != "" {
				t.Fatalf("n1 holds %q of a shard it does not lead", got)
			}
			if got := sinks["n3"].get("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-refused", "source": SourceClient}); got != 1 {
				t.Fatalf("n3 misrouted-refused{client} %v, want 1", got)
			}
			if got := sinks["n3"].get("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-forwarded", "source": SourceClient}); got != 0 {
				t.Fatalf("a client's refused publish was forwarded again (%v)", got)
			}
		})
	}
}

// TestMisroutedHeldForwardIsForwardedOnce: a forward no barrier waits on (a
// Will, a module's publish, a QoS 0 publish) refused as misrouted is held. If
// n3 applies v2 within the acknowledgement timeout it is forwarded to v2's
// leader, which serves it; if not, it is counted dropped.
func TestMisroutedHeldForwardIsForwardedOnce(t *testing.T) {
	for _, source := range []string{SourceWill, SourceModule, SourceClientQoS0} {
		for _, c := range []struct {
			name    string
			applies bool
		}{{"map reached", true}, {"timeout", false}} {
			t.Run(source+"/"+c.name, func(t *testing.T) {
				h, sinks := withCountersAnd(t, []string{"n1", "n2", "n3"}, 2, 2, quickAck)
				v2, tp := misroutedSetup(t, h)
				m := &message.Message{Namespace: "default", Topic: tp, Payload: []byte("x"), Retain: true}
				if h.nodes["n3"].rep.StoreRetainedFrom(m, source) {
					t.Fatal("precondition: n3 stored the write locally")
				}
				label := sourceLabel(source)
				h.waitFor("the refusal to be counted", 5*time.Second, func() bool {
					return sinks["n3"].get("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-refused", "source": label}) == 1
				})
				if !c.applies {
					h.waitFor("the drop to be counted", 5*time.Second, func() bool {
						return sinks["n3"].get("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-dropped", "source": label}) == 1
					})
					if ownHolds(h, "n1", tp) != "" || ownHolds(h, "n2", tp) != "" {
						t.Fatal("a dropped forward is held somewhere")
					}
					return
				}
				if _, err := h.nodes["n3"].table.Apply(0, shardmap.Encode(v2)); err != nil {
					t.Fatal(err)
				}
				h.waitFor("v2's leader to hold the message", 5*time.Second, func() bool { return ownHolds(h, "n2", tp) == "x" })
				if got := sinks["n3"].get("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-forwarded", "source": label}); got != 1 {
					t.Fatalf("n3 misrouted-forwarded{%s} %v, want 1", label, got)
				}
				if got := sinks["n3"].get("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-dropped", "source": label}); got != 0 {
					t.Fatalf("n3 misrouted-dropped{%s} %v, want 0", label, got)
				}
			})
		}
	}
}

// TestMisroutedPredicates: the refusal and the re-forward, each refusal row
// included.
func TestMisroutedPredicates(t *testing.T) {
	for _, c := range []struct {
		leads, handOver, install, want bool
	}{
		{false, false, false, true},
		{true, false, false, false},
		{false, true, false, false},
		{false, false, true, false},
	} {
		if got := misroutedForward(c.leads, c.handOver, c.install); got != c.want {
			t.Errorf("misroutedForward(leads=%v, handOver=%v, install=%v) = %v, want %v", c.leads, c.handOver, c.install, got, c.want)
		}
	}
	for _, c := range []struct {
		name             string
		retried          bool
		mapV, refusalMap uint64
		leader           string
		send, wait       bool
	}{
		{"sends", false, 2, 2, "n2", true, false},
		{"waits for the map", false, 1, 2, "n1", false, true},
		{"retried once already", true, 2, 2, "n2", false, false},
		{"its map names the refuser", false, 2, 1, "n1", false, false},
		{"its map names this node", false, 2, 2, "n3", false, false},
		{"no leader", false, 2, 2, "", false, false},
	} {
		send, wait := reforward(c.retried, c.mapV, c.refusalMap, c.leader, "n3", "n1")
		if send != c.send || wait != c.wait {
			t.Errorf("%s: reforward = send %v wait %v, want %v %v", c.name, send, wait, c.send, c.wait)
		}
	}
}
