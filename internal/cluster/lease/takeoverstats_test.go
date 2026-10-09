package lease

import (
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TestTakeoverStatsCountThePullAndThePass: a takeover from a live owner is
// counted on both sides — the requester's pull with what it brought back, and
// the owner's request, pass and the parts of it that take the session
// manager's locks (WI 1691).
func TestTakeoverStatsCountThePullAndThePass(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	a, b := c.nodes["n1"], c.nodes["n2"]
	res, err := acquire(t, a, "x", nil, 300, true)
	if err != nil {
		t.Fatal(err)
	}
	a.sessions.put("default", "x", &store.SessionState{
		SessionMeta:   store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: res.Epoch},
		Subscriptions: []store.SubscriptionRecord{{Filter: "t/x", QoS: 1}},
	})
	if res, err = acquire(t, b, "x", nil, 300, false); err != nil || res.State == nil {
		t.Fatalf("takeover: %+v %v, want the state pulled", res, err)
	}

	got := b.svc.TakeoverStats()
	if got.Pulls != 1 || got.PullsWithState != 1 || got.PullsEmpty != 0 || got.PullsUnanswered != 0 || got.PullsSuperseded != 0 {
		t.Errorf("the requester's pull counters %+v, want one pull that came back with state", got)
	}
	if got.PullMsTotal <= 0 || got.PullMaxMs <= 0 {
		t.Errorf("the requester recorded no time for its pull: %+v", got)
	}
	// The pass runs on the owner's takeover queue, so it is not finished when
	// the requester's acquire returns; wait on the counter the pass writes.
	waitFor(t, "the previous owner's pass", 3*time.Second, func() bool { return a.svc.TakeoverStats().Passes == 1 })
	owner := a.svc.TakeoverStats()
	if owner.Requests != 1 || owner.Passes != 1 {
		t.Errorf("the owner's counters %+v, want one request served by one pass", owner)
	}
	if owner.PassMsTotal <= 0 || owner.QueueMsTotal < 0 {
		t.Errorf("the owner recorded no time for its pass: %+v", owner)
	}
	if owner.Pulls != 0 {
		t.Errorf("the owner recorded %d pulls of its own", owner.Pulls)
	}
}

// TestTakeoverPassSeparatesItsQueueWaitFromItsOwnTime: the queue wait is
// counted from the moment the request was handed to the queue, and the pass's
// own time from the moment a worker started it — the two must not be one
// figure, since it is the difference between "the owner was slow" and "the
// owner had four workers and a storm" (WI 1691).
func TestTakeoverPassSeparatesItsQueueWaitFromItsOwnTime(t *testing.T) {
	const queued, taking = 200 * time.Millisecond, 60 * time.Millisecond
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	n := c.nodes["n1"]
	n.sessions.put("default", "x", &store.SessionState{SessionMeta: store.SessionMeta{Namespace: "default", ClientID: "x", Epoch: 1}})
	n.sessions.setOnTake(func(string, string) { time.Sleep(taking) })

	k := key{"default", "x"}
	n.svc.serveTakeover(k, []any{pendingTakeover{
		req:      takeoverRequest{Req: 1, Namespace: k.ns, ClientID: k.id, Epoch: 2},
		peer:     "n2",
		queuedAt: time.Now().Add(-queued),
	}})

	st := n.svc.TakeoverStats()
	if st.Requests != 1 || st.Passes != 1 {
		t.Fatalf("counters after one pass %+v, want one request and one pass", st)
	}
	if st.QueueMsTotal < float64(queued/time.Millisecond) {
		t.Errorf("queue wait %.1f ms, want at least the %v the request was queued for: %+v", st.QueueMsTotal, queued, st)
	}
	if st.PassMsTotal >= float64(queued/time.Millisecond) {
		t.Errorf("the pass's own time %.1f ms includes the queue wait: %+v", st.PassMsTotal, st)
	}
	if st.TakeMsTotal < float64(taking/time.Millisecond) {
		t.Errorf("take %.1f ms, want at least the %v it slept: %+v", st.TakeMsTotal, taking, st)
	}
	if rest := st.PassMsTotal - st.TakeMsTotal - st.DetachMsTotal; rest >= float64(taking/time.Millisecond) {
		t.Errorf("the rest of the pass is %.1f ms, which is not less than the take it was meant to exclude: %+v", rest, st)
	}
}

// TestNotePullClassifiesEveryOutcome: the four ways a pull can end are
// counted apart. A storm produces all four — at 30,000 clients a run has
// thousands answered empty and hundreds unanswered — and they mean different
// things about the previous owner, so one of them absorbing another would
// misreport the wait rather than lose it.
func TestNotePullClassifiesEveryOutcome(t *testing.T) {
	var c takeoverCounters
	c.notePull(time.Millisecond, true, true, false)   // answered with state
	c.notePull(time.Millisecond, false, true, false)  // answered, nothing to give
	c.notePull(time.Millisecond, false, false, false) // no answer
	c.notePull(time.Millisecond, false, true, true)   // superseded
	c.notePull(time.Millisecond, true, true, true)    // superseded, state ignored
	st := c.stats()
	if st.Pulls != 5 || st.PullsWithState != 1 || st.PullsEmpty != 1 || st.PullsUnanswered != 1 || st.PullsSuperseded != 2 {
		t.Fatalf("outcomes %+v, want 5 pulls: 1 with state, 1 empty, 1 unanswered, 2 superseded", st)
	}
	if st.PullMsTotal < 5 || st.PullMaxMs < 1 {
		t.Fatalf("times %+v, want 5 ms in all and a longest of at least 1 ms", st)
	}
}
