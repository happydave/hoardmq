package lease

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// TestTakeoverNotServedEmptyBelowASeenGrant: a takes x's lease from b at t1
// and pulls b, which holds nothing. While a's replica fallback is still out, b
// takes the lease back at t2 and pulls a, which notes t2. a's fallback then
// finds nothing. Served on its takeover, the client would get a fresh session
// at t1 while the lease has moved on to t2: it must be refused Server Busy,
// counted below a seen grant, with its count in flight ended.
func TestTakeoverNotServedEmptyBelowASeenGrant(t *testing.T) {
	holdWorkerBack(t)
	var gatePrev string
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	fixtureFallback = func(ctx context.Context, _, _, prev string) (*store.SessionState, bool) {
		if prev == gatePrev {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return nil, true
	}
	t.Cleanup(func() { fixtureFallback = nil })
	c := newCluster(t, []string{"n1", "n2", "n3"}, 2*time.Second, nil)
	b := c.settled()
	var a *tnode
	for _, n := range c.nodes {
		if n != b {
			a = n
			break
		}
	}
	if _, err := acquire(t, b, "x", nil, 300, true); err != nil {
		t.Fatal(err)
	}
	ownedBy(t, a, "x", b.name)
	gatePrev = b.name
	t.Cleanup(unblock)

	conn := acquireUnsettled(a, "x", 300)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition: a's takeover from b did not reach its fallback")
	}
	t1 := a.svc.tokens.Seen("default", "x")
	taken, err := acquire(t, b, "x", nil, 300, false)
	if err != nil || taken.PrevOwner != a.name {
		t.Fatalf("precondition: b's takeover from a: %+v, %v; want the lease taken from a", taken, err)
	}
	t2 := taken.Epoch
	waitFor(t, "a to note b's later grant", 5*time.Second, func() bool { return a.svc.tokens.Seen("default", "x") >= t2 })
	refusedBefore := a.svc.LivenessStats().BelowSeenGrantRefused
	unblock()
	o := outcomeOf(t, conn)
	o.res.Settle()
	st := a.svc.LivenessStats()
	t.Logf("t1 %d, t2 %d; a's CONNECT: err %v, previous owner %q, token %d, state %v, evidence %+v; a has seen %d; refused below a seen grant %d",
		t1, t2, o.err, o.res.PrevOwner, o.res.Epoch, o.res.State != nil, o.res.Empty, a.svc.tokens.Seen("default", "x"), st.BelowSeenGrantRefused)
	if o.err == nil && o.res.PrevOwner != b.name {
		t.Fatalf("precondition not reached: a's CONNECT did not return on the takeover branch from b: %+v", o.res)
	}
	if !refusedBusy(o.err) || st.BelowSeenGrantRefused != refusedBefore+1 {
		t.Fatalf("a's takeover answered %+v, %v after a saw b's later grant %d; want Server Busy, counted below a seen grant", o.res, o.err, t2)
	}
	if st.ConnectsInFlight != 0 {
		t.Fatalf("%d CONNECTs in flight after the refusal, want 0", st.ConnectsInFlight)
	}
}
