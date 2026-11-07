package module

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/hook"
	"github.com/happydave/hoardmq/internal/packet"
)

// clusterStub is a cluster listener that reports each event on got. With
// hold set, every call first signals entered and then waits for hold to
// close; panics makes that many calls panic first.
type clusterStub struct {
	stub
	got     chan ClusterEvent
	hold    chan struct{}
	entered chan struct{}
	panics  atomic.Int32
}

func (c *clusterStub) SetEnabled(b bool) { c.enabled = b }
func (c *clusterStub) OnClusterEvent(_ context.Context, ev ClusterEvent) {
	if c.hold != nil {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		<-c.hold
	}
	if c.panics.Add(-1) >= 0 {
		panic("boom")
	}
	c.got <- ev
}

// clusterStubs holds the instance each registered factory returns, so a
// test can steer the module the registry instantiates.
var clusterStubs = map[ID]*clusterStub{}

func newClusterStub(t *testing.T, id ID, log *[]string) *clusterStub {
	t.Helper()
	c := &clusterStub{stub: stub{id: id, caps: CapCluster, started: log}, got: make(chan ClusterEvent, 4096)}
	clusterStubs[id] = c
	reg(t, id, func() Module { return clusterStubs[id] })
	return c
}

type unsubscribeStub struct {
	stub
	refuse bool
	calls  *[]string
}

func (u *unsubscribeStub) InterceptUnsubscribe(_ context.Context, _ hook.Info, filter string) hook.Outcome {
	*u.calls = append(*u.calls, string(u.id)+" "+filter)
	if u.refuse {
		return hook.Outcome{Action: hook.Drop, Reason: packet.NotAuthorized}
	}
	return hook.OK
}

type unsubscribeOnly struct{ stub }

func (*unsubscribeOnly) InterceptUnsubscribe(context.Context, hook.Info, string) hook.Outcome {
	return hook.OK
}

type clusterOnly struct{ stub }

func (*clusterOnly) OnClusterEvent(context.Context, ClusterEvent) {}

func receive(t *testing.T, c *clusterStub) ClusterEvent {
	t.Helper()
	select {
	case ev := <-c.got:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatalf("%s received no event", c.id)
	}
	return ClusterEvent{}
}

func clusterRegistry(t *testing.T, policy hook.Policy, ids ...string) *Registry {
	t.Helper()
	var log []string
	reg(t, "store.cl", func() Module { return &stub{id: "store.cl", started: &log} })
	reg(t, "metrics.cl", func() Module { return &stub{id: "metrics.cl", started: &log} })
	r, problems := New(cfgWith(append(ids, "store.cl", "metrics.cl")...), ctx(), policy)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

// TestUnsubscribeAndClusterCapabilities: each new family's bit and method go
// together, both ways.
func TestUnsubscribeAndClusterCapabilities(t *testing.T) {
	var log []string
	reg(t, "hook.un1", func() Module { return &stub{id: "hook.un1", caps: CapUnsubscribe, started: &log} })
	reg(t, "hook.un2", func() Module { return &unsubscribeOnly{stub{id: "hook.un2", started: &log}} })
	reg(t, "hook.cl1", func() Module { return &stub{id: "hook.cl1", caps: CapCluster, started: &log} })
	reg(t, "hook.cl2", func() Module { return &clusterOnly{stub{id: "hook.cl2", started: &log}} })
	reg(t, "store.uc", func() Module { return &stub{id: "store.uc", started: &log} })
	reg(t, "metrics.uc", func() Module { return &stub{id: "metrics.uc", started: &log} })
	want := map[string]string{
		"hook.un1": "declares the unsubscribe capability without implementing it",
		"hook.un2": "implements the unsubscribe hooks without declaring the capability",
		"hook.cl1": "declares the cluster capability without implementing it",
		"hook.cl2": "implements the cluster hooks without declaring the capability",
	}
	for id, msg := range want {
		_, problems := New(cfgWith(id, "store.uc", "metrics.uc"), ctx(), hook.Policy{})
		if len(problems) != 1 || problems[0].String() != "/modules/"+id+": "+msg {
			t.Errorf("%s: %v, want %q", id, problems, msg)
		}
	}
	reg(t, "hook.un3", func() Module {
		return &unsubscribeOnly{stub{id: "hook.un3", caps: CapUnsubscribe, started: &log}}
	})
	reg(t, "hook.cl3", func() Module { return &clusterOnly{stub{id: "hook.cl3", caps: CapCluster, started: &log}} })
	r, problems := New(cfgWith("hook.un3", "hook.cl3", "store.uc", "metrics.uc"), ctx(), hook.Policy{})
	if r == nil || len(problems) != 0 {
		t.Fatalf("modules declaring and implementing the families: %v", problems)
	}
	if r.Hooks().Empty() || !r.Hooks().Clustering() {
		t.Fatalf("Empty %v, Clustering %v", r.Hooks().Empty(), r.Hooks().Clustering())
	}
}

// TestUnsubscribeChain: the first Drop in the module order ends the chain.
func TestUnsubscribeChain(t *testing.T) {
	var calls, log []string
	reg(t, "hook.uza", func() Module {
		return &unsubscribeStub{stub: stub{id: "hook.uza", caps: CapUnsubscribe, started: &log}, refuse: true, calls: &calls}
	})
	reg(t, "hook.uab", func() Module {
		return &unsubscribeStub{stub: stub{id: "hook.uab", caps: CapUnsubscribe, started: &log}, calls: &calls}
	})
	reg(t, "store.uo", func() Module { return &stub{id: "store.uo", started: &log} })
	reg(t, "metrics.uo", func() Module { return &stub{id: "metrics.uo", started: &log} })
	for _, c := range []struct {
		order []string
		want  string
	}{
		{nil, "hook.uab f; hook.uza f"},
		{[]string{"hook.uza"}, "hook.uza f"},
	} {
		doc := cfgWith("hook.uza", "hook.uab", "store.uo", "metrics.uo")
		doc.Hooks.Order = c.order
		r, problems := New(doc, ctx(), hook.Policy{})
		if len(problems) != 0 {
			t.Fatal(problems)
		}
		calls = calls[:0]
		out := r.Hooks().Unsubscribe(context.Background(), hook.Info{}, "f")
		if out.Action != hook.Drop || out.Reason != packet.NotAuthorized {
			t.Errorf("order %q: %+v, want the refusal", c.order, out)
		}
		if got := strings.Join(calls, "; "); got != c.want {
			t.Errorf("order %q: calls %q, want %q", c.order, got, c.want)
		}
	}
}

// TestClusterEventsReachEachEnabledListener: an event reaches every enabled
// listener whole, and a disabled one receives nothing and counts nothing.
func TestClusterEventsReachEachEnabledListener(t *testing.T) {
	var log []string
	a := newClusterStub(t, "hook.cea", &log)
	b := newClusterStub(t, "hook.ceb", &log)
	r := clusterRegistry(t, hook.Policy{}, "hook.cea", "hook.ceb")
	defer func() { _ = r.Stop(context.Background()) }()
	ev := ClusterEvent{Kind: LeaseMoved, Time: time.Unix(5, 0), Namespace: "default", ClientID: "c1", From: "a", To: "b"}
	r.Hooks().Cluster(ev)
	for _, c := range []*clusterStub{a, b} {
		if got := receive(t, c); got != ev {
			t.Errorf("%s received %+v, want %+v", c.id, got, ev)
		}
	}
	if !r.SetEnabled("hook.ceb", false) {
		t.Fatal("toggle refused")
	}
	r.Hooks().Cluster(ClusterEvent{Kind: MemberLeft, Member: "c"})
	if got := receive(t, a); got.Kind != MemberLeft || got.Member != "c" {
		t.Errorf("hook.cea received %+v", got)
	}
	select {
	case got := <-b.got:
		t.Errorf("a disabled listener received %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	if d := r.Hooks().ClusterDropped(); d["hook.ceb"] != 0 || d["hook.cea"] != 0 {
		t.Errorf("dropped %v", d)
	}
}

// TestClusterEnqueueNeverBlocks: with the listener held in its call, 2,000
// events return without waiting for it; 1,024 are queued and the rest
// counted dropped for that module. Stop drops and counts the queued ones,
// and an event after Stop is counted too.
func TestClusterEnqueueNeverBlocks(t *testing.T) {
	var log []string
	c := newClusterStub(t, "hook.ceh", &log)
	c.hold, c.entered = make(chan struct{}), make(chan struct{}, 1)
	r := clusterRegistry(t, hook.Policy{}, "hook.ceh")
	h := r.Hooks()
	h.Cluster(ClusterEvent{Kind: DrainStarted, Member: "a"})
	select {
	case <-c.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the listener was never called")
	}
	done := make(chan time.Duration, 1)
	go func() {
		began := time.Now()
		for range 2000 {
			h.Cluster(ClusterEvent{Kind: MemberJoined, Member: "x"})
		}
		done <- time.Since(began)
	}()
	select {
	case took := <-done:
		t.Logf("2,000 enqueues took %s with the listener held", took)
	case <-time.After(5 * time.Second):
		close(c.hold)
		t.Fatal("the enqueues waited for the held listener")
	}
	if n := h.ClusterDropped()["hook.ceh"]; n != 2000-clusterQueueSize {
		t.Fatalf("dropped %d, want %d", n, 2000-clusterQueueSize)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := h.ClusterDropped()["hook.ceh"]; n != 2000 {
		t.Fatalf("after Stop dropped %d, want 2000", n)
	}
	h.Cluster(ClusterEvent{Kind: MemberJoined, Member: "y"})
	if n := h.ClusterDropped()["hook.ceh"]; n != 2001 {
		t.Fatalf("after an event past Stop dropped %d, want 2001", n)
	}
	close(c.hold)
	if got := receive(t, c); got.Kind != DrainStarted {
		t.Fatalf("the held call delivered %+v", got)
	}
	select {
	case got := <-c.got:
		t.Fatalf("delivered after Stop: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestClusterListenerPanic: a panic is a hook failure, and the worker goes on
// to the next event.
func TestClusterListenerPanic(t *testing.T) {
	var log []string
	c := newClusterStub(t, "hook.cep", &log)
	c.panics.Store(1)
	r := clusterRegistry(t, hook.Policy{}, "hook.cep")
	defer func() { _ = r.Stop(context.Background()) }()
	r.Hooks().Cluster(ClusterEvent{Kind: MemberJoined, Member: "p"})
	r.Hooks().Cluster(ClusterEvent{Kind: MemberJoined, Member: "q"})
	if got := receive(t, c); got.Member != "q" {
		t.Fatalf("received %+v, want the second event", got)
	}
	if n := r.Hooks().Failures()["hook.cep"]; n != 1 {
		t.Fatalf("failures %d, want 1", n)
	}
}

// TestNoClusterListener: with no listener Cluster does nothing and nothing is
// counted.
func TestNoClusterListener(t *testing.T) {
	r := clusterRegistry(t, hook.Policy{})
	defer func() { _ = r.Stop(context.Background()) }()
	if r.Hooks().Clustering() {
		t.Fatal("Clustering with no listener")
	}
	r.Hooks().Cluster(ClusterEvent{Kind: MemberJoined, Member: "x"})
	if d := r.Hooks().ClusterDropped(); len(d) != 0 {
		t.Fatalf("dropped %v", d)
	}
}
