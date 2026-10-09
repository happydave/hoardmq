package sim

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/broker"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/module"
	"github.com/happydave/hoardmq/internal/packet"
)

// lockedBuffer is a log writer every node's handler can share.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// drainNoReturn is a cluster holding one durable subscriber's session on one
// node only (one replica) with five QoS 1 messages queued for it, and every
// takeover request to that node dropped, so no pull can reach it.
type drainNoReturn struct {
	c                *Cluster
	receiver, victim *Node
	ctx              context.Context
}

const drainSubID, drainTopic = "drain-sub", "drain/t"

func setupDrainNoReturn(t *testing.T, o Options) *drainNoReturn {
	t.Helper()
	o.Replicas = 1
	c := New(t, o)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	// The hand-off goes to the leader when it is another live node.
	receiver := c.Node(leaderName)
	var victim *Node
	for _, n := range c.Nodes {
		if n != receiver {
			victim = n
			break
		}
	}
	setup := NewClient(drainSubID, []string{victim.TCP}, 600, 1)
	if err := setup.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := setup.Subscribe(ctx, drainTopic, 1); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	waitUntil(t, "the subscriber recorded offline on its node", 5*time.Second, func() bool {
		rec, ok := victim.Server().Sessions().Record(drainSubID)
		return ok && !rec.Online
	})
	waitUntil(t, "the route on the receiver", 5*time.Second, func() bool {
		nodes, _ := receiver.Server().Routes().Table().Match("default", drainTopic)
		return len(nodes) == 1
	})
	pub := NewClient("drain-pub", []string{receiver.TCP}, 600, 2)
	if err := pub.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		code, err := publishWithRetry(ctx, pub, drainTopic, 1, i, 5)
		if err != nil || code.IsError() {
			t.Fatalf("publish %d: %s %v", i, code, err)
		}
	}
	pub.Close()
	for _, n := range c.Nodes {
		if n != victim {
			c.Fault(n, victim, &transport.Fault{Drop: 1, Classes: []transport.Class{transport.TakeoverRequest}, Rand: rand.New(rand.NewSource(1))}) //nolint:gosec // seeded simulation
		}
	}
	return &drainNoReturn{c: c, receiver: receiver, victim: victim, ctx: ctx}
}

// drainNoReturnOutcome is what the subscriber found on the receiver after
// the node that held its session drained and never came back.
type drainNoReturnOutcome struct {
	present     bool          // the first accepted CONNACK's Session Present
	received    int           // of the five queued messages, how many reached it
	servedEmpty int           // served-empty sessions counted across the running nodes
	drain       time.Duration // from Drain to Drained
	status      module.DrainStatus
}

// runDrainNoReturn drains the victim, stopping it the moment its drain
// completes, as the serve loop exits. Once the receiver holds the lease and
// its adoption's first fetch has gone unanswered, every fault is healed: a
// drain still running can now be pulled from, one that has completed cannot.
// The victim is never restarted; the subscriber connects to the receiver
// after the leader has declared the victim dead.
func runDrainNoReturn(t *testing.T, defects broker.Defects) drainNoReturnOutcome {
	t.Helper()
	sc := setupDrainNoReturn(t, Options{Defects: defects, DrainConfirmTimeout: 10 * time.Second, Log: halfLog(t, "drainnoreturn-"+t.Name())})
	c, receiver, victim := sc.c, sc.receiver, sc.victim
	drained := victim.Server()
	began := time.Now()
	stopped := make(chan time.Duration, 1)
	drained.Drain()
	go func() {
		<-drained.Drained()
		took := time.Since(began)
		c.Stop(victim)
		stopped <- took
	}()
	waitUntil(t, "the receiver owning the lease", 10*time.Second, func() bool {
		l, ok := receiver.Server().Leases().Table().Lookup("default", drainSubID)
		return ok && l.Owner == receiver.Name
	})
	waitUntil(t, "the receiver's adoption fetch going unanswered", 10*time.Second, func() bool {
		return receiver.Server().Leases().LivenessStats().AdoptWorkerUnanswered >= 1
	})
	if _, held := receiver.Server().Sessions().Record(drainSubID); held {
		t.Fatal("precondition not reached: the receiver holds the session although no pull could reach the drained node")
	}
	c.HealAll()
	out := drainNoReturnOutcome{}
	select {
	case out.drain = <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatalf("the drain did not complete: %+v", drained.DrainStatus())
	}
	out.status = drained.DrainStatus()
	waitUntil(t, "the leader declaring the drained node dead", 20*time.Second, func() bool {
		return !receiver.Server().Leases().Table().Live(victim.Name)
	})
	if victim.Running() {
		t.Fatal("precondition not reached: the drained node is running")
	}
	sub := NewClient(drainSubID, []string{receiver.TCP}, 600, 3)
	if err := sub.Run(sc.ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for _, e := range sub.TakeEvents() {
		if e.Kind == "connack" && e.Code == packet.Success {
			out.present = e.Present
			break
		}
	}
	seen := map[uint64]bool{}
	deadline := time.Now().Add(10 * time.Second)
	if !out.present {
		// A fresh session has nothing queued; a short look is enough to show it.
		deadline = time.Now().Add(time.Second)
	}
	for len(seen) < 5 && time.Now().Before(deadline) {
		for _, d := range sub.TakeDeliveries() {
			if d.Publisher == "drain-pub" {
				seen[d.Seq] = true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	out.received = len(seen)
	for _, byReason := range servedEmptyByNode(c) {
		for _, n := range byReason {
			out.servedEmpty += n
		}
	}
	t.Logf("drain %v, status %+v, Session Present %v, received %d, served empty %d", out.drain.Round(time.Millisecond), out.status, out.present, out.received, out.servedEmpty)
	return out
}

// TestDrainedHolderNotReturningKeepsTheSession: the drained node's journal is
// the only copy, its receiver's first pull cannot reach it, and it never comes
// back. The drain waits for the receiver's confirmation, so the receiver's
// next pull finds the node still up, and the subscriber gets its session and
// its five messages (WI 1763).
func TestDrainedHolderNotReturningKeepsTheSession(t *testing.T) {
	out := runDrainNoReturn(t, broker.Defects{})
	if !out.present || out.received != 5 || out.servedEmpty != 0 {
		t.Fatalf("Session Present %v, received %d of 5, served empty %d; want the session and every message", out.present, out.received, out.servedEmpty)
	}
	if out.status.SessionsConfirmed != 1 || out.status.SessionsUnconfirmed != 0 {
		t.Fatalf("drain status %+v; want the one session confirmed", out.status)
	}
}

// TestDrainedHolderNotReturningLosesTheSessionWhenTheDrainDoesNotWait is the
// control: with the drain completing at its hand-off, as it did before, the
// node stops before any pull reaches it and the subscriber is served a fresh
// session.
func TestDrainedHolderNotReturningLosesTheSessionWhenTheDrainDoesNotWait(t *testing.T) {
	out := runDrainNoReturn(t, broker.Defects{DrainSkipsConfirmations: true})
	if out.present || out.received != 0 {
		t.Fatalf("Session Present %v, received %d with the drain not waiting: the scenario no longer reaches the loss it is the control for", out.present, out.received)
	}
}

// TestDrainConfirmationTimeoutIsCountedLoggedAndBounded: a receiver that can
// never pull ends the drain at its confirmation timeout, with the session
// counted and logged unconfirmed.
func TestDrainConfirmationTimeoutIsCountedLoggedAndBounded(t *testing.T) {
	const confirmTimeout = 2 * time.Second
	logs := &lockedBuffer{}
	var w io.Writer = logs
	if extra := halfLog(t, "drainexpiry"); extra != nil {
		w = io.MultiWriter(logs, extra)
	}
	sc := setupDrainNoReturn(t, Options{DrainConfirmTimeout: confirmTimeout, Log: w})
	srv := sc.victim.Server()
	began := time.Now()
	srv.Drain()
	select {
	case <-srv.Drained():
	case <-time.After(30 * time.Second):
		t.Fatalf("the drain did not complete: %+v", srv.DrainStatus())
	}
	took := time.Since(began)
	defer sc.c.Stop(sc.victim)
	if l, ok := sc.receiver.Server().Leases().Table().Lookup("default", drainSubID); !ok || l.Owner != sc.receiver.Name {
		t.Fatalf("precondition not reached: the receiver's table shows the lease as %+v, %v", l, ok)
	}
	// The wait starts at the hand-off, a few milliseconds after Drain.
	if took < confirmTimeout || took >= confirmTimeout+1500*time.Millisecond {
		t.Fatalf("the drain took %v; want its confirmation timeout of %v and not much more", took, confirmTimeout)
	}
	st := srv.DrainStatus()
	if st.SessionsUnconfirmed != 1 || st.SessionsConfirmed != 0 || st.SessionsAwaiting != 0 {
		t.Fatalf("drain status %+v; want the one session unconfirmed", st)
	}
	if got := metricValue(t, srv, "hoardmq_drain_sessions_unconfirmed_total"); got != 1 {
		t.Fatalf("hoardmq_drain_sessions_unconfirmed_total %v, want 1", got)
	}
	var line string
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, "node="+sc.victim.Name) && strings.Contains(l, "not confirmed held") {
			line = l
		}
	}
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "ended=timeout") || !strings.Contains(line, drainSubID) || !strings.Contains(line, "unconfirmed=1") {
		t.Fatalf("the expiry's log line %q; want a Warn naming the client and the count", line)
	}
}

// drainGate holds a drain between its hand-off and its confirmation wait:
// arrived is signalled when a draining node reaches the gate, and release
// lets it go on. The release is registered as a cleanup, so a failed test
// never leaves the drain goroutine blocked.
type drainGate struct {
	arrived chan struct{}
	release func()
	opened  chan struct{}
}

func newDrainGate(t *testing.T) *drainGate {
	g := &drainGate{arrived: make(chan struct{}, 1), opened: make(chan struct{})}
	g.release = sync.OnceFunc(func() { close(g.opened) })
	t.Cleanup(g.release)
	return g
}

func (g *drainGate) hook() {
	g.arrived <- struct{}{}
	<-g.opened
}

func (g *drainGate) await(t *testing.T) {
	t.Helper()
	select {
	case <-g.arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("precondition not reached: the drain did not reach its confirmation wait")
	}
}

// heldHere reports whether a node holds a client's session, in its map or
// sealed.
func heldHere(srv *broker.Server, id string) bool {
	if _, ok := srv.Sessions().HeldAt("default", id); ok {
		return true
	}
	_, _, ok := srv.Sessions().Sealed("default", id)
	return ok
}

// TestDrainCountsSessionsConfirmedBeforeItsWait: the receiver's confirmations
// arrive, and drop the draining node's copies, before the drain's
// confirmation wait begins. The drain still counts each of those sessions
// confirmed (WI 1789).
func TestDrainCountsSessionsConfirmedBeforeItsWait(t *testing.T) {
	gate := newDrainGate(t)
	c := New(t, Options{DrainGate: gate.hook})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	var victim *Node
	for _, n := range c.Nodes {
		if n.Name != leaderName {
			victim = n
			break
		}
	}
	ids := []string{"early-0", "early-1", "early-2"}
	for i, id := range ids {
		cl := NewClient(id, []string{victim.TCP}, 600, int64(20+i))
		if err := cl.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := cl.Subscribe(ctx, "early/"+id, 1); err != nil {
			t.Fatal(err)
		}
		cl.Close()
	}
	waitUntil(t, "the offline sessions recorded on their node", 5*time.Second, func() bool {
		for _, id := range ids {
			if rec, ok := victim.Server().Sessions().Record(id); !ok || rec.Online {
				return false
			}
		}
		return true
	})
	srv := victim.Server()
	srv.Drain()
	gate.await(t)
	waitUntil(t, "precondition: every session's copy dropped on the drained node by its receiver's confirmation, before the wait", 10*time.Second, func() bool {
		for _, id := range ids {
			l, ok := srv.Leases().Table().Lookup("default", id)
			if !ok || l.Owner == victim.Name || heldHere(srv, id) {
				return false
			}
		}
		return true
	})
	gate.release()
	select {
	case <-srv.Drained():
	case <-time.After(30 * time.Second):
		t.Fatalf("the drain did not complete: %+v", srv.DrainStatus())
	}
	st := srv.DrainStatus()
	confirmedTotal := metricValue(t, srv, "hoardmq_drain_sessions_confirmed_total")
	c.Stop(victim)
	if st.SessionsConfirmed != len(ids) || st.SessionsUnconfirmed != 0 || st.SessionsAwaiting != 0 {
		t.Fatalf("drain status %+v; want the %d sessions confirmed before the wait counted confirmed", st, len(ids))
	}
	if confirmedTotal != float64(len(ids)) {
		t.Fatalf("hoardmq_drain_sessions_confirmed_total %v, want %d", confirmedTotal, len(ids))
	}
}

// TestDrainWaitsForASessionThatLeftWithoutAConfirmation: the draining node's
// copy is gone before the wait begins and no confirmation for it ever comes.
// The drain waits for it until its confirmation timeout and reports it
// unconfirmed; it does not complete as though nothing were owed (WI 1789).
func TestDrainWaitsForASessionThatLeftWithoutAConfirmation(t *testing.T) {
	const confirmTimeout = 2 * time.Second
	gate := newDrainGate(t)
	sc := setupDrainNoReturn(t, Options{DrainConfirmTimeout: confirmTimeout, DrainGate: gate.hook, Log: halfLog(t, "drainleftunconfirmed")})
	srv := sc.victim.Server()
	defer sc.c.Stop(sc.victim)
	srv.Drain()
	gate.await(t)
	if l, ok := srv.Leases().Table().Lookup("default", drainSubID); !ok || l.Owner == sc.victim.Name {
		t.Fatalf("precondition not reached: the lease was not handed off: %+v, %v", l, ok)
	}
	// Every takeover request to the victim is dropped, so this copy leaves
	// with no confirmation behind it.
	if !srv.Sessions().EvictOffline("default", drainSubID, math.MaxUint64) {
		srv.Sessions().DropSealed("default", drainSubID, math.MaxUint64)
	}
	if heldHere(srv, drainSubID) {
		t.Fatal("precondition not reached: the draining node still holds the session")
	}
	released := time.Now()
	gate.release()
	select {
	case <-srv.Drained():
	case <-time.After(30 * time.Second):
		t.Fatalf("the drain did not complete: %+v", srv.DrainStatus())
	}
	took := time.Since(released)
	st := srv.DrainStatus()
	if st.SessionsUnconfirmed != 1 || st.SessionsConfirmed != 0 || st.SessionsAwaiting != 0 {
		t.Fatalf("drain status %+v after %v; want the session that left unconfirmed reported unconfirmed", st, took)
	}
	if took < confirmTimeout {
		t.Fatalf("the drain ended %v after its wait began; want it to wait its confirmation timeout of %v", took, confirmTimeout)
	}
	if got := metricValue(t, srv, "hoardmq_drain_sessions_unconfirmed_total"); got != 1 {
		t.Fatalf("hoardmq_drain_sessions_unconfirmed_total %v, want 1", got)
	}
}

// metricValue reads one unlabelled metric from a running node.
func metricValue(t *testing.T, srv *broker.Server, name string) float64 {
	t.Helper()
	snap, ok := srv.Registry().Metrics().(interface{ Snapshot() []module.Metric })
	if !ok {
		t.Fatal("the node's metrics cannot be read")
	}
	for _, m := range snap.Snapshot() {
		if m.Name == name && len(m.Labels) == 0 {
			return m.Value
		}
	}
	return 0
}

const drainCostEnv = "HOARDMQ_SIM_DRAIN_COST"

// TestDrainDurationCost measures what the confirmation wait adds to a drain
// with nothing in its way: a node holding one connected client and ten
// offline sessions, two copies, drained with the old completion (at the
// hand-off) and the new one (at the confirmations), alternating. Set
// HOARDMQ_SIM_DRAIN_COST=N for N runs an arm; without it one run of the new
// completion checks all eleven sessions are confirmed.
func TestDrainDurationCost(t *testing.T) {
	runs, err := strconv.Atoi(os.Getenv(drainCostEnv))
	measuring := err == nil && runs > 0
	arms := []bool{false}
	if measuring {
		arms = []bool{true, false}
	} else {
		runs = 1
	}
	for i := 1; i <= runs; i++ {
		for _, skip := range arms {
			name := fmt.Sprintf("run%d/after", i)
			if skip {
				name = fmt.Sprintf("run%d/before", i)
			}
			t.Run(name, func(t *testing.T) {
				took, st := drainCostRun(t, skip)
				t.Logf("drain-cost arm=%s took=%v confirmed=%d unconfirmed=%d", map[bool]string{true: "before", false: "after"}[skip], took.Round(time.Millisecond), st.SessionsConfirmed, st.SessionsUnconfirmed)
				if !skip && (st.SessionsUnconfirmed != 0 || st.SessionsConfirmed != 11) {
					t.Fatalf("drain status %+v; want all eleven sessions it held confirmed (ten offline, one redirected) and none unconfirmed", st)
				}
			})
		}
	}
}

func drainCostRun(t *testing.T, skip bool) (time.Duration, module.DrainStatus) {
	t.Helper()
	c := New(t, Options{Defects: broker.Defects{DrainSkipsConfirmations: skip}})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	leaderName, ok := agreedLeader(leaderViews(c, nil))
	if !ok {
		t.Fatal("no agreed leader")
	}
	var victim *Node
	for _, n := range c.Nodes {
		if n.Name != leaderName {
			victim = n
			break
		}
	}
	all := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		all = append(all, n.TCP)
	}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("cost-off-%d", i)
		cl := NewClient(id, []string{victim.TCP}, 600, int64(10+i))
		if err := cl.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if err := cl.Subscribe(ctx, "cost/"+id, 1); err != nil {
			t.Fatal(err)
		}
		cl.Close()
	}
	live := NewClient("cost-live", append([]string{victim.TCP}, all...), 600, 99)
	if err := live.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if err := live.Subscribe(ctx, "cost/live", 1); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the offline sessions recorded on their node", 5*time.Second, func() bool {
		for i := 0; i < 10; i++ {
			if rec, ok := victim.Server().Sessions().Record(fmt.Sprintf("cost-off-%d", i)); !ok || rec.Online {
				return false
			}
		}
		return true
	})
	srv := victim.Server()
	began := time.Now()
	srv.Drain()
	select {
	case <-srv.Drained():
	case <-time.After(60 * time.Second):
		t.Fatalf("the drain did not complete: %+v", srv.DrainStatus())
	}
	took := time.Since(began)
	st := srv.DrainStatus()
	c.Stop(victim)
	return took, st
}

// TestDrainWaitEndsAtShutdown: a node shut down while its drain waits for
// confirmations stops waiting at once and reports the unconfirmed session
// as the timeout would.
func TestDrainWaitEndsAtShutdown(t *testing.T) {
	sc := setupDrainNoReturn(t, Options{DrainConfirmTimeout: time.Minute, Log: halfLog(t, "drainshutdown")})
	srv := sc.victim.Server()
	srv.Drain()
	waitUntil(t, "the drain waiting for its confirmation", 10*time.Second, func() bool {
		st := srv.DrainStatus()
		return st.Phase == "awaiting-confirmations" && st.SessionsAwaiting == 1
	})
	sc.c.Stop(sc.victim)
	select {
	case <-srv.Drained():
	case <-time.After(5 * time.Second):
		t.Fatalf("the drain kept waiting after the shutdown: %+v", srv.DrainStatus())
	}
	if st := srv.DrainStatus(); st.SessionsUnconfirmed != 1 || st.SessionsAwaiting != 0 {
		t.Fatalf("drain status %+v; want the one session reported unconfirmed", st)
	}
}
