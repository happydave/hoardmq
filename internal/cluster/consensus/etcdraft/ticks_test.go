package etcdraft

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// tickCounter is a raft.Node that counts ticks and can stall the loop by
// blocking inside one. It is shared between the loop and the test, so it
// locks like any other shared state.
type tickCounter struct {
	raft.Node
	mu     sync.Mutex
	n      int
	stall  time.Duration
	stalls int
	ready  chan raft.Ready
}

func newTickCounter(stall time.Duration) *tickCounter {
	return &tickCounter{stall: stall, ready: make(chan raft.Ready)}
}

func (t *tickCounter) Tick() {
	t.mu.Lock()
	t.n++
	first := t.n == 1 && t.stall > 0 && t.stalls == 0
	if first {
		t.stalls++
	}
	t.mu.Unlock()
	if first {
		// The loop is inside this call and serving nothing else, which is
		// what a stalled process looks like from the ticker's side.
		time.Sleep(t.stall)
	}
}

func (t *tickCounter) Ready() <-chan raft.Ready { return t.ready }
func (t *tickCounter) Stop()                    {}

func (t *tickCounter) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.n
}

// loopCore builds the minimum Core that run() drives: a tick period, a node,
// and the channels the select reads.
func loopCore(tick time.Duration, n raft.Node) *Core {
	c := &Core{
		o:         Options{Tick: tick, ElectionTicks: 10, HeartbeatTicks: 1},
		log:       slog.New(slog.DiscardHandler),
		node:      n,
		inbox:     make(chan *pb.Message, 1),
		joins:     make(chan joinEvent, 1),
		proposals: make(chan proposal, 1),
		started:   make(chan struct{}),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		waiters:   map[uint64]chan result{},
		ccWaiters: map[uint64][]chan error{},
		idNames:   map[uint64]string{},
	}
	close(c.started)
	return c
}

// TestTicksFollowWallTimeThroughAStall is the defect: time.NewTicker drops
// ticks for a slow receiver and its channel holds one, so a stall costs
// raft's clock very nearly the whole gap and never repays it. Measured on
// this loop without the fix, a 200 ms stall over 30 wall periods delivered
// 10 ticks and a 1 s stall over 110 delivered 11 — the stall's own ticks are
// simply gone. Raft counts its election and check-quorum timeouts in ticks,
// so that is a node whose stated election timeout has quietly become much
// longer.
func TestTicksFollowWallTimeThroughAStall(t *testing.T) {
	const tick = 10 * time.Millisecond
	const stall = 200 * time.Millisecond

	n := newTickCounter(stall)
	c := loopCore(tick, n)
	go c.run()

	// Long enough for the stall and a few ordinary ticks after it.
	time.Sleep(stall + 10*tick)
	close(c.stop)
	<-c.done

	got := n.count()
	// The stall alone owes about twenty. Anything near one is the defect.
	if got < 15 {
		t.Fatalf("delivered %d ticks across a %v stall at a %v tick; wall time owed about %d",
			got, stall, tick, int(stall/tick))
	}
	delivered, owed, dropped, maxRun := c.TickStats()
	if maxRun < 10 {
		t.Errorf("largest catch-up %d; the stall should have produced one much larger", maxRun)
	}
	if dropped != 0 {
		t.Errorf("dropped %d ticks for a stall far inside the catch-up window", dropped)
	}
	if delivered != uint64(got) || owed < delivered {
		t.Errorf("stats disagree with the node: delivered=%d owed=%d counted=%d", delivered, owed, got)
	}
}

// TestTicksDoNotDriftOnAQuietLoop is the other side: an undisturbed loop must
// not deliver more ticks than time passed, or the fix would make every
// election timeout early instead of late.
func TestTicksDoNotDriftOnAQuietLoop(t *testing.T) {
	const tick = 10 * time.Millisecond

	n := newTickCounter(0)
	c := loopCore(tick, n)
	start := time.Now()
	go c.run()
	time.Sleep(30 * tick)
	close(c.stop)
	<-c.done
	elapsed := time.Since(start)

	got := n.count()
	// A generous ceiling: the point is that it tracks elapsed time, not that
	// a shared test machine schedules a 10 ms ticker precisely.
	if ceiling := int(elapsed/tick) + 5; got > ceiling {
		t.Fatalf("delivered %d ticks in %v at a %v tick; at most about %d were owed", got, elapsed, tick, ceiling)
	}
	if got < 10 {
		t.Fatalf("delivered %d ticks in %v; the loop is not ticking at all", got, elapsed)
	}
}

// TestCatchUpIsBounded covers the backstop for a gap that is not a stall — a
// suspended machine, a laptop lid — where reconstructing every tick would be
// a burst worth waiting through and would mean nothing anyway.
func TestCatchUpIsBounded(t *testing.T) {
	const tick = 10 * time.Millisecond

	n := newTickCounter(0)
	c := loopCore(tick, n)
	if !c.advanceTicks(n, 2*time.Hour) {
		t.Fatal("advanceTicks reported a stop it was not given")
	}

	bound := int(maxCatchUpWindow / tick)
	if got := n.count(); got != bound {
		t.Fatalf("delivered %d ticks for a two-hour gap; the bound is %d", got, bound)
	}
	_, owed, dropped, _ := c.TickStats()
	if want := int(2*time.Hour/tick) - bound; dropped != uint64(want) {
		t.Fatalf("dropped %d, want %d (owed %d)", dropped, want, owed)
	}
}

// TestCatchUpYieldsToStop keeps the loop stoppable: a catch-up must never be
// something a shutdown has to wait out.
func TestCatchUpYieldsToStop(t *testing.T) {
	const tick = time.Millisecond

	n := newTickCounter(0)
	c := loopCore(tick, n)
	close(c.stop)

	if c.advanceTicks(n, 2*time.Hour) {
		t.Fatal("advanceTicks ran a catch-up to completion after the core stopped")
	}
	if got := n.count(); got > 1 {
		t.Fatalf("delivered %d ticks after the core stopped", got)
	}
}
