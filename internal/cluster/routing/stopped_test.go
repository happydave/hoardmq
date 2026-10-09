package routing

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// firstFrame is a forwarded publish as it arrives from a peer, the frame that
// makes receiveForward create the peer's inbound.
func firstFrame(topic string) []byte {
	return encodeForward(fwdPublish, "", qos1(topic)).frame(1)
}

func hasInbound(p *Plane, peer string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.in[peer] != nil
}

// assertPlaneDrained fails when something still holds the plane's wait group
// after Stop has returned. An apply loop started past Stop's drain waits on
// its inbound's cond forever, so the wait never ends.
func assertPlaneDrained(t *testing.T, p *Plane) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a goroutine is running on a plane that has stopped: an apply loop was started past Stop's drain")
	}
}

// TestStoppedPlaneStartsNoApplyLoop: a first frame from a new peer after Stop
// records no inbound and starts nothing.
func TestStoppedPlaneStartsNoApplyLoop(t *testing.T) {
	p, _ := stubPlane(t, "b", nil, nil, nil)

	// The same frame, before Stop, is one the plane takes: the absence below
	// is the guard's doing, not a frame the plane would discard anyway.
	p.receiveForward("before", firstFrame("t/before"))
	if !hasInbound(p, "before") {
		t.Fatal("precondition: a first frame from a new peer records no inbound on a running plane")
	}

	p.Stop()
	p.receiveForward("late", firstFrame("t/late"))
	if hasInbound(p, "late") {
		t.Fatal("an inbound was recorded for a peer whose first frame arrived after Stop")
	}
	assertPlaneDrained(t, p)
}

// TestStopFinishesWhileFramesArrive drives first frames from new peers while
// Stop runs. On a plane that starts an apply loop past the drain, Stop waits
// forever, so it runs on its own goroutine with a bound and a regression fails
// here by name instead of hanging the package. One round misses a stopped
// check read before the lock more often than it catches it, so it runs ten.
func TestStopFinishesWhileFramesArrive(t *testing.T) {
	for round := 0; round < 10; round++ {
		stopDuringStorm(t, round)
	}
}

// stopDuringStorm is one round. Each sender keeps going for a fixed number of
// frames once the stop channel has closed, which spans the critical section
// that follows it and bounds how many apply loops a regression leaves behind.
func stopDuringStorm(t *testing.T, round int) {
	t.Helper()
	p, _ := stubPlane(t, "b", nil, nil, nil)

	const afterStop = 2000
	ready := make(chan struct{})
	markReady := sync.OnceFunc(func() { close(ready) })
	var senders sync.WaitGroup
	for g := 0; g < 4; g++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			left := afterStop
			for i := 0; left > 0; i++ {
				select {
				case <-p.stop:
					left--
				default:
				}
				p.receiveForward(fmt.Sprintf("peer-%d-%d", g, i), firstFrame("t/storm"))
				if i == 100 {
					markReady()
				}
			}
		}()
	}
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("round %d, precondition: the senders never got a hundred frames in", round)
	}

	stopped := make(chan struct{})
	go func() {
		p.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatalf("round %d: Stop did not finish: a frame that arrived during the drain started an apply loop nothing will close", round)
	}
	senders.Wait()
}

// TestFirstFrameDuringStopIsApplied holds Stop after it has closed the stop
// channel and before it closes the inbounds, by blocking the sync loop Stop
// waits for. A new peer's first frame arriving there is applied, as a known
// peer's frame is: refusing from the stop channel instead of the flag set with
// the inbounds' close would drop it.
func TestFirstFrameDuringStopIsApplied(t *testing.T) {
	var armed atomic.Bool
	entered := make(chan struct{})
	enter := sync.OnceFunc(func() { close(entered) })
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })

	tr := newStubTransport()
	sink := &fakeSink{}
	p := New(Options{Node: "b", Transport: tr, SyncInterval: 20 * time.Millisecond, Logger: quiet,
		Members: func() []string {
			if armed.Load() {
				enter()
				<-release
			}
			return nil
		}})
	p.SetSink(sink)
	p.Start()
	t.Cleanup(p.Stop)
	t.Cleanup(unblock)

	armed.Store(true)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition: the sync loop never reached Members")
	}
	stopped := make(chan struct{})
	go func() {
		p.Stop()
		close(stopped)
	}()
	select {
	case <-p.stop:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition: Stop never closed the stop channel")
	}

	p.receiveForward("newcomer", firstFrame("t/newcomer"))
	waitFor(t, "a new peer's first frame, arriving while Stop waits for the sync loop, to be applied", 5*time.Second,
		func() bool { return sink.directCount() >= 1 })

	unblock()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("Stop did not finish once the sync loop was released")
	}
	assertPlaneDrained(t, p)
}
