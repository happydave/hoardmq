package replication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// stoppedReplicator is one node's replicator with Stop already called, and
// the names of the streams it held when it stopped.
func stoppedReplicator(t *testing.T) (*Replicator, []string) {
	t.Helper()
	h := newHarness(t, []string{"n1"}, 4, 2)
	r := h.nodes["n1"].rep
	r.Stop()
	return r, peerNames(r)
}

// peerNames is the set of streams the replicator is recording right now,
// sorted so two readings can be compared by name and not only by count.
func peerNames(r *Replicator) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.peers))
	for name := range r.peers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// assertNoGoroutineStarted fails if the replicator's wait group is holding
// anything. Stop returned, so it was at zero; a send loop started since would
// keep this waiting, which is the goroutine outliving the replicator.
func assertNoGoroutineStarted(t *testing.T, r *Replicator) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a goroutine is running on a replicator that has stopped: something reached wg.Add past Stop")
	}
}

// TestStoppedReplicatorStartsNoStream: once Stop has run, a peer with no
// stream gets a closed one that was never recorded and never started a send
// loop. This is the defect WI 1705's race report named, asserted without
// needing the race to happen.
func TestStoppedReplicatorStartsNoStream(t *testing.T) {
	r, held := stoppedReplicator(t)

	p := r.outboundFor("ghost")
	if p == nil {
		t.Fatal("outboundFor returned nil; eleven call sites dereference it")
	}
	if got := peerNames(r); !slices.Equal(got, held) {
		t.Fatalf("the recorded streams changed after Stop: held %v, now %v", held, got)
	}
	assertNoGoroutineStarted(t, r)

	// The stream is inert in both directions a caller can use it.
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c"})
	p.mu.Lock()
	sent := p.sent
	p.mu.Unlock()
	if sent != 0 {
		t.Fatalf("a push onto a stopped replicator's stream was queued: sent %d", sent)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := p.wait(ctx, r.up, []store.Scope{{Namespace: "default", ClientID: "c"}})
	if !errors.Is(err, ErrReplicaUnavailable) {
		t.Fatalf("a barrier over a stopped replicator's stream must refuse; got %v", err)
	}
	// The reason is asserted, not only the class: a peer the transport does
	// not have refuses with "down", so the class alone would be satisfied by
	// a stream that was never closed at all.
	if got := barrierReason(err); got != "stopped" {
		t.Fatalf("the barrier must refuse because replication stopped; refused as %q (%v)", got, err)
	}
}

// TestStoppedReplicatorKeepsTheStreamsItHad: the guard refuses to create,
// never to return. A stream the node really had is still answered with, so
// StreamStates, HoledStreams and RefusedKeys keep reporting it.
func TestStoppedReplicatorKeepsTheStreamsItHad(t *testing.T) {
	h := newHarness(t, []string{"n1"}, 4, 2)
	r := h.nodes["n1"].rep
	before := r.outboundFor("n2")
	r.Stop()

	if after := r.outboundFor("n2"); after != before {
		t.Fatal("a stream the node held before Stop was replaced rather than returned")
	}
	found := false
	for _, name := range peerNames(r) {
		if name == "n2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the stream held before Stop is no longer recorded: %v", peerNames(r))
	}
}

// TestStoppedStreamRefusesAResync: resync has an r.wg.Add of its own and
// nothing in resync guards it, so what keeps it unreachable is startResync
// refusing on a closed stream. The goroutine assertion cannot stand in for
// this — the goroutine resync starts for an unreachable peer finishes at
// once, so a wait group briefly held reads as one never held.
func TestStoppedStreamRefusesAResync(t *testing.T) {
	// Two cases, because two different clauses of startResync refuse them.
	// A stream that never existed carries no hole, so it is refused whether
	// or not it is closed; the one where closed is the load-bearing clause is
	// a stream that broke before Stop, which is also the one a returning peer
	// would really be resynced for.
	t.Run("a stream that never existed", func(t *testing.T) {
		r, _ := stoppedReplicator(t)
		assertNoResync(t, r.outboundFor("ghost"))
	})
	t.Run("a stream that broke before Stop", func(t *testing.T) {
		h := newHarness(t, []string{"n1"}, 4, 2)
		r := h.nodes["n1"].rep
		p := r.outboundFor("n2")
		p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c"})
		p.broken()
		if !p.holed() {
			t.Fatal("this case needs a holed stream; broken() left none")
		}
		r.Stop()
		assertNoResync(t, r.outboundFor("n2"))
	})
}

// assertNoResync fails if the stream would begin a resync, which is what
// reaches resync's own r.wg.Add, or would arrange one through resyncAfter.
func assertNoResync(t *testing.T, p *outbound) {
	t.Helper()
	gen, ok, wait := p.startResync(time.Now(), time.Second)
	if ok {
		t.Fatalf("a stopped replicator's stream began a resync, which reaches resync's own wg.Add (gen %d)", gen)
	}
	if wait != 0 {
		t.Fatalf("a refusal with a wait arranges a retry through resyncAfter; got %v", wait)
	}
}

// TestStoppedReplicatorEntryPointsStartNoStream covers the three ways a late
// transport notification reaches outboundFor. peerChanged runs both of the
// others, in that order: announcePresence before resync. resync is the one
// with an r.wg.Add of its own, and it is closed by startResync refusing on a
// closed stream rather than by a second guard.
func TestStoppedReplicatorEntryPointsStartNoStream(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(r *Replicator)
	}{
		{"sendPresence", func(r *Replicator) { r.sendPresence("ghost", []int{1}, 1) }},
		{"resync", func(r *Replicator) { r.resync("ghost") }},
		{"peerChanged", func(r *Replicator) { r.peerChanged("ghost", transport.Up) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, held := stoppedReplicator(t)
			tc.call(r)
			if got := peerNames(r); !slices.Equal(got, held) {
				t.Fatalf("%s changed the recorded streams after Stop: held %v, now %v", tc.name, held, got)
			}
			assertNoGoroutineStarted(t, r)
		})
	}
}

// TestPeerChangedDuringStopFinishes drives the interleaving the gate caught:
// notifications arriving while Stop drains, each with a name no stream exists
// for, so every one attempts a creation. Under -race it is the wg.Add against
// Stop's wg.Wait, and that half is probabilistic; the deterministic half is
// that Stop finishes at all, since a stream started past the drain is never
// closed and its send loop takes Stop's wg.Wait with it. Stop runs on its own
// goroutine so a regression fails by name instead of hanging the package.
func TestPeerChangedDuringStopFinishes(t *testing.T) {
	h := newHarness(t, []string{"n1"}, 4, 2)
	r := h.nodes["n1"].rep

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				r.peerChanged(fmt.Sprintf("ghost-%d-%d", i, n), transport.Up)
			}
		}(i)
	}
	stopped := make(chan struct{})
	go func() {
		r.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		close(stop)
		wg.Wait()
		t.Fatal("Stop did not finish: a notification that arrived during the drain started a send loop nothing will close")
	}
	close(stop)
	wg.Wait()
}
