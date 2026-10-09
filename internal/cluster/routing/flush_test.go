package routing

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// blockingTransport holds every send until released, so a test can put the
// flusher's send in flight and keep it there. It records what was sent.
type blockingTransport struct {
	stubTransport
	mu      sync.Mutex
	frames  [][]byte
	hold    chan struct{}
	holding bool
	inSend  chan struct{}
}

func newBlockingTransport(peers ...string) *blockingTransport {
	t := &blockingTransport{hold: make(chan struct{}), inSend: make(chan struct{}, 64)}
	t.handlers = map[transport.Class]transport.Handler{}
	for _, name := range peers {
		t.peers = append(t.peers, transport.PeerInfo{Name: name, State: transport.Up})
	}
	return t
}

func (b *blockingTransport) Send(_ context.Context, _ string, _ transport.Class, payload []byte) error {
	b.mu.Lock()
	hold, holding := b.hold, b.holding
	frame := append([]byte(nil), payload...)
	b.frames = append(b.frames, frame)
	b.mu.Unlock()
	select {
	case b.inSend <- struct{}{}:
	default:
	}
	if holding {
		<-hold
	}
	return nil
}

func (b *blockingTransport) release() {
	b.mu.Lock()
	if b.holding {
		b.holding = false
		close(b.hold)
	}
	b.mu.Unlock()
}

func (b *blockingTransport) sent() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]byte(nil), b.frames...)
}

// deltas returns the entries of every route-delta frame sent, one slice per
// frame, so a test can count frames as well as entries.
func (b *blockingTransport) deltas(t *testing.T) [][]Entry {
	t.Helper()
	var out [][]Entry
	for _, f := range b.sent() {
		if len(f) == 0 || f[0] != routeDelta {
			continue
		}
		var es []Entry
		if err := json.Unmarshal(f[1:], &es); err != nil {
			t.Fatalf("route delta: %v", err)
		}
		out = append(out, es)
	}
	return out
}

func flushPlane(t *testing.T, tr Transport) *Plane {
	t.Helper()
	p := New(Options{Node: "n1", Transport: tr, SyncInterval: time.Hour, Logger: quiet,
		Members: func() []string { return []string{"n1"} }, OwnedSessions: func(string) (int, bool) { return 0, true }})
	p.SetSink(&fakeSink{})
	p.Start()
	t.Cleanup(p.Stop)
	return p
}

// TestEmitDoesNotBlockOnTheTransport is the defect: emit runs under a
// session mutex and the route-update class is not urgent, so its enqueue
// blocks. The local table must be updated before emit returns all the same —
// Match, HiddenMatch and MovingMatch are read on this node's publish path.
func TestEmitDoesNotBlockOnTheTransport(t *testing.T) {
	tr := newBlockingTransport("n2")
	tr.holding = true
	defer tr.release()
	p := flushPlane(t, tr)

	// Put the flusher's send in flight and keep it there.
	p.Observe("default", "t/0", 1)
	waitFor(t, "the first send to be in flight", 5*time.Second, func() bool {
		select {
		case <-tr.inSend:
			return true
		default:
			return false
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Observe("default", "t/1", 1)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("emit blocked on the transport while a send was in flight")
	}
	if nodes, _ := p.table.Match("default", "t/1"); nodes != nil {
		t.Fatal("unexpected remote match")
	}
	if own := p.table.Own(); len(own) != 2 {
		t.Fatalf("the change must be in this node's own table before emit returns: %+v", own)
	}
}

// TestFlusherCoalescesWhatArrivesDuringASend: one filter changed repeatedly
// behind an in-flight send costs one entry, carrying the highest version.
func TestFlusherCoalescesWhatArrivesDuringASend(t *testing.T) {
	tr := newBlockingTransport("n2")
	tr.holding = true
	p := flushPlane(t, tr)

	p.Observe("default", "t/0", 1) // this one goes out and blocks
	waitFor(t, "the first send to be in flight", 5*time.Second, func() bool {
		select {
		case <-tr.inSend:
			return true
		default:
			return false
		}
	})
	for i := 2; i <= 9; i++ {
		p.Observe("default", "t/0", i)
	}
	tr.release()

	waitFor(t, "the coalesced batch", 5*time.Second, func() bool { return len(tr.deltas(t)) >= 2 })
	batches := tr.deltas(t)
	second := batches[1]
	if len(second) != 1 {
		t.Fatalf("eight changes to one filter must coalesce to one entry, got %d: %+v", len(second), second)
	}
	if second[0].Count != 9 {
		t.Fatalf("the coalesced entry must carry the newest change, got count %d", second[0].Count)
	}
}

// TestFlusherSendsPromptlyWhenIdle is the other half: a design that always
// batches has added latency. Idle, one change goes out in its own frame.
func TestFlusherSendsPromptlyWhenIdle(t *testing.T) {
	tr := newBlockingTransport("n2")
	p := flushPlane(t, tr)
	p.Observe("default", "t/0", 1)
	waitFor(t, "the change to be sent", 2*time.Second, func() bool { return len(tr.deltas(t)) == 1 })
	if got := tr.deltas(t)[0]; len(got) != 1 {
		t.Fatalf("an idle plane must send the one change on its own: %+v", got)
	}
}

// TestFlusherNeverSendsAnOlderViewAfterANewer: versions per filter must not
// decrease across the frames a peer receives. Table.Merge would mask a
// violation on the receiver, so this asserts what is sent.
func TestFlusherNeverSendsAnOlderViewAfterANewer(t *testing.T) {
	tr := newBlockingTransport("n2")
	p := flushPlane(t, tr)
	for i := 1; i <= 40; i++ {
		p.Observe("default", "t/0", i)
		p.Observe("default", "t/1", i)
	}
	waitFor(t, "the changes to be sent", 5*time.Second, func() bool {
		n := 0
		for _, b := range tr.deltas(t) {
			n += len(b)
		}
		return n > 0
	})
	time.Sleep(100 * time.Millisecond)
	highest := map[string]uint64{}
	for _, batch := range tr.deltas(t) {
		for _, e := range batch {
			if e.Version < highest[e.Filter] {
				t.Fatalf("filter %s went backwards: version %d after %d", e.Filter, e.Version, highest[e.Filter])
			}
			highest[e.Filter] = e.Version
		}
	}
}

// TestPeerEventDoesNotSendOnTheObserverGoroutine: notifyAll walks the
// observers in turn, so a full sync sent there delays every other
// component's notice of the same peer event — and at a hundred thousand
// routes that payload is not small.
func TestPeerEventDoesNotSendOnTheObserverGoroutine(t *testing.T) {
	tr := newBlockingTransport("n2")
	tr.holding = true
	defer tr.release()
	p := flushPlane(t, tr)
	p.Observe("default", "t/0", 1) // occupy the flusher
	waitFor(t, "the first send to be in flight", 5*time.Second, func() bool {
		select {
		case <-tr.inSend:
			return true
		default:
			return false
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.setState("n2", transport.Up) // drives peerChanged, as the transport does
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a peer event blocked on a transport send")
	}
}
