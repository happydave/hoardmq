package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

func wakeStream() *outbound {
	p := &outbound{name: "peer"}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func alwaysUp(string) bool { return true }

// waitingOn returns the channel the barrier naming key is registered with,
// once it is registered. It reads the keys itself, not through namesAny,
// which the tests hold.
func waitingOn(t *testing.T, p *outbound, key string) chan struct{} {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for w := range p.waiters {
			for _, k := range w.keys {
				if k.ClientID == key {
					ch := w.ch
					p.mu.Unlock()
					return ch
				}
			}
		}
		p.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no barrier on %s registered", key)
	return nil
}

func stillWaiting(p *outbound, ch chan struct{}) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for w := range p.waiters {
		if w.ch == ch {
			select {
			case <-ch:
				return false
			default:
				return true
			}
		}
	}
	return false
}

type waitResult struct {
	err  error
	took time.Duration
}

func startWait(p *outbound, ctx context.Context, key string) <-chan waitResult {
	out := make(chan waitResult, 1)
	began := time.Now()
	go func() {
		err := p.wait(ctx, alwaysUp, []store.Scope{{Namespace: "default", ClientID: key}})
		out <- waitResult{err, time.Since(began)}
	}()
	return out
}

func sessionFrame(key string) frame {
	return frame{Kind: kindAppend, Namespace: "default", ClientID: key}
}

// TestARefusalWakesOnlyTheBarriersNamingItsKey: barriers on a and b wait on
// one stream. A refusal of b fails b's barrier at once, well inside the 100 ms
// re-check, and leaves a's barrier waiting on the channel it registered.
func TestARefusalWakesOnlyTheBarriersNamingItsKey(t *testing.T) {
	p := wakeStream()
	p.push(sessionFrame("a"))
	p.push(sessionFrame("b"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	a := startWait(p, ctx, "a")
	aCh := waitingOn(t, p, "a")
	b := startWait(p, ctx, "b")
	waitingOn(t, p, "b")

	p.failedAt(2, "epoch 1 is behind lease epoch 2", store.Scope{})
	p.refusalArrived()
	select {
	case r := <-b:
		if !errors.Is(r.err, ErrReplicaUnavailable) || barrierReason(r.err) != "not-held" {
			t.Fatalf("b's barrier: %v after %v, want not-held at once", r.err, r.took)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("b's barrier did not fail within 50 ms of its key's refusal")
	}
	if !stillWaiting(p, aCh) {
		t.Fatal("a's barrier was woken by a refusal of another key")
	}
	p.acked(2)
	if r := <-a; r.err != nil {
		t.Fatalf("a's barrier after the acknowledgement: %v", r.err)
	}
}

// TestAnAcknowledgementWakesOnlyTheTargetsItReaches: a waits for sequence 1
// and b for 2. An acknowledgement of 1 passes a at once and leaves b waiting;
// one of 2 passes b.
func TestAnAcknowledgementWakesOnlyTheTargetsItReaches(t *testing.T) {
	p := wakeStream()
	p.push(sessionFrame("a"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	a := startWait(p, ctx, "a")
	waitingOn(t, p, "a")
	p.push(sessionFrame("b"))
	b := startWait(p, ctx, "b")
	bCh := waitingOn(t, p, "b")

	p.acked(1)
	select {
	case r := <-a:
		if r.err != nil {
			t.Fatalf("a's barrier: %v", r.err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("a's barrier did not pass within 50 ms of its target's acknowledgement")
	}
	if !stillWaiting(p, bCh) {
		t.Fatal("b's barrier was woken by an acknowledgement below its target")
	}
	p.acked(2)
	select {
	case r := <-b:
		if r.err != nil {
			t.Fatalf("b's barrier: %v", r.err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("b's barrier did not pass within 50 ms of its target's acknowledgement")
	}
}

// TestAStreamWideChangeWakesEveryBarrier: a refusal that cannot be scoped
// fails every waiting barrier at once, and so do a break and a stop.
func TestAStreamWideChangeWakesEveryBarrier(t *testing.T) {
	for _, c := range []struct {
		name, reason string
		change       func(p *outbound)
	}{
		// blockWhole marks every frame sent as failed, which the barrier reads
		// first.
		{"an unscoped refusal", "refused", func(p *outbound) {
			p.failedAt(3, "epoch 1 is behind lease epoch 2", store.Scope{})
			p.refusalArrived()
		}},
		{"a break", "refused", func(p *outbound) { p.broken() }},
		{"a stop", "stopped", func(p *outbound) { p.close() }},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := wakeStream()
			p.push(sessionFrame("a"))
			p.push(sessionFrame("b"))
			p.push(frame{Kind: kindResyncMark})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			a := startWait(p, ctx, "a")
			waitingOn(t, p, "a")
			b := startWait(p, ctx, "b")
			waitingOn(t, p, "b")
			c.change(p)
			for _, w := range []<-chan waitResult{a, b} {
				select {
				case r := <-w:
					if barrierReason(r.err) != c.reason {
						t.Fatalf("barrier: %v, want %s", r.err, c.reason)
					}
				case <-time.After(50 * time.Millisecond):
					t.Fatalf("a barrier did not fail within 50 ms of %s", c.name)
				}
			}
		})
	}
}

// TestAWaitingBarrierIsRegisteredOnce: a barrier that re-checks several times
// holds one registration, and none once it has timed out.
func TestAWaitingBarrierIsRegisteredOnce(t *testing.T) {
	p := wakeStream()
	p.push(sessionFrame("a"))
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	a := startWait(p, ctx, "a")
	waitingOn(t, p, "a")
	time.Sleep(350 * time.Millisecond)
	p.mu.Lock()
	n := len(p.waiters)
	p.mu.Unlock()
	if n != 1 {
		t.Fatalf("after three re-checks %d registrations, want 1", n)
	}
	if r := <-a; barrierReason(r.err) != "timeout" {
		t.Fatalf("barrier: %v, want timeout", r.err)
	}
	p.mu.Lock()
	n = len(p.waiters)
	p.mu.Unlock()
	if n != 0 {
		t.Fatalf("after its timeout %d registrations, want 0", n)
	}
}

// TestARefusalWakesABarrierNamingManyKeys: a barrier naming twenty keys, as a
// confirmation wait does, is woken by a refusal of one of them at once.
func TestARefusalWakesABarrierNamingManyKeys(t *testing.T) {
	p := wakeStream()
	var keys []store.Scope
	for i := 0; i < 20; i++ {
		k := string(rune('a' + i))
		p.push(sessionFrame(k))
		keys = append(keys, store.Scope{Namespace: "default", ClientID: k})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.wait(ctx, alwaysUp, keys) }()
	waitingOn(t, p, "t")
	p.failedAt(20, "epoch 1 is behind lease epoch 2", store.Scope{})
	p.refusalArrived()
	select {
	case err := <-done:
		if barrierReason(err) != "not-held" {
			t.Fatalf("barrier: %v, want not-held", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("the barrier naming twenty keys did not fail within 50 ms of a refusal of one")
	}
}
