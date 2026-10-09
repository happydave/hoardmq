package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/shardmap"
	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
)

var topicR = store.Scope{Namespace: "default", Topic: "r"}

func retainedWrite(topic string) frame {
	return frame{Kind: kindRetainedWrite, Namespace: "default", Topic: topic, Epoch: 1}
}

func bareOutbound() *outbound {
	p := &outbound{name: "peer"}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// waitAsync starts a barrier naming keys and returns once it is waiting.
func waitAsync(t *testing.T, p *outbound, keys []store.Scope) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- p.wait(ctx, func(string) bool { return true }, keys)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		waiting := len(p.waiters) > 0
		p.mu.Unlock()
		if waiting {
			return done
		}
		select {
		case err := <-done:
			t.Fatalf("the barrier returned %v before waiting", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("precondition not reached: the barrier never waited")
		}
		time.Sleep(time.Millisecond)
	}
}

func quietFor(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s: the barrier returned %v", what, err)
	case <-time.After(200 * time.Millisecond):
	}
}

func returned(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("the barrier did not return")
		return nil
	}
}

// TestRetainedWriteClearsItsTopicsRefusal: a refused retained write fails a
// barrier naming its topic, and neither an unrelated session's frame nor
// another topic's retained write changes that.
// The topic's next retained write replaces the value, so a barrier naming the
// topic then waits for that write's acknowledgement.
func TestRetainedWriteClearsItsTopicsRefusal(t *testing.T) {
	p := bareOutbound()
	p.push(retainedWrite("r"))
	p.failedAt(1, notJournaledReason, store.Scope{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got := barrierReason(p.wait(ctx, func(string) bool { return true }, []store.Scope{topicR})); got != "not-held" {
		t.Fatalf("a barrier naming the refused topic: reason %q, want not-held", got)
	}
	p.push(frame{Kind: kindAppend, Namespace: "default", ClientID: "c", Epoch: 1})
	p.acked(2)
	if got := barrierReason(p.wait(ctx, func(string) bool { return true }, []store.Scope{topicR})); got != "not-held" {
		t.Fatalf("after another session's frame, a barrier naming the refused topic: reason %q, want not-held", got)
	}
	p.push(retainedWrite("s"))
	p.acked(3)
	if got := barrierReason(p.wait(ctx, func(string) bool { return true }, []store.Scope{topicR})); got != "not-held" {
		t.Fatalf("after another topic's retained write, a barrier naming the refused topic: reason %q, want not-held", got)
	}
	p.push(retainedWrite("r"))
	done := waitAsync(t, p, []store.Scope{topicR})
	quietFor(t, done, "before the new write's acknowledgement")
	p.acked(4)
	if err := returned(t, done); err != nil {
		t.Fatalf("a barrier naming the topic once its new write was acknowledged: %v", err)
	}
}

// TestBarrierWaitsForTheRetainedWriteThatClearedItsRefusal: a barrier over
// the topic began before its write was refused. The next write clears the
// refusal before the barrier looks again; the barrier must then pass on that
// write's acknowledgement, not on the refused frame's.
func TestBarrierWaitsForTheRetainedWriteThatClearedItsRefusal(t *testing.T) {
	p := bareOutbound()
	p.push(retainedWrite("r"))
	done := waitAsync(t, p, []store.Scope{topicR})
	p.failedAt(1, notJournaledReason, store.Scope{})
	p.push(retainedWrite("r"))
	p.refusalArrived()
	p.acked(1)
	quietFor(t, done, "on the refused frame's acknowledgement")
	p.acked(2)
	if err := returned(t, done); err != nil {
		t.Fatalf("once the clearing write was acknowledged: %v", err)
	}
}

// TestLateRefusalOfAnEarlierRetainedWriteIsRecorded: the later write replaces
// the earlier value without carrying it, so a refusal of the earlier write
// that arrives after the later was sent still fails a barrier over the topic.
func TestLateRefusalOfAnEarlierRetainedWriteIsRecorded(t *testing.T) {
	p := bareOutbound()
	p.push(retainedWrite("r"))
	p.push(retainedWrite("r"))
	p.failedAt(1, notJournaledReason, store.Scope{})
	p.acked(2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got := barrierReason(p.wait(ctx, func(string) bool { return true }, []store.Scope{topicR})); got != "not-held" {
		t.Fatalf("a barrier over the topic after a late refusal of its earlier write: reason %q, want not-held", got)
	}
}

// TestRejectClassNotJournaled: the leader's refusal of a retained write its
// journal did not take has its own class.
func TestRejectClassNotJournaled(t *testing.T) {
	if got := rejectClass(notJournaledReason); got != "not-journaled" {
		t.Fatalf("rejectClass(%q) = %q, want not-journaled", notJournaledReason, got)
	}
}

// TestLeaderRefusesARetainedWriteItsJournalDidNotTake: a retained write
// published on n1 and forwarded to its leader n2, whose session layer reports
// the record not durable, fails n1's barrier over the topic. Once n2's journal
// takes records again, the topic's next write passes.
func TestLeaderRefusesARetainedWriteItsJournalDidNotTake(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 3, 2)
	h.applyMap(shardmap.Compute(1, h.order, 2, 3)) // shard i: leader n(i+1), follower n(i+2 mod 3)
	waitSettled(h, 1)
	topic := topicIn(3, 1)
	scope := store.Scope{Namespace: "default", Topic: topic}
	n1, n2 := h.nodes["n1"], h.nodes["n2"]
	n2.sessions.unjournaled.Store(true)
	if n1.rep.StoreRetained(&message.Message{Namespace: "default", Topic: topic, Payload: []byte("a"), Retain: true}) {
		t.Fatal("precondition: n1 stored a write n2 leads")
	}
	err := n1.barrier(context.Background(), scope)
	if got := barrierReason(err); got != "not-held" {
		t.Fatalf("n1's barrier over a write n2's journal did not take: %v (reason %q), want not-held", err, got)
	}
	n2.sessions.unjournaled.Store(false)
	n1.rep.StoreRetained(&message.Message{Namespace: "default", Topic: topic, Payload: []byte("b"), Retain: true})
	if err := n1.barrier(context.Background(), scope); err != nil {
		t.Fatalf("the topic's next write once n2's journal took it: %v", err)
	}
}
