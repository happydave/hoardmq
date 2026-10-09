package etcdraft

import (
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// testCore builds the minimum Core that receive touches: an inbox of the
// given depth, a stop channel and a logger. It deliberately does not run
// the node loop, so the inbox is never drained and overflow is reachable.
// The tick matches the simulation harness's raft tick, so the tests
// exercise the real bound (five ticks) rather than the floor, which exists
// only for a Core built without options.
const testTick = 10 * time.Millisecond

func testCore(depth int) *Core {
	c := &Core{
		inbox:   make(chan *pb.Message, depth),
		joins:   make(chan joinEvent, 1),
		stop:    make(chan struct{}),
		log:     quiet,
		idNames: map[uint64]string{},
	}
	c.o.Tick = testTick
	return c
}

// TestInboxWaitStaysUnderTheSilenceLimit pins the relationship the bound
// exists to preserve. The transport judges a peer silent after the
// heartbeat times the permitted misses; this handler runs on that link's
// drainer, so the wait must be a small fraction of that limit in every
// configuration, not only in the product one.
func TestInboxWaitStaysUnderTheSilenceLimit(t *testing.T) {
	for _, tc := range []struct {
		what      string
		tick      time.Duration
		heartbeat time.Duration
		misses    int
	}{
		{"product", 100 * time.Millisecond, time.Second, 3},
		{"simulation harness", 10 * time.Millisecond, 100 * time.Millisecond, 3},
	} {
		t.Run(tc.what, func(t *testing.T) {
			c := &Core{}
			c.o.Tick = tc.tick
			silence := tc.heartbeat * time.Duration(tc.misses)
			if got := c.inboxWait(); got*4 > silence {
				t.Fatalf("wait %s is not a small fraction of the %s silence limit", got, silence)
			}
		})
	}
}

func raftFrame(t *testing.T, typ pb.MessageType) []byte {
	t.Helper()
	m := &pb.Message{Type: typ.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(3)}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte{envRaft}, b...)
}

// fill offers frames until the inbox is at capacity.
func fill(t *testing.T, c *Core) {
	t.Helper()
	for len(c.inbox) < cap(c.inbox) {
		c.inbox <- &pb.Message{}
	}
}

// TestSnapshotMessageIsNeverDropped is work item 1345's negative control.
// Every other raft message has a retry behind it -- the leader resends an
// append, a candidate re-campaigns after a lost vote -- so discarding one
// under pressure costs a round trip. A snapshot has no such retry: the
// leader holds the follower in StateSnapshot waiting for a ReportSnapshot
// that a discarded snapshot never produces. The receive path must therefore
// wait for a snapshot rather than counting it and moving on.
func TestSnapshotMessageIsNeverDropped(t *testing.T) {
	c := testCore(2)
	fill(t, c)

	// Free one slot shortly after the offer, so a receive that waits
	// succeeds and a receive that drops does not.
	go func() {
		time.Sleep(50 * time.Millisecond)
		<-c.inbox
	}()

	c.receive("n2", raftFrame(t, pb.MsgSnap))

	if got := c.dropped.Load(); got != 0 {
		t.Fatalf("a snapshot message was dropped: dropped=%d, want 0", got)
	}
}

// TestOrdinaryMessageWaitsBeforeItIsDropped covers the bounded wait: a
// consensus loop that is briefly busy must not cost a raft message.
func TestOrdinaryMessageWaitsBeforeItIsDropped(t *testing.T) {
	c := testCore(2)
	fill(t, c)
	go func() {
		time.Sleep(20 * time.Millisecond)
		<-c.inbox
	}()

	c.receive("n2", raftFrame(t, pb.MsgApp))

	if got := c.dropped.Load(); got != 0 {
		t.Fatalf("an append was dropped despite space appearing: dropped=%d, want 0", got)
	}
}

// TestOrdinaryMessageIsDroppedWhenTheLoopNeverResumes covers the other half
// of the bound: a wedged consensus loop must not hold the transport's
// drainer, because the drainer's caller is the connection reader and a
// reader that stops is a node its peer will judge silent.
func TestOrdinaryMessageIsDroppedWhenTheLoopNeverResumes(t *testing.T) {
	c := testCore(1)
	fill(t, c)

	started := time.Now()
	c.receive("n2", raftFrame(t, pb.MsgApp))
	took := time.Since(started)

	if got := c.dropped.Load(); got != 1 {
		t.Fatalf("a wedged loop did not drop: dropped=%d, want 1", got)
	}
	if took > 2*time.Second {
		t.Fatalf("receive held the drainer for %s; the bound must be well under the silence limit", took)
	}
}

// TestReceiveReturnsWhenTheCoreStops: a bounded wait that ignores the stop
// channel makes Stop take the bound for every queued frame.
func TestReceiveReturnsWhenTheCoreStops(t *testing.T) {
	c := testCore(1)
	fill(t, c)
	close(c.stop)

	done := make(chan struct{})
	go func() {
		c.receive("n2", raftFrame(t, pb.MsgSnap))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("receive did not return after the core stopped")
	}
}
