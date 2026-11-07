package etcdraft

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/cluster/consensus/raftlog"
	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/config"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// gatedTransport records each Send by peer and holds every Send to a held
// peer until released or its context ends; sends to other peers return at
// once. took, when set, is how long each Send to any peer takes.
type gatedTransport struct {
	mu      sync.Mutex
	sent    map[string][]string
	held    string
	release chan struct{}
	took    time.Duration
	// holding counts the sends waiting at the hold.
	holding atomic.Int64
}

func (g *gatedTransport) Send(ctx context.Context, peer string, _ transport.Class, payload []byte) error {
	if g.took > 0 {
		time.Sleep(g.took)
	}
	if peer == g.held {
		g.holding.Add(1)
		defer g.holding.Add(-1)
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sent == nil {
		g.sent = map[string][]string{}
	}
	g.sent[peer] = append(g.sent[peer], string(payload))
	return nil
}
func (g *gatedTransport) Handle(transport.Class, transport.Handler) {}
func (g *gatedTransport) ObserveData(transport.Observer)            {}
func (g *gatedTransport) SetPeers([]config.Peer)                    {}
func (g *gatedTransport) Peers() []transport.PeerInfo               { return nil }

func (g *gatedTransport) sentTo(peer string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.sent[peer]...)
}

// sendMetrics keeps every series by name and labels.
type sendMetrics struct {
	mu sync.Mutex
	v  map[string]float64
}

func seriesOf(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k+"="+labels[k])
	}
	sort.Strings(keys)
	return name + "{" + strings.Join(keys, ",") + "}"
}

func (m *sendMetrics) Add(name string, labels map[string]string, d float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[seriesOf(name, labels)] += d
}

func (m *sendMetrics) Set(name string, labels map[string]string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.v == nil {
		m.v = map[string]float64{}
	}
	m.v[seriesOf(name, labels)] = v
}

func (m *sendMetrics) get(name string, labels map[string]string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.v[seriesOf(name, labels)]
}

// sendCore is a core with only what its send queues use: the transport,
// the timing, the metrics and the stop. Its cleanup stops it and waits for
// the senders.
func sendCore(t *testing.T, tr *gatedTransport, m *sendMetrics) *Core {
	t.Helper()
	c := &Core{o: Options{Transport: tr, Tick: 10 * time.Millisecond, ElectionTicks: 10, Metrics: m}, stop: make(chan struct{}),
		members: consensus.NewMachine().Membership(), idNames: map[uint64]string{2: "slow", 3: "fast"}}
	stopOnce := sync.OnceFunc(func() { close(c.stop) })
	t.Cleanup(func() {
		stopOnce()
		c.bg.Wait()
	})
	return c
}

// TestASlowPeerDoesNotHoldAnothersSends: while every send to one peer is
// held, a Ready batch's messages to another peer reach the transport at
// once. The loop that sent each message in turn held them behind the first
// held send.
func TestASlowPeerDoesNotHoldAnothersSends(t *testing.T) {
	tr := &gatedTransport{held: "slow", release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(tr.release) })
	c := sendCore(t, tr, &sendMetrics{})
	t.Cleanup(release)
	var msgs []*pb.Message
	for i := uint64(0); i < 5; i++ {
		msgs = append(msgs,
			&pb.Message{Type: pb.MsgHeartbeat.Enum(), To: proto.Uint64(2), Commit: proto.Uint64(i)},
			&pb.Message{Type: pb.MsgHeartbeat.Enum(), To: proto.Uint64(3), Commit: proto.Uint64(i)})
	}
	sent := make(chan struct{})
	go func() {
		c.send(nil, raftlog.Durable{}, msgs)
		close(sent)
	}()
	waitFor(t, "the fast peer's five messages", 50*time.Millisecond, func() bool { return len(tr.sentTo("fast")) == 5 })
	select {
	case <-sent:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("the Ready batch's send waited on the held peer")
	}
	if got := tr.sentTo("slow"); len(got) != 0 {
		t.Fatalf("the held peer received %v", got)
	}
	release()
	waitFor(t, "the slow peer's five messages once released", time.Second, func() bool { return len(tr.sentTo("slow")) == 5 })
}

// TestSendsToOnePeerKeepTheLoopsOrder: messages to one peer reach the
// transport in the order they were queued, with another peer's interleaved.
func TestSendsToOnePeerKeepTheLoopsOrder(t *testing.T) {
	tr := &gatedTransport{took: 200 * time.Microsecond}
	c := sendCore(t, tr, &sendMetrics{})
	for i := 0; i < 200; i++ {
		c.enqueue("a", 2, []byte(fmt.Sprintf("%03d", i)))
		if i%3 == 0 {
			c.enqueue("b", 3, []byte(fmt.Sprintf("%03d", i)))
		}
	}
	waitFor(t, "every message to a", 5*time.Second, func() bool { return len(tr.sentTo("a")) == 200 })
	got := tr.sentTo("a")
	for i, p := range got {
		if p != fmt.Sprintf("%03d", i) {
			t.Fatalf("message %d to a is %s: out of order", i, p)
		}
	}
}

// TestAFullQueueDropsCountsAndReports: with a peer's sends held, the queue
// takes sendQueueDepth messages beside the one being sent; each one past
// that is dropped, counted for the peer and reported unreachable, and the
// gauge reads a full queue.
func TestAFullQueueDropsCountsAndReports(t *testing.T) {
	tr := &gatedTransport{held: "slow", release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(tr.release) })
	m := &sendMetrics{}
	c := sendCore(t, tr, m)
	t.Cleanup(release)
	var reports atomic.Int64
	c.unreachableHook = func(id uint64) {
		if id == 2 {
			reports.Add(1)
		}
	}
	c.enqueue("slow", 2, []byte("first"))
	waitFor(t, "the first message held in Send", time.Second, func() bool { return tr.holding.Load() == 1 })
	for i := 0; i < sendQueueDepth+3; i++ {
		c.enqueue("slow", 2, []byte(fmt.Sprint(i)))
	}
	if got := m.get("hoardmq_raft_send_dropped_total", map[string]string{"peer": "slow"}); got != 3 {
		t.Fatalf("dropped %v, want 3", got)
	}
	if got := reports.Load(); got != 3 {
		t.Fatalf("reported unreachable %d times, want 3", got)
	}
	if got := m.get("hoardmq_raft_send_queued", map[string]string{"peer": "slow"}); got != sendQueueDepth {
		t.Fatalf("queued gauge %v, want %d", got, sendQueueDepth)
	}
}

// TestTimeInSendIsAHistogram: a send of about 50 ms is counted under the
// 0.1 s and 1 s bounds and +Inf, not the 10 ms one, with its sum and count.
func TestTimeInSendIsAHistogram(t *testing.T) {
	tr := &gatedTransport{took: 50 * time.Millisecond}
	m := &sendMetrics{}
	c := sendCore(t, tr, m)
	c.enqueue("p", 2, []byte("x"))
	count := map[string]string{"peer": "p"}
	waitFor(t, "the send observed", time.Second, func() bool { return m.get("hoardmq_raft_send_seconds_count", count) == 1 })
	bucket := func(le string) float64 {
		return m.get("hoardmq_raft_send_seconds_bucket", map[string]string{"peer": "p", "le": le})
	}
	if bucket("0.01") != 0 || bucket("0.1") != 1 || bucket("1") != 1 || bucket("+Inf") != 1 {
		t.Fatalf("buckets 0.01=%v 0.1=%v 1=%v +Inf=%v; want 0, 1, 1, 1", bucket("0.01"), bucket("0.1"), bucket("1"), bucket("+Inf"))
	}
	if sum := m.get("hoardmq_raft_send_seconds_sum", count); sum < 0.05 || sum > 0.5 {
		t.Fatalf("sum %v s for one 50 ms send", sum)
	}
}

// TestStopEndsTheSenders: a core stopped while a peer's send is held waits
// for its senders, and they end once the held send returns at its timeout.
func TestStopEndsTheSenders(t *testing.T) {
	tr := &gatedTransport{held: "slow", release: make(chan struct{})}
	c := &Core{o: Options{Transport: tr, Tick: 10 * time.Millisecond, ElectionTicks: 10}, stop: make(chan struct{})}
	t.Cleanup(sync.OnceFunc(func() { close(tr.release) }))
	for i := 0; i < 10; i++ {
		c.enqueue("slow", 2, []byte(fmt.Sprint(i)))
	}
	close(c.stop)
	done := make(chan struct{})
	go func() {
		c.bg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the sender did not end after the stop")
	}
}
