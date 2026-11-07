package etcdraft

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
)

// sendQueueDepth is each peer's queue of raft messages awaiting the
// transport: the in-flight append window and as much again for heartbeats
// and responses. A full queue means the peer's link is far behind, and a
// message past it is dropped and the peer reported unreachable, which raft
// answers by probing that follower one append at a time.
const sendQueueDepth = 2 * maxInflightAppends

// sendBuckets are the upper bounds, in seconds, of the time-in-Send histogram.
var sendBuckets = []float64{0.001, 0.01, 0.1, 1}

// peerSend is one peer's send queue and the goroutine that drains it.
type peerSend struct {
	name string
	id   uint64
	q    chan []byte
}

// outbound is the raft loop's view of the peers it sends to: one queue each,
// created at the first message to the peer.
type outbound struct {
	mu    sync.Mutex
	peers map[string]*peerSend
}

// enqueue puts a marshalled message on to's queue without waiting. The Ready
// loop calls it only after the batch's save has returned, so the order to one
// peer is the loop's and nothing leaves before it is durable. A full queue
// drops the message, counted, and reports the peer unreachable.
func (c *Core) enqueue(to string, id uint64, frame []byte) {
	c.out.mu.Lock()
	p := c.out.peers[to]
	if p == nil {
		if c.out.peers == nil {
			c.out.peers = map[string]*peerSend{}
		}
		p = &peerSend{name: to, id: id, q: make(chan []byte, sendQueueDepth)}
		c.out.peers[to] = p
		c.bg.Add(1)
		go c.drainPeer(p)
	}
	c.out.mu.Unlock()
	select {
	case p.q <- frame:
		c.set("hoardmq_raft_send_queued", map[string]string{"peer": to}, float64(len(p.q)))
	default:
		c.count("hoardmq_raft_send_dropped_total", map[string]string{"peer": to}, 1)
		c.reportUnreachable(id)
	}
}

// drainPeer sends one peer's messages in the order they were queued until
// the core stops; what is still queued then is dropped. Each may wait up to
// an election timeout for room in the peer's control queue, not one tick:
// on a shared queue under a storm a tick turned every raft message into a
// drop.
func (c *Core) drainPeer(p *peerSend) {
	defer c.bg.Done()
	labels := map[string]string{"peer": p.name}
	for {
		var frame []byte
		select {
		case <-c.stop:
			return
		case frame = <-p.q:
		}
		c.set("hoardmq_raft_send_queued", labels, float64(len(p.q)))
		ctx, cancel := context.WithTimeout(context.Background(), c.o.Tick*time.Duration(c.o.ElectionTicks))
		began := time.Now()
		err := c.o.Transport.Send(ctx, p.name, transport.Consensus, frame)
		took := time.Since(began).Seconds()
		cancel()
		c.observeSend(p.name, took)
		if err != nil {
			c.reportUnreachable(p.id)
		}
	}
}

// observeSend records one Send's time as a cumulative histogram: a counter
// per bucket bound it fits under, the sum and the count.
func (c *Core) observeSend(peer string, seconds float64) {
	for _, le := range sendBuckets {
		if seconds <= le {
			c.count("hoardmq_raft_send_seconds_bucket", map[string]string{"peer": peer, "le": strconv.FormatFloat(le, 'g', -1, 64)}, 1)
		}
	}
	c.count("hoardmq_raft_send_seconds_bucket", map[string]string{"peer": peer, "le": "+Inf"}, 1)
	c.count("hoardmq_raft_send_seconds_sum", map[string]string{"peer": peer}, seconds)
	c.count("hoardmq_raft_send_seconds_count", map[string]string{"peer": peer}, 1)
}

// reportUnreachable tells the raft node a peer did not take a message. The
// node's own method waits on the node's channel, which is safe from any
// goroutine; unreachableHook, a test seam, sees each report.
func (c *Core) reportUnreachable(id uint64) {
	if c.unreachableHook != nil {
		c.unreachableHook(id)
	}
	c.mu.Lock()
	n := c.node
	c.mu.Unlock()
	if n != nil {
		n.ReportUnreachable(id)
	}
}
