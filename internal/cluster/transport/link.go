package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// link is one authenticated connection to a peer: a reader goroutine that
// dispatches frames in arrival order, a writer goroutine draining a bounded
// queue, and a heartbeat goroutine. Admission to the queue happens under one
// lock, which is what makes the stream ordered across concurrent senders.
type link struct {
	t        *Transport
	p        *peer
	c        net.Conn
	version  uint8
	dialedBy string
	// role is the connection's part in the pair; a version-2 link serves
	// both and is installed in both of the peer's slots.
	role Role
	both bool

	admitMu sync.Mutex
	out     chan frame
	// urgent is the lane the writer drains before out: lease renewals, which
	// must not wait behind a storm's replication traffic (WI 1337). It is
	// outside the admission order and never blocks a sender.
	urgent  chan frame
	done    chan struct{}
	closing chan struct{}
	once    sync.Once
	err     error

	lastSeen atomic.Int64 // unix nanoseconds
	// Handler time, measured on the drainers (WI 1342 put it on the reader;
	// WI 1340 moved the handlers off it): total, count, worst, and the class
	// of the worst, so a storm's timeline names the handler that stalls.
	handlerNs    atomic.Int64
	handlerCount atomic.Int64
	handlerMaxNs atomic.Int64
	slowestClass atomic.Uint32
	// Dispatch: one bounded queue and one drainer per class the peer sends
	// (WI 1340). The reader decodes, stamps lastSeen and enqueues; it never
	// runs a handler, so a slow handler delays its own class and nothing
	// else — the peer's next heartbeat is still read on time.
	dispatchMu sync.Mutex
	queues     map[Class]*classQueue
	waitMaxNs  atomic.Int64 // longest a frame waited in a queue before its handler
	superseded bool
	wg         sync.WaitGroup
}

// classQueue is one class's frames awaiting its drainer on one link.
type classQueue struct {
	ch chan queued
}

type queued struct {
	f  frame
	at time.Time
}

// queueDepth is a class's dispatch queue capacity: the consensus core's own
// inbox size for raft, the ordered lane's depth for the data classes, a
// short lane for renewals (refused when full, as the sender's lane is),
// and a small number for the request-style control classes.
func queueDepth(c Class) int {
	switch c {
	case Consensus:
		return 4096
	case ReplicationAppend, ReplicationAck, ForwardedPublish, RouteUpdate:
		return 1024
	case LeaseAlive:
		return 64
	}
	return 256
}

func newWriter(c net.Conn) *bufio.Writer { return bufio.NewWriterSize(c, 64<<10) }

func newLink(t *Transport, p *peer, c net.Conn, version uint8, dialedBy string, role Role) *link {
	l := &link{t: t, p: p, c: c, version: version, dialedBy: dialedBy, role: role, both: version < Version3, out: make(chan frame, t.o.QueueDepth), urgent: make(chan frame, urgentDepth), done: make(chan struct{}), closing: make(chan struct{})}
	l.lastSeen.Store(time.Now().UnixNano())
	return l
}

func (l *link) start() {
	l.wg.Add(3)
	go l.readLoop()
	go l.writeLoop()
	go l.heartbeatLoop()
	go func() {
		l.wg.Wait()
		l.finish()
	}()
}

// admit queues a frame under the admission lock. Holding the lock while the
// queue is full is deliberate: it is the back-pressure point, and it keeps
// admission order equal to wire order.
func (l *link) admit(ctx context.Context, f frame) error {
	l.admitMu.Lock()
	defer l.admitMu.Unlock()
	select {
	case <-l.closing:
		return fmt.Errorf("%w: %q", ErrPeerDown, l.p.name)
	default:
	}
	select {
	case l.out <- f:
		if l.t.admitted != nil {
			l.t.admitted(l.p.name, f.payload)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-l.closing:
		return fmt.Errorf("%w: %q", ErrPeerDown, l.p.name)
	}
}

// admitUrgent queues a frame on the urgent lane without blocking: a full
// lane refuses the frame, since a writer that has not drained sixty-four
// renewals is not one worth waiting on.
func (l *link) admitUrgent(f frame) error {
	select {
	case <-l.closing:
		return fmt.Errorf("%w: %q", ErrPeerDown, l.p.name)
	default:
	}
	select {
	case l.urgent <- f:
		l.t.setGauge("hoardmq_cluster_urgent_queued", map[string]string{"peer": l.p.name}, float64(len(l.urgent)))
		return nil
	default:
		l.t.count("hoardmq_cluster_urgent_dropped_total", map[string]string{"peer": l.p.name}, 1)
		return fmt.Errorf("%w: %q", ErrUrgentFull, l.p.name)
	}
}

// close ends the connection once; the reason is kept for the log.
func (l *link) close(err error) {
	l.once.Do(func() {
		l.err = err
		close(l.closing)
		_ = l.c.Close()
	})
}

// finish runs after every goroutine has ended: the peer is marked down if
// this was its control link, the data observers are told if it was its
// data link, and the transport is told either way.
func (l *link) finish() {
	control, data := l.p.dropLink(l)
	if control {
		l.p.setState(l.t, Down)
		l.t.audit("cluster.peer.down", l.p.name, "closed", map[string]string{"error": fmt.Sprint(l.err), "connection": "control"})
		l.t.log.Info("cluster peer down", "peer", l.p.name, "err", l.err)
		l.t.gauge()
	}
	if data {
		l.p.setDataState(l.t, Down)
		l.t.audit("cluster.data.down", l.p.name, "closed", map[string]string{"error": fmt.Sprint(l.err)})
		l.t.log.Info("cluster data connection down", "peer", l.p.name, "err", l.err)
	}
	close(l.done)
}

// silenceLimit is how long a link's reader may go without a frame before
// the peer is judged silent: the heartbeat interval times the misses on the
// control connection; ten times that on the data connection, whose reader
// legitimately pauses under back-pressure and whose liveness the control
// connection judges (WI 1340; a bounded defence, not the liveness rule).
func (l *link) silenceLimit() time.Duration {
	limit := l.t.o.Heartbeat * time.Duration(l.t.o.HeartbeatMisses)
	if l.role == RoleData && !l.both {
		limit *= 10
	}
	return limit
}

func (l *link) readLoop() {
	defer l.wg.Done()
	r := bufio.NewReaderSize(l.c, 64<<10)
	for {
		f, err := readFrame(r, l.t.o.MaxFrame, false)
		if err != nil {
			if errors.Is(err, ErrProtocol) {
				l.t.log.Warn("cluster protocol error", "peer", l.p.name, "err", err)
				l.t.audit("cluster.peer.protocol", l.p.name, "closed", map[string]string{"error": err.Error()})
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				select {
				case <-l.closing:
				default:
					l.t.log.Debug("cluster read ended", "peer", l.p.name, "err", err)
				}
			}
			l.close(err)
			return
		}
		if l.version < f.class.minVersion() {
			err := fmt.Errorf("%w: %s on a framing version %d link", ErrProtocol, f.class, l.version)
			l.t.log.Warn("cluster protocol error", "peer", l.p.name, "err", err)
			l.t.audit("cluster.peer.protocol", l.p.name, "closed", map[string]string{"error": err.Error()})
			l.close(err)
			return
		}
		l.lastSeen.Store(time.Now().UnixNano())
		l.t.count("hoardmq_cluster_frames_total", map[string]string{"class": f.class.String(), "direction": "in"}, 1)
		l.t.count("hoardmq_cluster_bytes_total", map[string]string{"direction": "in"}, float64(len(f.payload)+headerSize))
		if f.class == Heartbeat {
			if len(f.payload) >= 1 {
				l.p.mu.Lock()
				l.p.stress = min(f.payload[0], 3)
				l.p.mu.Unlock()
			}
			continue
		}
		if l.t.handler(f.class) == nil {
			l.t.count("hoardmq_cluster_unhandled_total", map[string]string{"class": f.class.String()}, 1)
			continue
		}
		if !l.enqueue(f) {
			return
		}
	}
}

// enqueue hands a frame to its class's drainer, starting the drainer on the
// class's first frame. Every class blocks the reader when its queue is full
// — the sender's timeouts bound the wait, and on the data connection a
// blocked reader blocks only data — except the renewal lane, which is
// refused and counted, as its sending side is. It returns false when the
// link closed while waiting.
func (l *link) enqueue(f frame) bool {
	l.dispatchMu.Lock()
	q := l.queues[f.class]
	if q == nil {
		q = &classQueue{ch: make(chan queued, queueDepth(f.class))}
		if l.queues == nil {
			l.queues = map[Class]*classQueue{}
		}
		l.queues[f.class] = q
		go l.drain(f.class, q)
	}
	l.dispatchMu.Unlock()
	item := queued{f: f, at: time.Now()}
	if f.class.urgent() {
		select {
		case q.ch <- item:
		default:
			l.t.count("hoardmq_cluster_dispatch_dropped_total", map[string]string{"peer": l.p.name, "class": f.class.String()}, 1)
		}
		return true
	}
	select {
	case q.ch <- item:
		l.t.setGauge("hoardmq_cluster_dispatch_queued", map[string]string{"peer": l.p.name, "class": f.class.String()}, float64(len(q.ch)))
		return true
	case <-l.closing:
		return false
	}
}

// drain runs one class's handler over its queue, in arrival order, until
// the link closes. The handler is looked up per frame: a component that
// restarts on a kept transport re-registers, and the queue must follow. It
// is not part of the link's wait group: a handler that never returns must
// not keep the peer from being marked down.
func (l *link) drain(c Class, q *classQueue) {
	for {
		var item queued
		select {
		case <-l.closing:
			return
		case item = <-q.ch:
		}
		h := l.t.handler(c)
		if h == nil {
			l.t.count("hoardmq_cluster_unhandled_total", map[string]string{"class": c.String()}, 1)
			continue
		}
		waited := int64(time.Since(item.at))
		for {
			cur := l.waitMaxNs.Load()
			if waited <= cur || l.waitMaxNs.CompareAndSwap(cur, waited) {
				break
			}
		}
		started := time.Now()
		ok := l.dispatch(h, item.f)
		took := int64(time.Since(started))
		l.handlerNs.Add(took)
		l.handlerCount.Add(1)
		for {
			cur := l.handlerMaxNs.Load()
			if took <= cur {
				break
			}
			if l.handlerMaxNs.CompareAndSwap(cur, took) {
				l.slowestClass.Store(uint32(c))
				break
			}
		}
		if !ok {
			return
		}
	}
}

// dispatch runs a handler, recovering a panic by closing the connection so
// the process survives a handler bug.
func (l *link) dispatch(h Handler, f frame) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			l.t.log.Error("cluster handler panicked", "peer", l.p.name, "class", f.class.String(), "panic", r)
			l.close(fmt.Errorf("handler for %s panicked: %v", f.class, r))
			ok = false
		}
	}()
	h(l.p.name, f.payload)
	return true
}

// drainBatch bounds how many queued frames the writer puts in the buffer
// between flushes: a sender that keeps the queue full must not defer the
// flush until the buffer fills on its own.
const drainBatch = 128

// reorderWait is how long the reorder seam waits for a frame to swap with.
// Long enough that a sender producing a burst is never missed, short enough
// that a scenario driving traffic through it is not slowed materially.
const reorderWait = 10 * time.Millisecond

func (l *link) writeLoop() {
	defer l.wg.Done()
	w := newWriter(l.c)
	// The write deadline closes a connection whose peer stops reading. On
	// the data connection a slow peer is back-pressure, not silence, so its
	// deadline is the data silence limit rather than the control one.
	timeout := l.silenceLimit()
	write := func(f frame) bool {
		if fault := l.p.fault.Load(); fault != nil && f.class != Heartbeat && fault.affects(f.class) {
			if fault.drops() {
				l.t.count("hoardmq_cluster_frames_dropped_total", map[string]string{"class": f.class.String()}, 1)
				return true
			}
			if fault.Delay > 0 {
				select {
				case <-l.closing:
					return false
				case <-time.After(fault.Delay):
				}
			}
		}
		for l.t.pauseWrites.Load() || (!l.both && l.t.pauseRole[l.role].Load()) {
			select {
			case <-l.closing:
				return false
			case <-time.After(10 * time.Millisecond):
			}
		}
		_ = l.c.SetWriteDeadline(time.Now().Add(timeout))
		if err := writeFrame(w, f); err != nil {
			l.close(err)
			return false
		}
		if l.t.written != nil && f.class != Heartbeat {
			l.t.written(l.p.name, f.class, f.payload)
		}
		l.t.count("hoardmq_cluster_frames_total", map[string]string{"class": f.class.String(), "direction": "out"}, 1)
		l.t.count("hoardmq_cluster_bytes_total", map[string]string{"direction": "out"}, float64(len(f.payload)+headerSize))
		return true
	}
	flush := func() bool {
		if err := w.Flush(); err != nil {
			l.close(err)
			return false
		}
		return true
	}
	// urgentFirst writes and flushes everything on the urgent lane; it runs
	// before every take from the ordered queue so a renewal is never behind
	// a burst, and flushes because a liveness frame left in the buffer is a
	// frame the peer has not seen.
	urgentFirst := func() bool {
		wrote := false
		for {
			select {
			case u := <-l.urgent:
				l.t.setGauge("hoardmq_cluster_urgent_queued", map[string]string{"peer": l.p.name}, float64(len(l.urgent)))
				if !write(u) {
					return false
				}
				wrote = true
			default:
				return !wrote || flush()
			}
		}
	}
	for {
		if !urgentFirst() {
			return
		}
		var f frame
		select {
		case <-l.closing:
			return
		case f = <-l.urgent:
			l.t.setGauge("hoardmq_cluster_urgent_queued", map[string]string{"peer": l.p.name}, float64(len(l.urgent)))
			if !write(f) || !flush() {
				return
			}
			continue
		case f = <-l.out:
		}
		fault := l.p.fault.Load()
		if l.t.reorder || (fault != nil && fault.Reorder) {
			// Test seam: swap this frame with the next one. A non-blocking
			// take here made the swap depend on the sender already being
			// ahead of the writer, so the negative control that relies on it
			// missed about a third of the time under the race detector. It
			// waits briefly instead: only a build with the seam on pays it,
			// and only when the queue happens to be empty.
			timer := time.NewTimer(reorderWait)
			select {
			case g := <-l.out:
				timer.Stop()
				if !write(g) {
					return
				}
			case <-timer.C:
			}
		}
		if !write(f) {
			return
		}
		// Drain what is already queued before flushing, so bursts batch; the
		// urgent lane is checked before each queued frame, and a queue that
		// never empties still flushes every drainBatch frames.
	drain:
		for n := 0; n < drainBatch; n++ {
			if !urgentFirst() {
				return
			}
			select {
			case g := <-l.out:
				if !write(g) {
					return
				}
			default:
				break drain
			}
		}
		if !flush() {
			return
		}
	}
}

// heartbeatLoop sends this node's stress level every interval and closes
// the connection when the peer has been silent for the configured misses.
func (l *link) heartbeatLoop() {
	defer l.wg.Done()
	tick := time.NewTicker(l.t.o.Heartbeat)
	defer tick.Stop()
	limit := l.silenceLimit()
	for {
		select {
		case <-l.closing:
			return
		case <-tick.C:
		}
		if since := time.Since(time.Unix(0, l.lastSeen.Load())); since > limit {
			l.t.count("hoardmq_cluster_heartbeat_misses_total", map[string]string{"peer": l.p.name}, 1)
			l.close(fmt.Errorf("no frame from peer for %s", since.Round(time.Millisecond)))
			return
		}
		hb := frame{class: Heartbeat, payload: []byte{uint8(l.t.stress.Load())}}
		_ = l.admitUrgent(hb)
	}
}
