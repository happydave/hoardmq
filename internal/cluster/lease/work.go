package lease

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

// workQueue runs a fixed set of workers over session keys, coalescing
// repeated enqueues of one key into one pass that sees every payload
// attached since the last pass. It replaces a goroutine per event on the
// takeover, lost-lease and adoption paths: under a reconnect storm that
// shape put over a million goroutines on the session manager's lock
// (WI 1292 round 2), and a mutex with that many waiters serves none of them.
// The queue bounds the lock's waiters to the worker count and the memory to
// the node's session count. Nothing is dropped: a key is queued until a pass
// runs it, and a pass acknowledges every payload it was handed.
type workQueue struct {
	name    string
	workers int
	run     func(k key, payloads []any)
	log     *slog.Logger
	// onEvent reports processed / coalesced / panicked / dropped for metrics.
	onEvent func(outcome string)
	onDepth func(depth int)
	// onDrop is told the key of work abandoned after its second panic.
	onDrop func(k key)

	mu      sync.Mutex
	order   []key
	pending map[key][]any
	retried map[key]bool
	wake    chan struct{}
	stop    chan struct{}
	stopped bool
	wg      sync.WaitGroup

	depth     atomic.Int64
	coalesced atomic.Int64
	processed atomic.Int64
}

func newWorkQueue(name string, workers int, log *slog.Logger, run func(key, []any)) *workQueue {
	if workers < 1 {
		workers = 1
	}
	return &workQueue{
		name:    name,
		workers: workers,
		run:     run,
		log:     log,
		pending: make(map[key][]any),
		retried: make(map[key]bool),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
	}
}

// start launches the workers. Enqueues before start are kept and served
// once workers run.
func (q *workQueue) start() {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.loop()
	}
}

// stopAndWait ends the workers after their current pass. Keys still queued
// are abandoned: the service is stopping and its sessions go with it.
func (q *workQueue) stopAndWait() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	close(q.stop)
	q.mu.Unlock()
	q.wg.Wait()
}

// enqueue attaches payload to the key's pending pass, creating the pass if
// the key is not queued. A nil payload queues the key with nothing attached.
// Never blocks: it is called from the transport's receive goroutine and from
// the consensus-apply path.
func (q *workQueue) enqueue(k key, payload any) {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	if _, queued := q.pending[k]; queued {
		if payload != nil {
			q.pending[k] = append(q.pending[k], payload)
		}
		q.coalesced.Add(1)
		q.mu.Unlock()
		q.event("coalesced")
		return
	}
	var payloads []any
	if payload != nil {
		payloads = []any{payload}
	}
	q.pending[k] = payloads
	q.order = append(q.order, k)
	depth := len(q.order)
	q.mu.Unlock()
	q.depth.Store(int64(depth))
	if q.onDepth != nil {
		q.onDepth(depth)
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// pop takes the oldest queued key and its payloads.
func (q *workQueue) pop() (key, []any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return key{}, nil, false
	}
	k := q.order[0]
	q.order = q.order[1:]
	payloads := q.pending[k]
	delete(q.pending, k)
	q.depth.Store(int64(len(q.order)))
	if q.onDepth != nil {
		q.onDepth(len(q.order))
	}
	return k, payloads, true
}

func (q *workQueue) loop() {
	defer q.wg.Done()
	for {
		k, payloads, ok := q.pop()
		if !ok {
			select {
			case <-q.stop:
				return
			case <-q.wake:
				continue
			}
		}
		q.serve(k, payloads)
		// More keys may be waiting; keep draining before sleeping.
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
}

// serve runs one pass and, if it panics, queues the key once more so a
// transient fault does not leave a session in limbo; a second panic drops
// the key, counted and logged — the only way work leaves this queue unserved.
func (q *workQueue) serve(k key, payloads []any) {
	defer func() {
		if r := recover(); r != nil {
			q.mu.Lock()
			retry := !q.retried[k]
			q.retried[k] = true
			q.mu.Unlock()
			q.log.Error("lease work panicked", "queue", q.name, "client", k.id, "panic", r, "retrying", retry)
			q.event("panicked")
			if retry {
				for _, p := range payloads {
					q.enqueue(k, p)
				}
				if len(payloads) == 0 {
					q.enqueue(k, nil)
				}
			} else {
				q.event("dropped")
				if q.onDrop != nil {
					q.onDrop(k)
				}
			}
		}
	}()
	q.run(k, payloads)
	q.mu.Lock()
	delete(q.retried, k)
	q.mu.Unlock()
	q.processed.Add(1)
	q.event("processed")
}

func (q *workQueue) event(outcome string) {
	if q.onEvent != nil {
		q.onEvent(outcome)
	}
}

// WorkStats is one queue's counters, readable without any lock the storm
// contends for.
type WorkStats struct {
	Queue     string `json:"queue"`
	Depth     int64  `json:"depth"`
	Coalesced int64  `json:"coalesced"`
	Processed int64  `json:"processed"`
}

func (q *workQueue) stats() WorkStats {
	return WorkStats{Queue: q.name, Depth: q.depth.Load(), Coalesced: q.coalesced.Load(), Processed: q.processed.Load()}
}
