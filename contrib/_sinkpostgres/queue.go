package sinkpostgres

import (
	"context"
	"sync"
	"time"
)

// queue is the buffer: rows waiting per table, in the order they were
// taken, and the batch the writer holds. Its size counts both, so a batch
// being retried still holds its room.
type queue struct {
	maxRecords, maxBytes int64

	mu      sync.Mutex
	wake    chan struct{}
	tables  map[string]*tableRows
	order   []string // tables with rows, the next to write first
	records int64
	bytes   int64
	closed  bool
}

type tableRows struct {
	route route
	rows  []pending
}

type pending struct {
	vals []any
	size int64
	at   time.Time
}

// batch is rows of one table the writer holds.
type batch struct {
	route route
	rows  [][]any
	sizes int64
}

func newQueue(maxRecords, maxBytes int64) *queue {
	return &queue{maxRecords: maxRecords, maxBytes: maxBytes, wake: make(chan struct{}, 1), tables: map[string]*tableRows{}}
}

func (q *queue) size() (records, bytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.records, q.bytes
}

func (q *queue) full() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.records >= q.maxRecords || q.bytes >= q.maxBytes
}

// push takes a row, or reports false when the buffer is full or closed.
func (q *queue) push(r route, vals []any, size int64) bool {
	q.mu.Lock()
	if q.closed || q.records >= q.maxRecords || q.bytes >= q.maxBytes {
		q.mu.Unlock()
		return false
	}
	t := q.tables[r.table]
	if t == nil {
		t = &tableRows{route: r}
		q.tables[r.table] = t
	}
	if len(t.rows) == 0 {
		q.order = append(q.order, r.table)
	}
	t.rows = append(t.rows, pending{vals: vals, size: size, at: time.Now()})
	q.records++
	q.bytes += size
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}

// close takes no more rows; next returns false once the rows are taken.
func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// next waits for rows and returns up to max of the next table's, waiting up
// to linger past its oldest row for a partial batch to fill. It returns false
// when ctx ends, or when the queue is closed and empty.
func (q *queue) next(ctx context.Context, max int, linger time.Duration) (batch, bool) {
	for {
		q.mu.Lock()
		if len(q.order) > 0 {
			t := q.tables[q.order[0]]
			if wait := time.Until(t.rows[0].at.Add(linger)); len(t.rows) < max && wait > 0 && !q.closed {
				q.mu.Unlock()
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return batch{}, false
				}
				continue
			}
			n := min(max, len(t.rows))
			b := batch{route: t.route}
			for _, p := range t.rows[:n] {
				b.rows = append(b.rows, p.vals)
				b.sizes += p.size
			}
			t.rows = t.rows[n:]
			q.order = q.order[1:]
			if len(t.rows) > 0 {
				q.order = append(q.order, t.route.table)
			}
			q.mu.Unlock()
			return b, true
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return batch{}, false
		}
		select {
		case <-q.wake:
		case <-ctx.Done():
			return batch{}, false
		}
	}
}

// done releases a batch's room once it is written or dropped.
func (q *queue) done(b batch) {
	q.mu.Lock()
	q.records -= int64(len(b.rows))
	q.bytes -= b.sizes
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// putBack returns a batch the writer could not finish to the front of its
// table, still counted, for drain.
func (q *queue) putBack(b batch) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t := q.tables[b.route.table]
	if t == nil {
		t = &tableRows{route: b.route}
		q.tables[b.route.table] = t
	}
	rows := make([]pending, 0, len(b.rows)+len(t.rows))
	per := int64(0)
	if len(b.rows) > 0 {
		per = b.sizes / int64(len(b.rows))
	}
	for _, v := range b.rows {
		rows = append(rows, pending{vals: v, size: per})
	}
	if rem := b.sizes - per*int64(len(b.rows)); rem != 0 && len(rows) > 0 {
		rows[0].size += rem
	}
	if len(t.rows) == 0 {
		q.order = append([]string{b.route.table}, q.order...)
	}
	t.rows = append(rows, t.rows...)
}

// drain empties the queue, returning every row left as batches by table.
func (q *queue) drain() []batch {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []batch
	for _, name := range q.order {
		t := q.tables[name]
		b := batch{route: t.route}
		for _, p := range t.rows {
			b.rows = append(b.rows, p.vals)
			b.sizes += p.size
		}
		t.rows = nil
		out = append(out, b)
	}
	q.order = nil
	q.records, q.bytes = 0, 0
	return out
}
