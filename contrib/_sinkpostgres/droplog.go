package sinkpostgres

import (
	"errors"
	"log/slog"
	"sync"
	"time"
)

// dropLog logs drops at Warn, at most once a second per reason, with the
// count since the last line, and transient failures at most once a second.
type dropLog struct {
	log *slog.Logger

	mu        sync.Mutex
	last      map[string]time.Time
	pending   map[string]int
	lastErr   map[string]error
	lastTable map[string]string
}

func (d *dropLog) init() {
	if d.last == nil {
		d.last, d.pending, d.lastErr, d.lastTable = map[string]time.Time{}, map[string]int{}, map[string]error{}, map[string]string{}
	}
}

func (d *dropLog) note(reason, table string, n int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	d.pending[reason] += n
	d.lastErr[reason], d.lastTable[reason] = err, table
	now := time.Now()
	if now.Sub(d.last[reason]) < time.Second {
		return
	}
	d.last[reason] = now
	d.emit(reason)
}

// retry logs a transient write failure, at most once a second.
func (d *dropLog) retry(table string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	now := time.Now()
	if now.Sub(d.last["retry"]) < time.Second {
		return
	}
	d.last["retry"] = now
	if d.log != nil {
		d.log.Warn("sink.postgres: write failed; retrying", "table", table, "err", err)
	}
}

// flush logs every reason with drops not yet logged.
func (d *dropLog) flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for reason := range d.pending {
		d.emit(reason)
	}
}

func (d *dropLog) emit(reason string) {
	n := d.pending[reason]
	if n == 0 || d.log == nil {
		return
	}
	attrs := []any{"reason", reason, "records", n, "table", d.lastTable[reason]}
	if err := d.lastErr[reason]; err != nil && !errors.Is(err, errStopped) {
		attrs = append(attrs, "err", err)
	}
	d.log.Warn("sink.postgres: records dropped", attrs...)
	d.pending[reason] = 0
}
