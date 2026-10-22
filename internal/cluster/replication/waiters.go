package replication

import "github.com/happydave/hoardmq/internal/store"

// waiter is one barrier waiting on a stream: the channel a change closes to
// wake it, the target it waits for, and the keys it names.
type waiter struct {
	ch     chan struct{}
	target uint64
	keys   []store.Scope
	set    map[store.Scope]struct{} // the bare keys, when there are many
}

// waiterSetMin is the key count from which a waiter keeps a set of its keys,
// so a refusal's wake does not scan a confirmation's tens of thousands.
const waiterSetMin = 16

func newWaiter(keys []store.Scope) *waiter {
	w := &waiter{keys: keys}
	if len(keys) >= waiterSetMin {
		w.set = make(map[store.Scope]struct{}, len(keys))
		for _, k := range keys {
			w.set[k.Bare()] = struct{}{}
		}
	}
	return w
}

// namesAny reports whether the waiter names any of these bare keys.
func (w *waiter) namesAny(bare []store.Scope) bool {
	for _, b := range bare {
		if w.set != nil {
			if _, ok := w.set[b]; ok {
				return true
			}
			continue
		}
		for _, k := range w.keys {
			if k.Bare() == b {
				return true
			}
		}
	}
	return false
}

// wakeAllLocked removes every waiting barrier and returns their channels to
// close. Callers hold p.mu.
func (p *outbound) wakeAllLocked() []chan struct{} {
	return p.wakeLocked(func(*waiter) bool { return true })
}

// wakeLocked removes the waiting barriers match selects and returns their
// channels to close. Callers hold p.mu.
func (p *outbound) wakeLocked(match func(*waiter) bool) []chan struct{} {
	var out []chan struct{}
	for w := range p.waiters {
		if match(w) {
			out = append(out, w.ch)
			delete(p.waiters, w)
		}
	}
	return out
}

// forget removes a barrier that stopped waiting on its own: a re-check, its
// timeout, or the peer down.
func (p *outbound) forget(w *waiter) {
	p.mu.Lock()
	delete(p.waiters, w)
	p.mu.Unlock()
}
