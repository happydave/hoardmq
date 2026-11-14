package session

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// objectHistory records, for each session object, its install in the map
// (and what it replaced), its router adds and removals with the manager path
// that made them, and its removal; and, per client, an import's steps. Each
// event carries a global sequence number and the goroutine that made it.
type objectHistory struct {
	seq    atomic.Uint64
	mu     sync.Mutex
	names  map[*Session]int
	byObj  map[*Session][]string
	byID   map[string][]string
	nextID int
}

func newObjectHistory() *objectHistory {
	return &objectHistory{names: map[*Session]int{}, byObj: map[*Session][]string{}, byID: map[string][]string{}}
}

// name is a stable short name for an object, o<n>, nil as "-".
func (h *objectHistory) nameLocked(s *Session) string {
	if s == nil {
		return "-"
	}
	n, ok := h.names[s]
	if !ok {
		h.nextID++
		n = h.nextID
		h.names[s] = n
	}
	return fmt.Sprintf("o%d", n)
}

func goroutineID() string {
	var b [64]byte
	n := runtime.Stack(b[:], false)
	f := bytes.Fields(b[:n])
	if len(f) > 1 {
		return "g" + string(f[1])
	}
	return "g?"
}

// caller names the first manager function on the stack outside the router,
// the history and removal's helpers: the path that made the change.
func caller() string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		fr, more := frames.Next()
		fn := fr.Function
		i := strings.LastIndex(fn, "/")
		short := fn[i+1:]
		switch {
		case strings.Contains(short, "historyRouter"), strings.Contains(short, "exactRouter"),
			strings.Contains(short, "objectHistory"), strings.HasSuffix(short, ".removeLocked"),
			strings.HasSuffix(short, ".discardWith"), strings.HasSuffix(short, ".discardLocked"),
			strings.Contains(short, "traceSession"), strings.Contains(short, "func"):
		default:
			if strings.HasPrefix(short, "session.") {
				return strings.TrimPrefix(short, "session.")
			}
		}
		if !more {
			return "?"
		}
	}
}

func (h *objectHistory) add(id string, s *Session, line string) {
	n := h.seq.Add(1)
	g := goroutineID()
	h.mu.Lock()
	ev := fmt.Sprintf("#%d %s %s %s", n, g, h.nameLocked(s), line)
	if s != nil {
		h.byObj[s] = append(h.byObj[s], ev)
	}
	h.byID[id] = append(h.byID[id], ev)
	h.mu.Unlock()
}

// trace is the manager's traceSession hook.
func (h *objectHistory) trace(ev, id string, s, before *Session) {
	h.mu.Lock()
	b := h.nameLocked(before)
	h.mu.Unlock()
	switch {
	case strings.HasSuffix(ev, "install"):
		h.add(id, s, fmt.Sprintf("%s (map held %s)", ev, b))
		if before != nil && before != s {
			h.add(id, before, fmt.Sprintf("overwritten by %s's %s", h.objName(s), ev))
		}
	case ev == "remove":
		h.add(id, s, fmt.Sprintf("removed by %s (map held %s)", caller(), b))
	default:
		h.add(id, s, ev)
	}
}

func (h *objectHistory) objName(s *Session) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nameLocked(s)
}

// of returns an object's events and its client's, in order.
func (h *objectHistory) of(s *Session) (obj, client []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.byObj[s]...), append([]string(nil), h.byID[s.key.id]...)
}

// historyRouter records every router change for the history.
type historyRouter struct {
	*exactRouter
	h *objectHistory
}

func (r *historyRouter) Add(s *Session, sub Subscription) bool {
	existed := r.exactRouter.Add(s, sub)
	r.h.add(s.key.id, s, fmt.Sprintf("router add %s by %s (existed %v)", sub.Filter, caller(), existed))
	return existed
}

func (r *historyRouter) Remove(s *Session, filter string) bool {
	ok := r.exactRouter.Remove(s, filter)
	r.h.add(s.key.id, s, fmt.Sprintf("router remove %s by %s (held %v)", filter, caller(), ok))
	return ok
}

func (r *historyRouter) RemoveAll(s *Session, filters []string) {
	r.exactRouter.RemoveAll(s, filters)
	r.h.add(s.key.id, s, fmt.Sprintf("router remove-all %v by %s", filters, caller()))
}
