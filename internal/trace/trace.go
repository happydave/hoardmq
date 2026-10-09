// Package trace records the packets one client connection sends and
// receives, in a bounded ring, for the operator to inspect.
package trace

import (
	"sync"
	"time"
)

// Direction of a recorded packet.
type Direction string

// Directions.
const (
	In  Direction = "in"
	Out Direction = "out"
)

// Event is one recorded packet.
type Event struct {
	Time      time.Time `json:"time"`
	Direction Direction `json:"direction"`
	Type      string    `json:"type"`
	PacketID  uint16    `json:"packetId,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Size      int       `json:"size,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

// Recorder is a bounded ring of events.
type Recorder struct {
	mu    sync.Mutex
	ring  []Event
	next  int
	full  bool
	total int
}

// New creates a recorder holding at most limit events (minimum 1).
func New(limit int) *Recorder {
	if limit < 1 {
		limit = 1
	}
	return &Recorder{ring: make([]Event, limit)}
}

// Record adds an event, overwriting the oldest when full.
func (r *Recorder) Record(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ring[r.next] = e
	r.next = (r.next + 1) % len(r.ring)
	if r.next == 0 {
		r.full = true
	}
	r.total++
}

// Events returns the recorded events, oldest first.
func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		out := make([]Event, r.next)
		copy(out, r.ring[:r.next])
		return out
	}
	out := make([]Event, 0, len(r.ring))
	out = append(out, r.ring[r.next:]...)
	out = append(out, r.ring[:r.next]...)
	return out
}

// Total is the number of events recorded, including overwritten ones.
func (r *Recorder) Total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}
