package routing

import (
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/topic"
)

// A record of the forward refusals this node made or was told of, for the
// failover measurement: capped samples read at the end of a run, with what
// the plane knew of moving filters at the time. Nothing reads it to decide.

const refusalSamples = 256

// RefusalSample is one refusal: a forward this node answered as failed, a
// publish made here refused for a moving filter, or a publish whose barrier
// failed on a forward's failure mark.
type RefusalSample struct {
	AtNs   int64  `json:"at_ns"`
	Where  string `json:"where"` // receiver, local, barrier
	Reason string `json:"reason"`
	Peer   string `json:"peer,omitempty"`
	Topic  string `json:"topic,omitempty"`
	Seq    uint64 `json:"seq,omitempty"`
	// Delivered is how many local sessions a refused forward reached.
	Delivered int `json:"delivered,omitempty"`
	// Moving lists the filters marked moving here that match the topic, each
	// with how long ago its move began, in milliseconds.
	Moving map[string]int64 `json:"moving,omitempty"`
	// OwnCount is this node's own subscriber count for the topic as a filter.
	OwnCount int `json:"own_count,omitempty"`
	// LastFailed and Acked are the forward stream's marks for a barrier
	// failure.
	LastFailed uint64 `json:"last_failed,omitempty"`
	Acked      uint64 `json:"acked,omitempty"`
}

type refusalTrace struct {
	mu      sync.Mutex
	counts  map[string]int64
	samples []RefusalSample
	latest  []RefusalSample
	next    int
}

func (t *refusalTrace) add(s RefusalSample) {
	s.AtNs = time.Now().UnixNano()
	t.mu.Lock()
	if t.counts == nil {
		t.counts = map[string]int64{}
	}
	t.counts[s.Where+"/"+s.Reason]++
	switch {
	case len(t.samples) < refusalSamples:
		t.samples = append(t.samples, s)
	case len(t.latest) < refusalSamples:
		t.latest = append(t.latest, s)
	default:
		t.latest[t.next] = s
		t.next = (t.next + 1) % refusalSamples
	}
	t.mu.Unlock()
}

// RefusalTrace is the plane's record of refusals.
type RefusalTrace struct {
	Counts  map[string]int64 `json:"counts,omitempty"`
	Samples []RefusalSample  `json:"samples,omitempty"`
	Latest  []RefusalSample  `json:"latest,omitempty"`
}

// RefusalTrace reports the refusals this node made or was told of. Nothing
// reads it to decide.
func (p *Plane) RefusalTrace() RefusalTrace {
	t := &p.refusals
	t.mu.Lock()
	defer t.mu.Unlock()
	out := RefusalTrace{Samples: append([]RefusalSample(nil), t.samples...)}
	if len(t.counts) > 0 {
		out.Counts = make(map[string]int64, len(t.counts))
		for k, v := range t.counts {
			out.Counts[k] = v
		}
	}
	if len(t.latest) < refusalSamples {
		out.Latest = append([]RefusalSample(nil), t.latest...)
	} else {
		out.Latest = append(append([]RefusalSample(nil), t.latest[t.next:]...), t.latest[:t.next]...)
	}
	return out
}

// movingFor lists the moving filters matching a topic, with how long ago each
// move began.
func (p *Plane) movingFor(ns, name string) map[string]int64 {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	var out map[string]int64
	for id, until := range p.moving {
		if id.ns != ns || !topic.Match(id.filter, name) {
			continue
		}
		if out == nil {
			out = map[string]int64{}
		}
		out[id.filter] = (p.o.MoveGrace - until.Sub(now)).Milliseconds()
	}
	return out
}

// noteRefusal samples a refusal of a message on a topic.
func (p *Plane) noteRefusal(where, reason, peer, ns, name string, seq uint64, delivered int) {
	p.refusals.add(RefusalSample{Where: where, Reason: reason, Peer: peer, Topic: name, Seq: seq,
		Delivered: delivered, Moving: p.movingFor(ns, name), OwnCount: p.table.OwnCount(ns, name)})
}
