package replication

import (
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/message"
)

// The sources of a retained write, as the session manager names them. Only a
// client's QoS 1 or 2 publish has a barrier waiting on the forward: a refusal
// fails that barrier and the client is refused, and retries. The others have
// nobody to tell, so their forwards are held and, refused as misrouted,
// forwarded once more.
const (
	SourceClient     = "client"
	SourceClientQoS0 = "client-qos0"
	SourceWill       = "will"
	SourceModule     = "module"
)

// maxHeldForwards bounds the held forwards; past it the oldest is let go, as
// every forward was before forwards were held.
const maxHeldForwards = 4096

// heldForward is a forward no barrier waits on, kept until its peer answers.
type heldForward struct {
	msg     *message.Message
	source  string
	peer    string
	seq     uint64
	retried bool
}

type retainedHeld struct {
	mu   sync.Mutex
	held []*heldForward // in push order
}

// sourceLabel is the counter's source: a QoS 0 client publish is a client's.
func sourceLabel(source string) string {
	if source == SourceClientQoS0 {
		return SourceClient
	}
	return source
}

// holdForward keeps a forward until its peer acknowledges or refuses it.
func (r *Replicator) holdForward(peer string, seq uint64, msg *message.Message, source string, retried bool) {
	if seq == 0 {
		return
	}
	h := &r.rheld
	h.mu.Lock()
	h.held = append(h.held, &heldForward{msg: msg, source: source, peer: peer, seq: seq, retried: retried})
	if len(h.held) > maxHeldForwards {
		h.held = h.held[len(h.held)-maxHeldForwards:]
	}
	h.mu.Unlock()
}

// releaseHeld lets go of the forwards a cumulative acknowledgement covers.
func (r *Replicator) releaseHeld(peer string, upTo uint64) {
	h := &r.rheld
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.held) == 0 {
		return
	}
	kept := h.held[:0]
	for _, f := range h.held {
		if f.peer != peer || f.seq > upTo {
			kept = append(kept, f)
		}
	}
	h.held = kept
}

// takeHeld removes and returns the forward a refusal names, or nil.
func (r *Replicator) takeHeld(peer string, seq uint64) *heldForward {
	h := &r.rheld
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, f := range h.held {
		if f.peer == peer && f.seq == seq {
			h.held = append(h.held[:i], h.held[i+1:]...)
			return f
		}
	}
	return nil
}

// reforward decides a held forward refused as misrouted: wait while this
// node's map is below the version the refusal named; send once it has reached
// it, to that map's leader unless it is this node or the refuser; and never a
// second time. It is pure.
func reforward(retried bool, mapVersion, refusalMap uint64, leader, self, refuser string) (send, wait bool) {
	switch {
	case retried:
		return false, false
	case mapVersion < refusalMap:
		return false, true
	case leader == "" || leader == self || leader == refuser:
		return false, false
	}
	return true, false
}

// misroutedRefused handles a refusal of a retained write by a node that does
// not lead its shard: counted by source; a held forward is forwarded once more
// when this node's map reaches the refuser's, within the acknowledgement
// timeout, and counted dropped otherwise.
func (r *Replicator) misroutedRefused(peer string, a ack) {
	f := r.takeHeld(peer, a.Seq)
	source := SourceClient
	if f != nil {
		source = f.source
	}
	r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-refused", "source": sourceLabel(source)}, 1)
	if f == nil {
		return
	}
	go r.forwardAgain(peer, f, a.Map)
}

// forwardAgain waits, at most the acknowledgement timeout, for this node's map
// to reach refusalMap, then forwards f once more to its shard's leader, or
// counts it dropped.
func (r *Replicator) forwardAgain(refuser string, f *heldForward, refusalMap uint64) {
	deadline := time.Now().Add(r.o.AckTimeout)
	for {
		m := r.o.Map.Current()
		leader := ""
		if shard := r.o.Map.ShardOf(f.msg.Namespace, f.msg.Topic); shard >= 0 && shard < len(m.Shards) {
			leader = m.Shards[shard].Leader
		}
		send, wait := reforward(f.retried, m.Version, refusalMap, leader, r.o.Node, refuser)
		if send && r.up(leader) {
			seq := r.outboundFor(leader).push(frame{Kind: kindRetainedWrite, Namespace: f.msg.Namespace, Topic: f.msg.Topic, Epoch: m.Version, Msg: f.msg})
			r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-forwarded", "source": sourceLabel(f.source)}, 1)
			r.holdForward(leader, seq, f.msg, f.source, true)
			return
		}
		if !wait || !time.Now().Before(deadline) {
			break
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	r.count("hoardmq_retained_forwards_total", map[string]string{"outcome": "misrouted-dropped", "source": sourceLabel(f.source)}, 1)
	if f.source == SourceWill || f.source == SourceModule {
		r.log.Warn("retained write dropped: refused by a node that does not lead its shard, and not forwarded again", "source", f.source, "topic", f.msg.Topic, "refuser", refuser, "refusal_map", refusalMap)
	}
}
