package routing

import (
	"sync"
	"sync/atomic"
	"time"
)

// A shared-group message decided on this node for another node's publish and
// given to a third node is acknowledged to the publisher's node before the
// member has it. The publisher's node therefore holds that forward pending
// (outbound.pending) until this node relays the give's outcome, which the
// give watcher below decides from this node's own stream to the member.

// giveRef names a give: the peer it went to and its sequence on this node's
// stream to that peer. The zero value is no give.
type giveRef struct {
	peer string
	seq  uint64
}

// watchedGive is a give whose outcome the decide's sender is waiting for.
type watchedGive struct {
	decidePeer string
	decideSeq  uint64
	give       giveRef
	deadline   time.Time
}

// maxWatchedGives bounds the gives awaiting an outcome on this node; one past
// it is relayed refused at once.
const maxWatchedGives = 4096

type giveWatcher struct {
	mu      sync.Mutex
	entries []watchedGive
	count   atomic.Int64
	wake    chan struct{}
}

// wakeGives prompts the watcher after a stream's marks moved. It costs an
// atomic load while nothing is watched.
func (p *Plane) wakeGives() {
	if p.gives.count.Load() == 0 {
		return
	}
	select {
	case p.gives.wake <- struct{}{}:
	default:
	}
}

// watchGives takes the gives a batch from decidePeer made, once that batch's
// acknowledgement naming them has been sent.
func (p *Plane) watchGives(decidePeer string, gives []watchedGive) {
	if len(gives) == 0 {
		return
	}
	deadline := time.Now().Add(p.o.GiveTimeout)
	var over []watchedGive
	p.gives.mu.Lock()
	for _, g := range gives {
		g.decidePeer, g.deadline = decidePeer, deadline
		if len(p.gives.entries) >= maxWatchedGives {
			over = append(over, g)
			continue
		}
		p.gives.entries = append(p.gives.entries, g)
	}
	p.gives.count.Store(int64(len(p.gives.entries)))
	p.gives.mu.Unlock()
	for _, g := range over {
		p.relayGive(g, false)
	}
	select {
	case p.gives.wake <- struct{}{}:
	default:
	}
}

// giveLoop relays each watched give's outcome: held once this node's stream
// to the member passes it, refused once that stream fails or refuses it, or
// at the entry's deadline. It never waits on a peer.
func (p *Plane) giveLoop() {
	defer p.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		next := p.resolveGives(time.Now())
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if !next.IsZero() {
			timer.Reset(time.Until(next))
		}
		select {
		case <-p.stop:
			return
		case <-p.gives.wake:
		case <-timer.C:
		}
	}
}

// resolveGives relays every watched give whose outcome is known or whose
// deadline has passed, and returns the earliest deadline still watched.
func (p *Plane) resolveGives(now time.Time) time.Time {
	type relay struct {
		g    watchedGive
		held bool
	}
	var out []relay
	var next time.Time
	p.gives.mu.Lock()
	// One hold of p.mu for the whole scan: it runs on every acknowledgement
	// while any give is watched.
	p.mu.Lock()
	verdicts := make([]int, len(p.gives.entries))
	for i, g := range p.gives.entries {
		verdicts[i] = p.giveVerdictLocked(g.give)
	}
	p.mu.Unlock()
	kept := p.gives.entries[:0]
	for i, g := range p.gives.entries {
		v := verdicts[i]
		switch {
		case v == forwardPass:
			out = append(out, relay{g, true})
		case v != forwardWait || !now.Before(g.deadline):
			out = append(out, relay{g, false})
		default:
			kept = append(kept, g)
			if next.IsZero() || g.deadline.Before(next) {
				next = g.deadline
			}
		}
	}
	for i := len(kept); i < len(p.gives.entries); i++ {
		p.gives.entries[i] = watchedGive{}
	}
	p.gives.entries = kept
	p.gives.count.Store(int64(len(kept)))
	p.gives.mu.Unlock()
	for _, r := range out {
		p.relayGive(r.g, r.held)
	}
	return next
}

// giveVerdictLocked judges a give on this node's stream to its member as a
// barrier judges a forward. Callers hold p.mu.
func (p *Plane) giveVerdictLocked(g giveRef) int {
	o := p.out[g.peer]
	if o == nil {
		return forwardFailed
	}
	_, refused := o.refused[g.seq]
	_, pending := o.pending[g.seq]
	return forwardVerdict(g.seq, o.lastFailed, o.acked, refused, pending)
}

// relayGive tells the decide's sender whether the member held the give.
func (p *Plane) relayGive(g watchedGive, held bool) {
	outcome := "held"
	if !held {
		outcome = "refused"
		p.refusals.add(RefusalSample{Where: "owner", Reason: "give-refused", Peer: g.give.peer, Seq: g.give.seq})
	}
	p.count("hoardmq_group_gives_relayed_total", map[string]string{"outcome": outcome}, 1)
	p.sendControl(g.decidePeer, fwdGiven, forwardEnvelope{Seq: g.decideSeq, Failed: !held})
}
