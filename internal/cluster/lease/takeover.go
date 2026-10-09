package lease

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/store"
)

type takeoverRequest struct {
	Req       uint64 `json:"req"`
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	Epoch     uint64 `json:"epoch"`
	// Requester attribution (WI 1338): a duration, so clocks need not agree.
	ConnAgeMs   int64 `json:"connAgeMs,omitempty"`
	Prebuffered bool  `json:"prebuffered,omitempty"`
	Attributed  bool  `json:"attributed,omitempty"` // the requester filled the fields above
}

type takeoverAck struct {
	Req   uint64              `json:"req"`
	State *store.SessionState `json:"state,omitempty"`
	// Superseded says the previous owner has seen a grant above the
	// requester's: the session was not given up on this request's account.
	Superseded bool `json:"superseded,omitempty"`
	// Pending names the previous owner the answering node was still waiting
	// on for the session, sent only with no state: a requester that finds
	// nothing elsewhere judges that owner as it judges its own. An older build
	// neither sends nor reads it.
	Pending string `json:"pending,omitempty"`
}

// pull asks the previous owner for the session state after a takeover. It
// returns the state and whether the owner answered: an owner that answered
// with no state has nothing for the new owner to wait for, unless it names a
// pending owner of its own, while a missing or late answer may mean the owner
// is still serving the client. Superseded means the owner has seen a grant
// above this one and gave nothing up for it.
func (s *Service) pull(ctx context.Context, prev, ns, id string, epoch uint64, from Requester) (state *store.SessionState, answered, superseded bool, pending string) {
	if s.o.Transport == nil {
		return nil, false, false, ""
	}
	req := s.nextReq.Add(1)
	ch := make(chan takeoverAck, 1)
	s.mu.Lock()
	s.pulls[req] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pulls, req)
		s.mu.Unlock()
	}()
	tr := takeoverRequest{Req: req, Namespace: ns, ClientID: id, Epoch: epoch}
	if from != (Requester{}) {
		tr.Attributed, tr.ConnAgeMs, tr.Prebuffered = true, from.ConnAge.Milliseconds(), from.Prebuffered
	}
	body, _ := json.Marshal(tr)
	sctx, cancel := context.WithTimeout(ctx, s.o.TakeoverTimeout)
	defer cancel()
	// Timed from the send: the wait this measures is the one a CONNECT's
	// handshake slot is held for (WI 1691).
	asked := time.Now()
	if err := s.o.Transport.Send(sctx, prev, transport.TakeoverRequest, body); err != nil {
		s.log.Warn("takeover pull: previous owner unreachable", "owner", prev, "client", id, "err", err)
		s.takeoverTimes.notePull(time.Since(asked), false, false, false)
		return nil, false, false, ""
	}
	select {
	case a := <-ch:
		s.takeoverTimes.notePull(time.Since(asked), a.State != nil, true, a.Superseded)
		return a.State, true, a.Superseded, a.Pending
	case <-sctx.Done():
		s.log.Warn("takeover pull timed out", "owner", prev, "client", id)
		s.takeoverTimes.notePull(time.Since(asked), false, false, false)
		return nil, false, false, ""
	case <-s.stop:
		s.takeoverTimes.notePull(time.Since(asked), false, false, false)
		return nil, false, false, ""
	}
}

// pendingTakeover is one request waiting for a pass over its key. queuedAt
// is when it was handed to the queue, so the pass can say how long it waited
// for a worker (WI 1691); it is this node's own clock on both readings.
type pendingTakeover struct {
	req      takeoverRequest
	peer     string
	queuedAt time.Time
}

// ackOnControl marks a takeover acknowledgement carried on the request
// class when the data connection was absent; the request class's payloads
// are JSON objects, so the marker byte cannot be mistaken for one.
const ackOnControl byte = 0

// receiveTakeoverRequest runs on the previous owner's link goroutine: it
// queues the request and returns. The pass that serves the key seals the
// session and hands its state to the requester, closing the client with 0x8E.
// An acknowledgement that took the control lane, and a new owner's
// confirmation, are told apart by their marker bytes.
func (s *Service) receiveTakeoverRequest(peer string, payload []byte) {
	if len(payload) > 0 && payload[0] == ackOnControl {
		s.receiveTakeoverAck(peer, payload[1:])
		return
	}
	if len(payload) > 0 && payload[0] == confirmMarker {
		s.receiveConfirmation(peer, payload[1:])
		return
	}
	var r takeoverRequest
	if json.Unmarshal(payload, &r) != nil {
		return
	}
	s.takeovers.enqueue(key{r.Namespace, r.ClientID}, pendingTakeover{req: r, peer: peer, queuedAt: time.Now()})
}

// serveTakeover is one pass over a key: one Detach, one Take, one ack per
// request that arrived for the key — a client that retried on both
// survivors has both acquires answered from the same snapshot.
func (s *Service) serveTakeover(k key, payloads []any) {
	if d := s.o.TakeoverDelay; d > 0 {
		select {
		case <-time.After(d):
		case <-s.ctx.Done():
			return
		}
	}
	began := time.Now()
	var detachFor, takeFor time.Duration
	defer func() { s.takeoverTimes.notePass(time.Since(began), detachFor, takeFor) }()
	for _, p := range payloads {
		if pt, ok := p.(pendingTakeover); ok && !pt.queuedAt.IsZero() {
			s.takeoverTimes.noteQueued(began.Sub(pt.queuedAt))
		}
	}
	// Attribution before the detach (WI 1338): when this node still serves
	// the client live, a requester accepted after that connection was made
	// is either a second client with the same identifier or a socket that
	// waited in a listen backlog while the client went elsewhere; a
	// prebuffered CONNECT says which. Counted, not refused.
	// The session goes to the requester holding the highest grant token, and
	// only if nothing higher has been seen: a requester that lost the lease
	// before it asked is told so, and gives this node no reason to let go.
	for _, p := range payloads {
		if pt, ok := p.(pendingTakeover); ok {
			s.tokens.Note(k.ns, k.id, pt.req.Epoch)
		}
	}
	top := s.tokens.Seen(k.ns, k.id)
	current := func(pt pendingTakeover) bool { return top == 0 || pt.req.Epoch >= top }
	served := false
	for _, p := range payloads {
		pt, ok := p.(pendingTakeover)
		if !ok {
			continue
		}
		if current(pt) {
			served = true
			continue
		}
		s.count("hoardmq_lease_stale_token_total", map[string]string{"where": "takeover"}, 1)
		s.log.Info("takeover request superseded by a later grant", "client", k.id, "peer", pt.peer, "token", pt.req.Epoch, "highest", top)
		s.answerTakeover(pt, nil, true, "")
	}
	if !served {
		return
	}
	liveAge, live := s.o.Sessions.ConnAge(k.ns, k.id)
	if live {
		for _, p := range payloads {
			pt, ok := p.(pendingTakeover)
			if !ok || !pt.req.Attributed || liveAge <= time.Duration(pt.req.ConnAgeMs)*time.Millisecond {
				continue
			}
			s.olderTakeovers.Add(1)
			s.count("hoardmq_lease_takeover_requests_total", map[string]string{"live": "older"}, 1)
			if pt.req.Prebuffered {
				s.staleTakeovers.Add(1)
				s.count("hoardmq_lease_takeover_stale_total", nil, 1)
			}
		}
	}
	// What this takeover cost (WI 1495). liveAge above already says whether a
	// connection is here and how old it is; until now that answer was used
	// only for the attribution branch and then discarded. A lease move that
	// detaches a live client costs that client a reconnect; one that finds
	// nothing here costs nothing, and the ratio between them is what separates
	// a fast leader-crash recovery from a slow one.
	s.takeoversServed.Add(1)
	// Consumed here rather than in changed(): the lease change applies first,
	// so a delete there would take the record away from its only reader.
	at, adopted := s.adoptedAt.LoadAndDelete(k)
	if live {
		s.takeoversLive.Add(1)
		ms := liveAge.Milliseconds()
		s.detachedAgeTotalMs.Add(ms)
		for {
			old := s.detachedAgeMaxMs.Load()
			if ms <= old || s.detachedAgeMaxMs.CompareAndSwap(old, ms) {
				break
			}
		}
		if adopted {
			s.takeoversLiveAdopt.Add(1)
		}
	} else {
		s.takeoversGone.Add(1)
	}
	// The interval, whether or not a client was here: an adopted lease taken
	// away with nothing connected is as much a wasted adoption as one that
	// cost a live connection, and the two are told apart by Live.
	if adopted {
		if t, ok := at.(time.Time); ok {
			s.recordAdoptTakeover(k, s.o.Clock().Sub(t), liveAge, live)
		}
	}

	sealFor := top
	if sealFor == 0 {
		sealFor = ^uint64(0)
	}
	if at, held := s.o.Sessions.HeldAt(k.ns, k.id); held && at > sealFor && !s.o.RemoveOnHandOver {
		// Granted here again after these requests were made: the client
		// connected here at that grant keeps its connection.
		s.supersededByOwnGrant(k, payloads, current, at, top)
		return
	}
	// The client's connection is closed before the state is read, so nothing
	// is sent after the snapshot the new owner receives (a message sent in
	// between would reach the client twice, the second time without DUP).
	detached := time.Now()
	s.o.Sessions.Detach(k.ns, k.id, packet.SessionTakenOver)
	detachFor = time.Since(detached)
	// Snapshot and seal are one step: a message forwarded here after the
	// snapshot finds no session and is reported undelivered to its sender
	// instead of being enqueued into state that has left. Nothing is deleted
	// until the requester confirms it holds the session.
	taken := time.Now()
	var st *store.SessionState
	if s.o.RemoveOnHandOver {
		st, _ = s.o.Sessions.Take(k.ns, k.id, packet.SessionTakenOver)
	} else {
		res := s.o.Sessions.Seal(k.ns, k.id, packet.SessionTakenOver, sealFor)
		switch res.Outcome {
		case SealNewer:
			// The grant here landed between the look above and the seal.
			takeFor = time.Since(taken)
			s.supersededByOwnGrant(k, payloads, current, res.Token, top)
			return
		case SealSealed:
			st = res.State
			s.sealsMade.Add(1)
			s.count("hoardmq_lease_seals_total", map[string]string{"by": "takeover"}, 1)
			s.armSeal(k, res.Token)
		case SealAlready:
			// Asked again for a session already given: the first answer may not
			// have been waited for.
			st = res.State
			s.givenResent.Add(1)
			if res.Raised {
				s.armSeal(k, res.Token)
			}
		}
	}
	takeFor = time.Since(taken)
	if st == nil {
		// A late state this node was keeping for the client goes with the
		// lease.
		if st = s.adopting.takeLateFor(k); st != nil {
			s.lateStateHandedOn.Add(1)
		}
	}
	pending := ""
	if st == nil {
		pending = s.waitingOn(k)
		s.adopting.markAnsweredEmpty(k, time.Now())
	}
	s.log.Debug("takeover served", "client", k.id, "state", st != nil, "live", live, "adoptionUnfinished", s.adopting.unfinishedFor(k), "requests", len(payloads), "token", top, "pending", pending)
	for _, p := range payloads {
		if pt, ok := p.(pendingTakeover); ok && current(pt) {
			s.answerTakeover(pt, st, false, pending)
		}
	}
}

// supersededByOwnGrant answers every current requester superseded: this node
// holds the session at a grant above theirs.
func (s *Service) supersededByOwnGrant(k key, payloads []any, current func(pendingTakeover) bool, held, requested uint64) {
	s.count("hoardmq_lease_stale_token_total", map[string]string{"where": "takeover"}, 1)
	s.log.Info("takeover requests superseded by this node's own later grant", "client", k.id, "held", held, "requested", requested)
	for _, p := range payloads {
		if pt, ok := p.(pendingTakeover); ok && current(pt) {
			s.answerTakeover(pt, nil, true, "")
		}
	}
}

// answerTakeover sends one requester its answer: the state stamped with its
// token, or that it was superseded, or neither and the owner this node was
// still waiting on.
func (s *Service) answerTakeover(pt pendingTakeover, st *store.SessionState, superseded bool, pending string) {
	ack := takeoverAck{Req: pt.req.Req, Superseded: superseded, Pending: pending}
	if st != nil {
		copy := *st
		copy.Epoch = pt.req.Epoch
		ack.State = &copy
	}
	body, _ := json.Marshal(ack)
	ctx, cancel := context.WithTimeout(context.Background(), s.o.TakeoverTimeout)
	defer cancel()
	// The acknowledgement carries the session's state, so it rides the data
	// connection (WI 1340); when that connection is momentarily absent while
	// the peer is up it goes on the control connection instead — the
	// requester is waiting, and a lost acknowledgement costs it the state.
	if err := s.o.Transport.Send(ctx, pt.peer, transport.TakeoverAck, body); errors.Is(err, transport.ErrPeerDown) {
		_ = s.o.Transport.Send(ctx, pt.peer, transport.TakeoverRequest, append([]byte{ackOnControl}, body...))
	}
}

// receiveTakeoverAck delivers the pulled state to the waiting acquire.
func (s *Service) receiveTakeoverAck(peer string, payload []byte) {
	var a takeoverAck
	if json.Unmarshal(payload, &a) != nil {
		return
	}
	s.mu.Lock()
	ch := s.pulls[a.Req]
	s.mu.Unlock()
	if ch == nil {
		// The pull gave up before its answer came. The previous owner holds
		// the session sealed for this grant; this is the copy nearest to the
		// client, and importing it is what lets that owner let go.
		s.orphanedAcks.Add(1)
		if a.State != nil {
			s.orphanedAckStates.Add(1)
			s.log.Debug("takeover acknowledgement outlived its pull", "client", a.State.ClientID, "token", a.State.Epoch, "entries", len(a.State.Entries), "subscriptions", len(a.State.Subscriptions))
			s.keepLateState(peer, a.State)
		}
		return
	}
	select {
	case ch <- a:
	default:
	}
}

// keepLateState keeps the state from an acknowledgement nobody waits for. It
// was given up for this node's grant, named by the token it is stamped with;
// while no higher grant has been seen the session is this node's to serve, and
// the key's adoption is where a CONNECT, the worker and a takeover look for it.
// giver is the node that sent it, owed a confirmation once it is imported.
// Runs on the transport's reader: it queues and returns.
func (s *Service) keepLateState(giver string, st *store.SessionState) {
	if s.o.DiscardLateState {
		return
	}
	if seen := s.tokens.Seen(st.Namespace, st.ClientID); seen > st.Epoch || st.Epoch == 0 {
		s.lateStateLeaseMoved.Add(1)
		s.log.Warn("a late takeover acknowledgement's state dropped: the lease has moved on", "client", st.ClientID, "token", st.Epoch, "highest", seen)
		return
	}
	k := key{st.Namespace, st.ClientID}
	job := adoptJob{epoch: st.Epoch, prev: giver, takeover: true}
	s.adopting.deliver(k, job, st)
	s.adopts.enqueue(k, job)
}
