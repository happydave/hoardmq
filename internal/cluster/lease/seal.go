package lease

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/happydave/hoardmq/internal/cluster/transport"
	"github.com/happydave/hoardmq/internal/store"
)

// The sealed hand-over. A takeover pass seals the session rather than removing it: nothing is journaled, so no
// delete reaches a replica, and the state is kept here. The new owner, once it
// holds the session at its grant and its records are durable, confirms. The
// confirmation, the lease coming back, the lease's end, or a bound's check
// that finds one of the new owner's targets holding the session ends the seal.

// SealOutcome says what Sessions.Seal did.
type SealOutcome int

const (
	// SealNone: no session here, sealed or not.
	SealNone SealOutcome = iota
	// SealSealed: the session was sealed now.
	SealSealed
	// SealAlready: it was already sealed; SealResult.Raised says whether for a
	// lower token.
	SealAlready
	// SealNewer: the session is held here at a token above the one asked for,
	// and was left as it is.
	SealNewer
)

// SealResult is Sessions.Seal's answer: a copy of the state the caller may
// stamp, and the token the copy is now sealed for (for SealNewer, the held
// session's own).
type SealResult struct {
	State   *store.SessionState
	Token   uint64
	Outcome SealOutcome
	Raised  bool
}

// confirmMarker begins a confirmation carried on the takeover-request class.
// That class's requests are JSON objects and ackOnControl is 0, so neither can
// be mistaken for one; a build that does not know the marker fails to decode
// the payload and drops it.
const confirmMarker byte = 1

// confirmation is the wire form: the new owner holds the client's session at
// Token, and its records are durable at its replicas. FromSeq is a publish
// sequence the new owner read once the session was installed there: every
// publish it numbered from there on was routed after the install. Zero, or
// absent from an older build, is no mark.
type confirmation struct {
	Namespace string `json:"ns"`
	ClientID  string `json:"id"`
	Token     uint64 `json:"token"`
	FromSeq   uint64 `json:"fromSeq,omitempty"`
}

// confirmIn is one confirmation queued for its key on the giver.
type confirmIn struct {
	token   uint64
	peer    string
	fromSeq uint64
	at      time.Time // received
}

// reclaimOnConfirm decides whether a confirmation for token confirmed may end
// what this node keeps of the session. It is pure: acting on it journals a
// delete. It refuses only when this node holds the lease itself at or above
// the confirmed grant, which means the client came back here after it.
func reclaimOnConfirm(confirmed uint64, ownerHere bool, ownerEpoch uint64) bool {
	return !ownerHere || ownerEpoch < confirmed
}

// sealVerdict is what a bound's check does with a seal nothing confirmed.
type sealVerdict int

const (
	sealKeep sealVerdict = iota
	// sealDropLeaseGone: the client's lease no longer exists, so no node owns
	// the session.
	sealDropLeaseGone
	// sealDropHeld: a target of the lease's owner holds the session at the
	// lease's grant or a later one.
	sealDropHeld
)

// sealEnds decides what a bound's check does with a seal nothing confirmed.
// It is pure: dropping journals a delete. lease, owner and token are the
// lease as read now, and self is this node; asked and heldAt are the owner the
// check asked about and the epoch it saw one of that owner's targets hold the
// session at (zero for none). While the lease is this node's the copy waits
// for the adoption or CONNECT here that will take it. No time ends a seal: a
// new owner alive by the log may not have fetched it, and this copy can be
// the only one.
func sealEnds(lease bool, owner, self string, token uint64, asked string, heldAt uint64) sealVerdict {
	switch {
	case !lease:
		return sealDropLeaseGone
	case owner == self:
		return sealKeep
	case asked == owner && token > 0 && heldAt >= token:
		return sealDropHeld
	}
	return sealKeep
}

// receiveConfirmation queues a confirmation for its key; it runs on the
// transport's reader and does not block.
func (s *Service) receiveConfirmation(peer string, payload []byte) {
	var c confirmation
	if json.Unmarshal(payload, &c) != nil || c.Token == 0 {
		return
	}
	s.confirmsReceived.Add(1)
	s.confirmed.enqueue(key{c.Namespace, c.ClientID}, confirmIn{token: c.Token, peer: peer, fromSeq: c.FromSeq, at: time.Now()})
}

// serveConfirmed acts on the highest confirmation queued for a key: the
// sealed copy is dropped, or, when the session was never sealed here (no pull
// came), an offline session held at or below the token is evicted. Either
// journals the delete, now that the new owner's whole write is durable.
func (s *Service) serveConfirmed(k key, payloads []any) {
	var top, mark uint64
	peer := ""
	var at time.Time
	for _, p := range payloads {
		if c, ok := p.(confirmIn); ok && c.token > top {
			top, peer, mark, at = c.token, c.peer, c.fromSeq, c.at
		}
	}
	if top == 0 {
		return
	}
	s.tokens.Note(k.ns, k.id, top)
	if s.o.Confirmed != nil {
		s.o.Confirmed(k.ns, k.id, peer, top)
	}
	held, _ := s.o.Sessions.HeldAt(k.ns, k.id)
	l, ok := s.table.Lookup(k.ns, k.id)
	if !reclaimOnConfirm(top, ok && l.Owner == s.o.Node, l.Epoch) {
		s.confirmsIgnored.Add(1)
		return
	}
	switch {
	case s.o.Sessions.DropSealed(k.ns, k.id, top):
		s.sealsConfirmed.Add(1)
		s.count("hoardmq_lease_seal_ends_total", map[string]string{"end": "confirmed"}, 1)
		s.log.Debug("sealed session dropped: the new owner confirmed", "client", k.id, "token", top, "by", peer)
	case s.o.Sessions.EvictOffline(k.ns, k.id, top):
		s.reclaimedOnConfirm.Add(1)
		s.noteReclaimed(k, held, top, peer)
		s.log.Debug("session reclaimed: the new owner confirmed", "client", k.id, "token", top, "by", peer)
	default:
		s.confirmsIgnored.Add(1)
		return
	}
	s.noteConfirmActed(at)
	// After the drop or the eviction: the eviction is what marks the move.
	if s.o.MoveEnded != nil {
		s.o.MoveEnded(k.ns, k.id, peer, mark)
	}
}

// noteConfirmActed counts a confirmation acted on and its wait since receipt.
func (s *Service) noteConfirmActed(received time.Time) {
	s.confirmsActed.Add(1)
	if received.IsZero() {
		return
	}
	d := int64(time.Since(received))
	s.confirmActNsTotal.Add(d)
	storeMax(&s.confirmActNsMax, d)
}

// SealCounts is the sealed copies held here now, and those sealed, ended on a
// confirmation and ended at the bound since start, read from counters.
func (s *Service) SealCounts() (held int, made, confirmed, expired int64) {
	return s.sealedNow(), s.sealsMade.Load(), s.sealsConfirmed.Load(), s.sealsExpired.Load()
}

// sealedNow is the session manager's count of sealed copies; zero for a
// Service built without one.
func (s *Service) sealedNow() int {
	if s.o.Sessions == nil {
		return 0
	}
	return s.o.Sessions.SealedCount()
}

// armSeal starts the bound for a copy sealed for token: when it passes, the
// seal's check is queued.
func (s *Service) armSeal(k key, token uint64) {
	time.AfterFunc(s.o.SealTimeout, func() { s.sealChecks.enqueue(k, token) })
}

// serveSealCheck runs the bound's check for the highest token queued for a
// key; a lower one's copy has since been sealed again for a later grant.
func (s *Service) serveSealCheck(k key, payloads []any) {
	var top uint64
	for _, p := range payloads {
		if t, ok := p.(uint64); ok && t > top {
			top = t
		}
	}
	if top > 0 {
		s.sealBound(k, top)
	}
}

// sealBound is the bound's check of a seal nothing confirmed. A copy since
// used, dropped or re-sealed for a higher token is not this check's. While
// the lease names another owner, that owner's targets are asked whether one
// holds the session at its grant; a copy kept is checked again one bound
// later.
func (s *Service) sealBound(k key, token uint64) {
	select {
	case <-s.stop:
		return
	default:
	}
	if _, at, ok := s.o.Sessions.Sealed(k.ns, k.id); !ok || at > token {
		return
	}
	l, ok := s.table.Lookup(k.ns, k.id)
	asked, heldAt := "", uint64(0)
	if ok && l.Owner != s.o.Node && s.o.HeldAtTargets != nil {
		asked = l.Owner
		heldAt = s.o.HeldAtTargets(s.ctx, k.ns, k.id, l.Owner, l.Epoch)
		outcome := "not-held"
		if heldAt >= l.Epoch && heldAt > 0 {
			outcome = "held"
			s.sealChecksHeld.Add(1)
		} else {
			s.sealChecksNotHeld.Add(1)
		}
		s.count("hoardmq_lease_seal_checks_total", map[string]string{"outcome": outcome}, 1)
		// The lease may have moved while its owner's targets were asked.
		l, ok = s.table.Lookup(k.ns, k.id)
	}
	verdict := sealEnds(ok, l.Owner, s.o.Node, l.Epoch, asked, heldAt)
	if verdict == sealKeep {
		s.log.Debug("sealed session kept at the bound: nothing shows it held elsewhere", "client", k.id, "token", token, "owner", l.Owner, "heldAt", heldAt)
		s.armSeal(k, token)
		return
	}
	if !s.o.Sessions.DropSealed(k.ns, k.id, token) {
		return
	}
	s.sealsExpired.Add(1)
	end := "held"
	if verdict == sealDropLeaseGone {
		end = "lease-gone"
		s.sealsLeaseGone.Add(1)
	} else {
		s.sealsHeld.Add(1)
	}
	s.count("hoardmq_lease_seal_ends_total", map[string]string{"end": end}, 1)
	s.log.Info("sealed session dropped unconfirmed", "client", k.id, "token", token, "end", end, "owner", l.Owner, "heldAt", heldAt)
}

// confirmJob is one confirmation this node owes a previous owner.
type confirmJob struct {
	k     key
	token uint64
	giver string
	// moved are nodes that stopped being the session's replica target at a
	// shard-map change; each keeps its copy until this confirmation.
	moved  []string
	queued time.Time
	due    time.Time
}

// confirmQueue holds confirmations waiting for their session to be held here
// and durable.
type confirmQueue struct {
	mu   sync.Mutex
	jobs []confirmJob
	wake chan struct{}
}

// confirmPoll is how often a waiting confirmation is looked at again.
const confirmPoll = 50 * time.Millisecond

// confirmLater queues a confirmation for the client at token: to giver, when
// this node took the lease from giver for that grant, and to the shard's
// holders this node does not replicate to.
func (s *Service) confirmLater(k key, token uint64, giver string) {
	if giver == s.o.Node {
		giver = ""
	}
	if (giver == "" && s.o.Unfed == nil) || token == 0 || s.o.Transport == nil || s.o.RemoveOnHandOver {
		return
	}
	now := time.Now()
	q := &s.confirmOut
	q.mu.Lock()
	q.jobs = append(q.jobs, confirmJob{k: k, token: token, giver: giver, queued: now, due: now})
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// ConfirmMoved queues a confirmation of a session this node holds, at the
// token it holds it at, to nodes that stopped being its replica targets at a
// shard-map change. It is sent once the session's records are durable at its
// targets now, which includes the rebalance's whole write to any it gained.
func (s *Service) ConfirmMoved(ns, id string, to []string) {
	if len(to) == 0 || s.o.Transport == nil || s.o.RemoveOnHandOver {
		return
	}
	token, ok := s.o.Sessions.HeldAt(ns, id)
	if !ok || token == 0 {
		return
	}
	now := time.Now()
	q := &s.confirmOut
	q.mu.Lock()
	q.jobs = append(q.jobs, confirmJob{k: key{ns, id}, token: token, moved: append([]string(nil), to...), queued: now, due: now})
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// confirmLoop sends confirmations. A job waits until its session is held here
// at its token or above; the jobs ready together share one durability wait.
func (s *Service) confirmLoop() {
	defer close(s.confirmDone)
	q := &s.confirmOut
	for {
		q.mu.Lock()
		pending := len(q.jobs)
		q.mu.Unlock()
		var tick <-chan time.Time
		if pending > 0 {
			tick = time.After(confirmPoll)
		}
		select {
		case <-s.stop:
			return
		case <-q.wake:
		case <-tick:
		}
		s.confirmDue()
	}
}

func (s *Service) confirmDue() {
	q := &s.confirmOut
	now := time.Now()
	q.mu.Lock()
	var due, wait []confirmJob
	for _, j := range q.jobs {
		if now.Before(j.due) {
			wait = append(wait, j)
		} else {
			due = append(due, j)
		}
	}
	q.jobs = wait
	q.mu.Unlock()
	if len(due) == 0 {
		return
	}
	var ready []confirmJob
	for _, j := range due {
		if at, ok := s.o.Sessions.HeldAt(j.k.ns, j.k.id); ok && at >= j.token {
			ready = append(ready, j)
			continue
		}
		if now.Sub(j.queued) >= 2*s.o.TakeoverTimeout {
			s.confirmsUnheld.Add(1)
			s.log.Debug("confirmation given up: session not held here", "client", j.k.id, "token", j.token, "giver", j.giver, "moved", j.moved)
			continue
		}
		j.due = now.Add(confirmPoll)
		s.requeueConfirm(j)
	}
	if len(ready) == 0 {
		return
	}
	scopes := make([]store.Scope, 0, len(ready))
	for _, j := range ready {
		scopes = append(scopes, store.Scope{Namespace: j.k.ns, ClientID: j.k.id})
	}
	err := s.durable(scopes)
	if err == nil {
		for _, j := range ready {
			s.sendConfirmation(j)
		}
		return
	}
	// A refused key fails a shared wait at once, so each is asked alone; a
	// wait that ran out of time would run out again for each, so all wait.
	timedOut := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
	for _, j := range ready {
		if !timedOut && s.durable([]store.Scope{{Namespace: j.k.ns, ClientID: j.k.id}}) == nil {
			s.sendConfirmation(j)
			continue
		}
		if time.Since(j.queued) >= s.o.TTL {
			s.confirmsUndurable.Add(1)
			s.log.Debug("confirmation given up: not durable", "client", j.k.id, "token", j.token, "giver", j.giver, "moved", j.moved, "timedOut", timedOut)
			continue
		}
		j.due = time.Now().Add(s.o.FallbackTimeout)
		s.requeueConfirm(j)
	}
}

func (s *Service) requeueConfirm(j confirmJob) {
	q := &s.confirmOut
	q.mu.Lock()
	q.jobs = append(q.jobs, j)
	q.mu.Unlock()
}

func (s *Service) durable(scopes []store.Scope) error {
	ctx, cancel := context.WithTimeout(s.ctx, s.o.FallbackTimeout)
	defer cancel()
	return s.o.Sessions.Durable(ctx, scopes)
}

func (s *Service) sendConfirmation(j confirmJob) {
	c := confirmation{Namespace: j.k.ns, ClientID: j.k.id, Token: j.token}
	if s.o.NextPublishSeq != nil {
		// Read after the session was found held here (confirmDue), so every
		// publish numbered from it on was routed after the install.
		c.FromSeq = s.o.NextPublishSeq()
	}
	body, _ := json.Marshal(c)
	payload := append([]byte{confirmMarker}, body...)
	ctx, cancel := context.WithTimeout(s.ctx, s.o.TakeoverTimeout)
	defer cancel()
	send := func(to string) bool {
		if err := s.o.Transport.Send(ctx, to, transport.TakeoverRequest, payload); err != nil {
			s.confirmsUnsent.Add(1)
			s.log.Debug("confirmation not sent", "client", j.k.id, "token", j.token, "to", to, "err", err)
			return false
		}
		s.confirmsSent.Add(1)
		s.log.Debug("confirmation sent", "client", j.k.id, "token", j.token, "to", to)
		return true
	}
	if j.giver != "" {
		send(j.giver)
	}
	for _, to := range j.moved {
		if to != j.giver && to != s.o.Node && send(to) {
			s.confirmsSentMoved.Add(1)
			s.count("hoardmq_lease_confirmations_moved_total", nil, 1)
		}
	}
	if s.o.Unfed == nil {
		return
	}
	for _, h := range s.o.Unfed(j.k.ns, j.k.id) {
		if h != j.giver && h != s.o.Node && !contains(j.moved, h) && send(h) {
			s.confirmsSentHolders.Add(1)
		}
	}
}
