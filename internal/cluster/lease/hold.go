package lease

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/happydave/hoardmq/internal/cluster/consensus"
	"github.com/happydave/hoardmq/internal/packet"
)

// ErrBelowLevel refuses a proposal from a node whose build is below the
// cluster's feature level: what it would propose is formed with semantics the
// cluster has left. Only its alive record, which tells the cluster what the
// build understands, is proposed.
var ErrBelowLevel = errors.New("lease: this build is below the cluster's feature level and proposes only its alive record")

// proposable decides whether this node may propose an operation of kind.
func proposable(kind OpKind, below bool) bool { return !below || kind == OpAlive }

// memberBelow reports whether a member advertising advertised is below the
// cluster's level; zero is the baseline, level 1.
func memberBelow(advertised, cluster uint64) bool { return max(advertised, 1) < cluster }

// MemberBelow reports whether the log's latest alive record from node
// advertises a level below the cluster's. A node with no record is not.
func (t *Table) MemberBelow(node string) bool {
	n, ok := t.Node(node)
	return ok && memberBelow(n.Level, t.Level())
}

// enterBelow runs once when this node falls below the cluster's level. It
// stops what its table would keep doing: its clients go elsewhere, its Will
// and expiry timers are disarmed (its table no longer learns that a client
// reconnected elsewhere), and a node still recovering seals the sessions its
// table says it lost, since the reconciliation that would end recovery never
// runs.
func (s *Service) enterBelow() {
	reason := packet.ServerBusy
	var r *Refusal
	if errors.As(s.refusal(), &r) && r.Reason == packet.UseAnotherServer {
		reason = packet.UseAnotherServer
	}
	s.o.Sessions.FenceAll(reason)
	s.mu.Lock()
	timers := s.timers
	s.timers = map[key]*adopted{}
	s.mu.Unlock()
	for _, a := range timers {
		a.cancel()
	}
	if s.o.Core.Recovering() {
		s.holdReconcile()
	}
}

// holdReconcile is a node below the level's reconciliation: it seals every
// session its table names another owner of, which withdraws the session's
// routes and deletes nothing. It arms no timer, queues no adoption, evicts
// nothing and arms no seal check: each of those acts on a table that has
// stopped learning.
func (s *Service) holdReconcile() {
	me := s.o.Node
	sealed := 0
	s.o.Sessions.Each(func(ns, id string) {
		l, ok := s.table.Lookup(ns, id)
		if !ok || l.Owner == me {
			return
		}
		if res := s.o.Sessions.Seal(ns, id, packet.SessionTakenOver, l.Epoch); res.Outcome == SealSealed {
			sealed++
			s.sealsMade.Add(1)
			s.sealsByRecovery.Add(1)
		}
	})
	if sealed > 0 {
		s.count("hoardmq_lease_seals_total", map[string]string{"by": "recovery"}, float64(sealed))
	}
	s.log.Info("below the cluster's level: sessions owned elsewhere sealed, nothing else reconciled", "sealed", sealed)
}

// TransferTarget is transferTarget over the core's members and this table's
// levels, for a leader handing leadership over before its own removal.
func (s *Service) TransferTarget() string {
	return transferTarget(s.o.Node, s.o.Core.Members(), s.table.MemberBelow)
}

// transferTarget picks the voter a node that holds the log hands leadership
// to: a caught-up voter other than itself whose latest advertisement is at the
// cluster's level, else any caught-up voter other than itself, else none.
// Names are tried in order, so the choice is stable.
func transferTarget(self string, members []consensus.Member, below func(string) bool) string {
	var any string
	ms := append([]consensus.Member(nil), members...)
	sort.Slice(ms, func(i, j int) bool { return ms[i].Name < ms[j].Name })
	for _, m := range ms {
		if m.Name == self || m.Role != consensus.Voter || !m.CaughtUp {
			continue
		}
		if !below(m.Name) {
			return m.Name
		}
		if any == "" {
			any = m.Name
		}
	}
	return any
}

// transferIfHeld runs on the tick: a node that holds the log answers no
// proposal, so while it leads it hands leadership over.
func (s *Service) transferIfHeld() {
	if _, held := s.o.Core.Held(); !held || s.o.Core.Status().Role != "leader" {
		return
	}
	to := transferTarget(s.o.Node, s.o.Core.Members(), s.table.MemberBelow)
	if to == "" {
		s.count("hoardmq_cluster_feature_level_transfers_total", map[string]string{"outcome": "no-target"}, 1)
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.o.TTL)
	defer cancel()
	outcome := "asked"
	if err := s.o.Core.TransferLeadership(ctx, to); err != nil {
		outcome = "failed"
		s.log.Warn("leadership transfer from a node that holds the log failed", "to", to, "err", err)
	} else {
		s.log.Info("this node holds the log and leads: leadership handed over", "to", to)
	}
	s.count("hoardmq_cluster_feature_level_transfers_total", map[string]string{"outcome": outcome}, 1)
	s.audit("cluster.level", s.o.Node, "transfer", map[string]string{"to": to, "outcome": outcome})
}

// handOffBelow runs on the leader's tick: a member below the cluster's level
// serves no client, so the leases it owns are handed to this node with the
// drain's operation, one member at a time.
func (s *Service) handOffBelow() {
	if s.o.Core.Status().Role != "leader" || s.BelowLevel() || !s.handingOff.CompareAndSwap(false, true) {
		return
	}
	var target string
	for _, m := range s.o.Core.Members() {
		if m.Name != s.o.Node && s.table.Live(m.Name) && s.table.MemberBelow(m.Name) && s.table.CountOwnedBy(m.Name) > 0 {
			target = m.Name
			break
		}
	}
	if target == "" {
		s.handingOff.Store(false)
		return
	}
	go func() {
		defer s.handingOff.Store(false)
		ctx, cancel := context.WithTimeout(s.ctx, s.o.TakeoverTimeout)
		defer cancel()
		res, err := s.submit(ctx, Op{Kind: OpHandoff, Node: target, To: s.o.Node, Now: s.o.Clock()})
		outcome := "ok"
		switch {
		case err != nil:
			outcome = "failed"
		case !res.OK:
			outcome = "refused"
		}
		s.count("hoardmq_lease_handoffs_below_level_total", map[string]string{"outcome": outcome}, 1)
		s.audit("lease.handoff-below", target, outcome, map[string]string{"to": s.o.Node, "moved": fmt.Sprint(res.Moved), "reason": res.Reason})
		s.log.Info("leases of a member below the cluster's level handed to the leader", "member", target, "outcome", outcome, "moved", res.Moved)
	}()
}
