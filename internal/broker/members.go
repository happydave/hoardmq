package broker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/happydave/hoardmq/internal/admin"
	"github.com/happydave/hoardmq/internal/cluster/consensus"
)

// removalFacts is everything a removal is decided on, gathered by the leader.
type removalFacts struct {
	Target, Self string
	Members      []consensus.Member
	// Alive is the log's verdict on each member, Reachable this node's link
	// to it as the fence reads it; this node is both.
	Alive, Reachable map[string]bool
	// The target's leases, its hand-off mark and whether it is below the
	// cluster's level.
	OwnedLeases   int
	Leaving, Held bool
	// Transfer is the voter leadership would go to when the target leads.
	Transfer string
}

// removalRefusal is why a removal is refused: a reason code and a sentence
// with the figures.
type removalRefusal struct {
	Reason, Detail string
}

// decideRemoval decides whether the leader may propose removing a member. It
// refuses a node that is not a member, the last voter, a removal after which
// the voters left that are alive and reachable are not a strict majority of
// the voters left, a member that owns leases, a live member that is neither
// leaving nor held, and its own removal with nobody to hand leadership to.
func decideRemoval(f removalFacts) *removalRefusal {
	var target *consensus.Member
	var voters []string
	for i, m := range f.Members {
		if m.Name == f.Target {
			target = &f.Members[i]
		}
		if m.Role == consensus.Voter {
			voters = append(voters, m.Name)
		}
	}
	if target == nil {
		return &removalRefusal{"not-a-member", fmt.Sprintf("%s is not a member", f.Target)}
	}
	if target.Role == consensus.Voter && len(voters) == 1 {
		return &removalRefusal{"last-voter", fmt.Sprintf("%s is the only voter", f.Target)}
	}
	left, reached := 0, 0
	for _, v := range voters {
		if v == f.Target {
			continue
		}
		left++
		if v == f.Self || (f.Alive[v] && f.Reachable[v]) {
			reached++
		}
	}
	if reached*2 <= left {
		return &removalRefusal{"quorum", fmt.Sprintf("after removing %s, %d of the %d voters left are alive and reachable, not a majority", f.Target, reached, left)}
	}
	if f.OwnedLeases > 0 {
		return &removalRefusal{"owns-leases", fmt.Sprintf("%s owns %d leases: drain it, or wait for them to be adopted once it is declared dead", f.Target, f.OwnedLeases)}
	}
	if f.Alive[f.Target] && !f.Leaving && !f.Held {
		return &removalRefusal{"not-drained", fmt.Sprintf("%s is alive and neither drained nor below the cluster's level: drain it first", f.Target)}
	}
	if f.Target == f.Self && f.Transfer == "" {
		return &removalRefusal{"no-transfer-target", fmt.Sprintf("%s leads and no other caught-up voter can take leadership", f.Target)}
	}
	return nil
}

// removalFacts reads what decideRemoval needs from this node's core, lease
// table and links.
func (s *Server) removalFacts(target string) removalFacts {
	self := adminBackend{s}.Node()
	table := s.leases.Table()
	f := removalFacts{Target: target, Self: self, Members: s.consensus.Members(), Alive: map[string]bool{}, Reachable: map[string]bool{},
		OwnedLeases: table.CountOwnedBy(target), Leaving: table.Leaving(target), Held: table.MemberBelow(target)}
	for _, m := range f.Members {
		f.Alive[m.Name] = m.Name == self || table.Live(m.Name)
		f.Reachable[m.Name] = m.Name == self || s.leases.PeerReachable(m.Name)
	}
	if target == self {
		f.Transfer = s.leases.TransferTarget()
	}
	return f
}

// removalBound is how long a removal, or the leadership transfer before it,
// may take: ten election timeouts.
func (s *Server) removalBound() time.Duration {
	r := s.config().Cluster.Raft
	return 10 * time.Duration(r.ElectionTicks) * time.Duration(r.TickMs) * time.Millisecond
}

// removeMember is the admin API's removal. A node that does not lead names
// the leader, and the route forwards the call there; the leader decides,
// hands leadership over first when it is the target, and proposes.
func (s *Server) removeMember(ctx context.Context, node string) (admin.MemberRemoval, error) {
	if s.consensus == nil || s.leases == nil {
		return admin.MemberRemoval{}, admin.Conflict("this node runs no consensus core")
	}
	self := adminBackend{s}.Node()
	leader, ok := s.consensus.Leader()
	if !ok {
		return admin.MemberRemoval{}, &admin.Error{Status: http.StatusServiceUnavailable, Detail: "no leader is known; retry"}
	}
	if leader != self {
		return admin.MemberRemoval{}, &admin.Error{Status: http.StatusConflict, Owner: leader, Detail: "node " + leader + " leads; the removal is made there"}
	}
	f := s.removalFacts(node)
	if r := decideRemoval(f); r != nil {
		status := http.StatusConflict
		switch r.Reason {
		case "not-a-member":
			status = http.StatusNotFound
		case "no-transfer-target":
			status = http.StatusServiceUnavailable
		}
		return admin.MemberRemoval{}, &admin.Error{Status: status, Detail: r.Reason + ": " + r.Detail}
	}
	ctx, cancel := context.WithTimeout(ctx, s.removalBound())
	defer cancel()
	if node == self {
		return admin.MemberRemoval{}, s.handOverLeadership(ctx, f.Transfer)
	}
	if err := s.consensus.RemoveMember(ctx, node); err != nil {
		var nl *consensus.NotLeaderError
		switch {
		case errors.Is(err, consensus.ErrNotMember):
			return admin.MemberRemoval{}, admin.NotFound(node + " is not a member")
		case errors.As(err, &nl) && nl.Leader != "":
			return admin.MemberRemoval{}, &admin.Error{Status: http.StatusConflict, Owner: nl.Leader, Detail: "node " + nl.Leader + " leads; the removal is made there"}
		case errors.Is(err, context.DeadlineExceeded):
			return admin.MemberRemoval{}, &admin.Error{Status: http.StatusServiceUnavailable, Detail: "the removal did not apply in time; it may still apply: read cluster status"}
		}
		return admin.MemberRemoval{}, err
	}
	s.log.Info("consensus member removed", "member", node)
	return admin.MemberRemoval{Node: node, Members: s.memberViews()}, nil
}

// handOverLeadership moves leadership from this node, which is to be removed,
// to another voter, then answers the 409 that sends the removal there.
func (s *Server) handOverLeadership(ctx context.Context, to string) error {
	self := adminBackend{s}.Node()
	if err := s.consensus.TransferLeadership(ctx, to); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	for {
		if l, ok := s.consensus.Leader(); ok && l != self {
			s.log.Info("leadership handed over before this node's removal", "to", l)
			return &admin.Error{Status: http.StatusConflict, Owner: l, Detail: "leadership moved to " + l + "; the removal is made there: repeat it if it was not forwarded"}
		}
		select {
		case <-ctx.Done():
			return &admin.Error{Status: http.StatusServiceUnavailable, Detail: "leadership did not move to " + to + " in time; retry"}
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// memberViews is the membership as the cluster view lists it.
func (s *Server) memberViews() []admin.Member {
	members := s.consensus.Members()
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	out := make([]admin.Member, 0, len(members))
	for _, m := range members {
		am := admin.Member{Name: m.Name, Role: string(m.Role), CaughtUp: m.CaughtUp}
		if s.leases != nil {
			am.Alive = s.leases.Table().Live(m.Name)
		}
		out = append(out, am)
	}
	return out
}
