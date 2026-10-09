package lease

import "fmt"

// BuildLevel is the highest cluster feature level this build understands.
// Every node advertises it in the alive operation it already sends, and the
// leader raises the cluster's level to it once every member has.
//
// Level 1 is everything that shipped before the gate existed: the inventory
// in the project's upgrade-gate design amendment. Raise this constant in the
// same change that keys a new semantic to the new level, never on its own.
const BuildLevel uint64 = 1

// BuildLevel is what this node advertises: the build's constant, unless a
// test pins it to stand a node in for another build.
func (s *Service) BuildLevel() uint64 {
	if l := s.levelPin.Load(); l != 0 {
		return l
	}
	return BuildLevel
}

// raiseTo decides the level a leader proposes, if any: the lowest level a
// member advertises, once every member has a record and that level is above
// the table's. A member with no record stops the raise; it has not said what
// it understands.
func raiseTo(current, minAdvertised uint64, everyMember bool) (uint64, bool) {
	if !everyMember || minAdvertised <= current {
		return 0, false
	}
	return minAdvertised, true
}

// raiseLevel runs on the leader's tick. Members are the consensus group's,
// voters and learners alike: a learner applies the same log.
func (s *Service) raiseLevel() {
	if s.o.Core.Status().Role != "leader" || s.BelowLevel() {
		return
	}
	members := s.o.Core.Members()
	if len(members) == 0 {
		return
	}
	names := make([]string, len(members))
	for i, m := range members {
		names[i] = m.Name
	}
	lowest, every := s.table.MinAdvertisedLevel(names)
	to, ok := raiseTo(s.table.Level(), lowest, every)
	if !ok || !s.raising.CompareAndSwap(0, to) {
		// Nothing to raise, or the raise this node proposed has not come
		// back yet: the next tick is soon enough.
		return
	}
	s.submitAsync(Op{Kind: OpSetLevel, Node: s.o.Node, Level: to, Now: s.o.Clock()})
	s.audit("cluster.level", fmt.Sprint(to), "proposed", map[string]string{"by": s.o.Node, "from": fmt.Sprint(s.table.Level())})
}

// SetBuildLevel is the harness's seam: it stands a running node in for
// another build, as Options.BuildLevel does from the start. Zero restores
// the build's own level.
func (s *Service) SetBuildLevel(l uint64) { s.levelPin.Store(l) }

// BelowLevel reports whether the cluster's feature level is above what this
// build understands. Such a node applies the log — it must, to stay a member
// — but serves no client.
func (s *Service) BelowLevel() bool { return s.BuildLevel() < s.table.Level() }

// noteLevel logs and audits the moment this node falls below the cluster's
// level, and the moment it no longer is.
func (s *Service) noteLevel() {
	below := s.BelowLevel()
	if s.levelBelow.Swap(below) == below {
		return
	}
	build, cluster := fmt.Sprint(s.BuildLevel()), fmt.Sprint(s.table.Level())
	if below {
		s.log.Error("this build is below the cluster's feature level and serves no client; upgrade it", "build", build, "cluster", cluster)
		s.audit("cluster.level", s.o.Node, "below", map[string]string{"build": build, "cluster": cluster})
		return
	}
	s.log.Info("this build understands the cluster's feature level again", "build", build, "cluster", cluster)
	s.audit("cluster.level", s.o.Node, "restored", map[string]string{"build": build, "cluster": cluster})
}

// noteRaise counts a raise this node proposed by how it ended: applied,
// refused because another raise reached the log first, or failed because the
// proposal never applied. Any of them lets the next tick propose again.
func (s *Service) noteRaise(to uint64, res Result, err error) {
	s.raising.Store(0)
	outcome := "applied"
	switch {
	case err != nil:
		outcome, res.Reason = "failed", err.Error()
	case !res.OK:
		outcome = "refused"
	}
	s.count("hoardmq_cluster_feature_level_raises_total", map[string]string{"outcome": outcome}, 1)
	s.audit("cluster.level", fmt.Sprint(to), outcome, map[string]string{"by": s.o.Node, "reason": res.Reason})
}

// Levels is the cluster's feature level, this build's, and the lowest any
// consensus member advertises — zero while a member has no record, which is
// what holds a raise back.
func (s *Service) Levels() (cluster, build, minAdvertised uint64) {
	members := s.o.Core.Members()
	names := make([]string, len(members))
	for i, m := range members {
		names[i] = m.Name
	}
	lowest, every := s.table.MinAdvertisedLevel(names)
	if !every {
		lowest = 0
	}
	return s.table.Level(), s.BuildLevel(), lowest
}
