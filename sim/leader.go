package sim

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Leader placement for the failover measurement (WI 1493). At 100k, recovery
// took 34 s or 42–59 s depending on which survivor became raft leader, so a
// run records the leaders and where the displaced clients go first, and a
// victim policy can put that storm on the leader, or away from it, without an
// election.

// The victim policies HOARDMQ_SIM_VICTIM selects.
const (
	victimEnv            = "HOARDMQ_SIM_VICTIM"
	victimMostClients    = "most-clients"
	victimBeforeLeader   = "before-leader"
	victimBeforeFollower = "before-follower"
)

// profileAtEnv lists the seconds after the crash at which goroutine and mutex
// profiles are written.
const profileAtEnv = "HOARDMQ_SIM_PROFILE_AT_SEC"

// agreedLeader returns the leader every view names. A view with no leader, or
// two views naming different nodes, is no agreement.
func agreedLeader(views map[string]string) (string, bool) {
	leader := ""
	if len(views) == 0 {
		return "", false
	}
	for _, v := range views {
		if v == "" {
			return "", false
		}
		if leader == "" {
			leader = v
		} else if v != leader {
			return "", false
		}
	}
	return leader, true
}

// leaderViews reads the leader each running node reports, leaving out skip.
func leaderViews(c *Cluster, skip *Node) map[string]string {
	views := map[string]string{}
	for _, n := range c.Nodes {
		if n == skip {
			continue
		}
		srv := n.Server()
		if srv == nil || srv.Consensus() == nil {
			continue
		}
		l, _ := srv.Consensus().Leader()
		views[n.Name] = l
	}
	return views
}

// stormTarget is the node a victim's clients try next: each client's order is
// the node list rotated to start at its own node, so the victim's successor.
func stormTarget(names []string, victim string) string {
	for i, n := range names {
		if n == victim {
			return names[(i+1)%len(names)]
		}
	}
	return ""
}

// chooseVictim picks the node to crash by policy. counts are the clients per
// node in the same order as names; leader is the agreed leader, or "" when
// there is none.
func chooseVictim(policy string, names []string, counts []int, leader string) (int, error) {
	switch policy {
	case "", victimMostClients:
		victim := 0
		for i := range names {
			if counts[i] > counts[victim] {
				victim = i
			}
		}
		return victim, nil
	case victimBeforeLeader, victimBeforeFollower:
		if leader == "" {
			return 0, fmt.Errorf("%s=%s needs a leader every node agrees on, and there is none", victimEnv, policy)
		}
		for i := range names {
			if names[i] == leader {
				continue
			}
			target := stormTarget(names, names[i])
			if (policy == victimBeforeLeader) == (target == leader) {
				return i, nil
			}
		}
		return 0, fmt.Errorf("%s=%s: no follower of %s fits among %v", victimEnv, policy, leader, names)
	default:
		return 0, fmt.Errorf("%s=%q: want %s, %s or %s", victimEnv, policy, victimMostClients, victimBeforeLeader, victimBeforeFollower)
	}
}

// parseProfileAt reads a comma-separated list of whole seconds, sorted and
// without duplicates.
func parseProfileAt(v string) ([]time.Duration, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	seen := map[int]bool{}
	var secs []int
	for _, f := range strings.Split(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("%s: %q is not a positive whole number of seconds", profileAtEnv, f)
		}
		if seen[n] {
			return nil, fmt.Errorf("%s: %d s is listed twice", profileAtEnv, n)
		}
		seen[n] = true
		secs = append(secs, n)
	}
	sort.Ints(secs)
	out := make([]time.Duration, len(secs))
	for i, n := range secs {
		out[i] = time.Duration(n) * time.Second
	}
	return out, nil
}

// onlineSeriesMax bounds the per-second series at the window's length.
const onlineSeriesMax = 720

// onlineSeries keeps the online count once a second from an origin. A pass
// that arrives late fills each second it missed with the count it saw, so the
// series stays one sample a second; the loop's own stall figures say when
// that happened.
type onlineSeries struct {
	from    time.Time
	samples []int
}

func (s *onlineSeries) observe(now time.Time, online int) {
	for len(s.samples) < onlineSeriesMax && now.Sub(s.from) >= time.Duration(len(s.samples)+1)*time.Second {
		s.samples = append(s.samples, online)
	}
}

// placementSection is the report's account of leadership around the crash.
func placementSection(policy, before, after, victim, target string) string {
	if policy == "" {
		policy = victimMostClients
	}
	name := func(s string) string {
		if s == "" {
			return "none agreed"
		}
		return "`" + s + "`"
	}
	return fmt.Sprintf("\n## Leader placement\n\nVictim policy `%s`: crashed `%s`, whose clients try `%s` first. Raft leader agreed before the crash: %s; after, among the survivors at window end: %s.\n",
		policy, victim, target, name(before), name(after))
}
