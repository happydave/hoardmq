package dispatch

import (
	"hash/fnv"
	"math/rand"
	"sort"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/session"
)

// Strategy names how a shared group picks the member for each message.
type Strategy string

// Strategies. LocalFirst is round-robin on a single node; it prefers members
// on the publishing node once the cluster exists.
const (
	RoundRobin Strategy = "round-robin"
	Random     Strategy = "random"
	Sticky     Strategy = "sticky"
	Hash       Strategy = "hash"
	LocalFirst Strategy = "local-first"
)

// group is one shared subscription group: the sessions subscribed with the
// same share name and inner filter.
type group struct {
	name    string
	filter  string
	members map[*session.Session]session.Subscription
	next    int
	sticky  *session.Session
}

// ordered returns the members sorted by client identifier so every strategy
// is deterministic given the same membership.
func (g *group) ordered() []*session.Session {
	out := make([]*session.Session, 0, len(g.members))
	for s := range g.members {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID() < out[j].ClientID() })
	return out
}

// choose picks the member for a message. Offline members are skipped when
// any member is online; when none is, the strategy's choice queues it.
func (g *group) choose(strategy Strategy, msg *message.Message, exclude *session.Session, rng *rand.Rand) (*session.Session, bool) {
	all := g.ordered()
	candidates := all[:0:0]
	for _, s := range all {
		if s != exclude {
			candidates = append(candidates, s)
		}
	}
	if len(candidates) == 0 {
		return nil, false
	}
	online := candidates[:0:0]
	for _, s := range candidates {
		if s.Online() {
			online = append(online, s)
		}
	}
	pool := candidates
	if len(online) > 0 {
		pool = online
	}
	switch strategy {
	case Random:
		return pool[rng.Intn(len(pool))], true
	case Sticky:
		if g.sticky != nil {
			for _, s := range pool {
				if s == g.sticky {
					return s, true
				}
			}
		}
		g.sticky = pool[0]
		return g.sticky, true
	case Hash:
		h := fnv.New32a()
		_, _ = h.Write([]byte(msg.Publisher.ClientID))
		return pool[int(h.Sum32()%uint32(len(pool)))], true
	default: // RoundRobin and LocalFirst
		s := pool[g.next%len(pool)]
		g.next++
		return s, true
	}
}
