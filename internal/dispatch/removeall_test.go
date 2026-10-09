package dispatch

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/session"
	"github.com/happydave/hoardmq/internal/topic"
)

// removeByWalk is RemoveAll as it was before WI 1484: search the namespace's
// trie for every filter the session holds, then remove each. It is the
// reference the filter-list removal is compared against.
func removeByWalk(r *Router, s *session.Session) {
	r.mu.Lock()
	root := r.root(s.Namespace(), false)
	var filters []string
	if root != nil {
		var walk func(n *node, prefix string)
		walk = func(n *node, prefix string) {
			if _, ok := n.subs[s]; ok {
				filters = append(filters, prefix)
			}
			for name, g := range n.groups {
				if _, ok := g.members[s]; ok {
					filters = append(filters, topic.SharePrefix+name+"/"+prefix)
				}
			}
			for level, child := range n.children {
				p := level
				if n != root {
					p = prefix + "/" + level
				}
				walk(child, p)
			}
		}
		walk(root, "")
	}
	r.mu.Unlock()
	for _, f := range filters {
		r.Remove(s, f)
	}
}

// describe renders a router's trie as sorted lines naming every direct
// subscriber, group member and sticky member, by client identifier.
func describe(r *Router) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for ns, root := range r.roots {
		var walk func(n *node, path string)
		walk = func(n *node, path string) {
			for s, sub := range n.subs {
				out = append(out, fmt.Sprintf("%s|%s|sub|%s|q%d", ns, path, s.ClientID(), sub.QoS))
			}
			for name, g := range n.groups {
				for s := range g.members {
					out = append(out, fmt.Sprintf("%s|%s|group %s|%s", ns, path, name, s.ClientID()))
				}
				if g.sticky != nil {
					out = append(out, fmt.Sprintf("%s|%s|group %s|sticky %s", ns, path, name, g.sticky.ClientID()))
				}
			}
			for level, child := range n.children {
				walk(child, path+"/"+level)
			}
			if n != root && n.empty() {
				out = append(out, fmt.Sprintf("%s|%s|empty node", ns, path))
			}
		}
		walk(root, "")
	}
	sort.Strings(out)
	return out
}

// TestRemoveAllMatchesTheWalk is WI 1484: removing a session by the filters
// it holds leaves the trie, and tells the observer, exactly what the walk
// did, across direct filters with wildcards and empty levels and shared
// groups with sticky members.
func TestRemoveAllMatchesTheWalk(t *testing.T) {
	filters := []string{"a", "a/b", "a/+", "+/b", "a/#", "#", "/", "/x", "a//b", "c/d/e", "c/+/e", "$SYS/x"}
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // seeded test
		h := newHarness(t, Sticky)
		ref := h.r
		got := New(Sticky)
		held := map[*session.Session][]string{}
		var sessions []*session.Session
		for i := 0; i < 12; i++ {
			id := fmt.Sprintf("c%02d", i)
			c := h.connect(id, true, 0)
			var subs []string
			for _, j := range rng.Perm(len(filters))[:1+rng.Intn(4)] {
				subs = append(subs, filters[j])
			}
			if rng.Intn(2) == 0 {
				subs = append(subs, fmt.Sprintf("$share/g%d/%s", rng.Intn(3), filters[rng.Intn(len(filters))]))
			}
			for _, f := range subs {
				h.subscribe(c, 0, sub(f, byte(rng.Intn(2))))
			}
			s := sessionFor(t, ref, id)
			sessions = append(sessions, s)
			held[s] = subs
			for _, f := range subs {
				got.Add(s, ref.subscriptionOf(s, f))
			}
		}
		// Make each group's first member sticky in both routers.
		stick(ref)
		stick(got)
		if a, b := describe(ref), describe(got); strings.Join(a, "\n") != strings.Join(b, "\n") {
			t.Fatalf("seed %d: the routers differ before any removal", seed)
		}
		var refCalls, gotCalls []string
		ref.SetObserver(func(ns, f string, n int) { refCalls = append(refCalls, fmt.Sprintf("%s|%s|%d", ns, f, n)) })
		got.SetObserver(func(ns, f string, n int) { gotCalls = append(gotCalls, fmt.Sprintf("%s|%s|%d", ns, f, n)) })
		for _, i := range rng.Perm(len(sessions))[:6] {
			s := sessions[i]
			refCalls, gotCalls = refCalls[:0], gotCalls[:0]
			removeByWalk(ref, s)
			got.RemoveAll(s, uniq(held[s]))
			a, b := describe(ref), describe(got)
			if strings.Join(a, "\n") != strings.Join(b, "\n") {
				t.Fatalf("seed %d, removing %s: tries differ\nwalk:\n%s\nfilters:\n%s", seed, s.ClientID(), strings.Join(a, "\n"), strings.Join(b, "\n"))
			}
			for _, line := range b {
				if strings.Contains(line, "|"+s.ClientID()) || strings.HasSuffix(line, "sticky "+s.ClientID()) {
					t.Fatalf("seed %d: %s still present: %s", seed, s.ClientID(), line)
				}
			}
			sort.Strings(refCalls)
			sort.Strings(gotCalls)
			if strings.Join(refCalls, ",") != strings.Join(gotCalls, ",") {
				t.Fatalf("seed %d, removing %s: notifications differ\nwalk: %v\nfilters: %v", seed, s.ClientID(), refCalls, gotCalls)
			}
		}
	}
}

// TestRemoveAllScenarioIsNotVacuous pins that the scenario above contains
// shared groups with sticky members and produces notifications on removal.
func TestRemoveAllScenarioIsNotVacuous(t *testing.T) {
	h := newHarness(t, Sticky)
	a := h.connect("a", true, 0)
	b := h.connect("b", true, 0)
	h.subscribe(a, 0, sub("x/+", 0), sub("$share/g/x/y", 1))
	h.subscribe(b, 0, sub("$share/g/x/y", 1))
	stick(h.r)
	before := strings.Join(describe(h.r), "\n")
	if !strings.Contains(before, "group g|sticky a") && !strings.Contains(before, "group g|sticky b") {
		t.Fatalf("no sticky member set:\n%s", before)
	}
	var calls []string
	h.r.SetObserver(func(ns, f string, n int) { calls = append(calls, fmt.Sprintf("%s|%d", f, n)) })
	sa := sessionFor(t, h.r, "a")
	h.r.RemoveAll(sa, []string{"x/+", "$share/g/x/y"})
	sort.Strings(calls)
	if strings.Join(calls, ",") != "$share/g/x/y|1,x/+|0" {
		t.Fatalf("notifications: %v", calls)
	}
	if after := strings.Join(describe(h.r), "\n"); strings.Contains(after, "|a") || strings.Contains(after, "sticky a") {
		t.Fatalf("a still present:\n%s", after)
	}
}

func sessionFor(t *testing.T, r *Router, id string) *session.Session {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var found *session.Session
	var walk func(n *node)
	walk = func(n *node) {
		for s := range n.subs {
			if s.ClientID() == id {
				found = s
			}
		}
		for _, g := range n.groups {
			for s := range g.members {
				if s.ClientID() == id {
					found = s
				}
			}
		}
		for _, c := range n.children {
			walk(c)
		}
	}
	for _, root := range r.roots {
		walk(root)
	}
	if found == nil {
		t.Fatalf("session %s not in the router", id)
	}
	return found
}

// subscriptionOf returns the subscription a session holds for a filter.
func (r *Router) subscriptionOf(s *session.Session, filter string) session.Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	share, inner := topic.SplitShared(filter)
	n := r.root(s.Namespace(), false).walk(inner, false)
	if share == "" {
		return n.subs[s]
	}
	return n.groups[share].members[s]
}

// stick makes the first member, by client identifier, of every group sticky.
func stick(r *Router) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var walk func(n *node)
	walk = func(n *node) {
		for _, g := range n.groups {
			if m := g.ordered(); len(m) > 0 {
				g.sticky = m[0]
			}
		}
		for _, c := range n.children {
			walk(c)
		}
	}
	for _, root := range r.roots {
		walk(root)
	}
}

func uniq(fs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, f := range fs {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	return out
}

func benchmarkRemoveAll(b *testing.B, others int, walk bool) {
	h := newHarness(&testing.T{}, RoundRobin)
	for i := 0; i < others; i++ {
		c := h.connect(fmt.Sprintf("o%06d", i), true, 0)
		h.subscribe(c, 0, sub(fmt.Sprintf("t/%d", i), 0))
	}
	// The victims are created and found before the clock starts, so the
	// timed loop is removal alone.
	for i := 0; i < b.N; i++ {
		c := h.connect(fmt.Sprintf("v%06d", i), true, 0)
		h.subscribe(c, 0, sub(fmt.Sprintf("v/%d", i), 0))
	}
	byID := map[string]*session.Session{}
	h.r.mu.Lock()
	var find func(n *node)
	find = func(n *node) {
		for s := range n.subs {
			byID[s.ClientID()] = s
		}
		for _, c := range n.children {
			find(c)
		}
	}
	for _, root := range h.r.roots {
		find(root)
	}
	h.r.mu.Unlock()
	victims := make([]*session.Session, b.N)
	for i := range victims {
		victims[i] = byID[fmt.Sprintf("v%06d", i)]
	}
	b.ResetTimer()
	for i, s := range victims {
		if walk {
			removeByWalk(h.r, s)
		} else {
			h.r.RemoveAll(s, []string{fmt.Sprintf("v/%d", i)})
		}
	}
}

func BenchmarkRemoveAll1k(b *testing.B)   { benchmarkRemoveAll(b, 1000, false) }
func BenchmarkRemoveAll100k(b *testing.B) { benchmarkRemoveAll(b, 100000, false) }
