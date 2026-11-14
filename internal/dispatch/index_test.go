package dispatch

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/packet"
	"github.com/happydave/hoardmq/internal/session"
)

// sessionObjects connects n clients through h's manager and returns their
// session objects, each holding no subscription.
func sessionObjects(t testing.TB, h *harness, prefix string, n int) []*session.Session {
	t.Helper()
	out := make([]*session.Session, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		c := h.connect(id, true, 0)
		h.subscribe(c, 0, sub("who/"+id, 0))
		ms := h.r.Match(&message.Message{Namespace: "default", Topic: "who/" + id}, nil)
		if len(ms) != 1 {
			t.Fatalf("who/%s matched %d, want 1", id, len(ms))
		}
		out = append(out, ms[0].Session)
		h.m.Unsubscribe(c, &packet.Unsubscribe{PacketID: 1, Filters: []string{"who/" + id}})
	}
	return out
}

// walkAll walks r from cursor 0 to done and returns each filter's
// subscribers by client, and the steps taken.
func walkAll(t *testing.T, r *Router) (map[string][]string, [][]session.RoutedFilter) {
	t.Helper()
	got := map[string][]string{}
	var steps [][]session.RoutedFilter
	for cursor, done := 0, false; !done; {
		var fs []session.RoutedFilter
		fs, cursor, done, _ = r.WalkStep(cursor)
		steps = append(steps, fs)
		for _, f := range fs {
			k := f.Namespace + " " + f.Filter
			if _, dup := got[k]; dup {
				t.Fatalf("the walk returned %s twice", k)
			}
			ids := make([]string, 0, len(f.Subs))
			for _, s := range f.Subs {
				ids = append(ids, s.ClientID())
			}
			sort.Strings(ids)
			got[k] = ids
			if f.Group != strings.HasPrefix(f.Filter, "$share/") {
				t.Fatalf("%s returned with Group %v", f.Filter, f.Group)
			}
		}
	}
	return got, steps
}

// trieHolding counts the trie nodes holding a subscriber or a group.
func trieHolding(r *Router) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	var rec func(*node)
	rec = func(x *node) {
		if x.holding() {
			n++
		}
		for _, c := range x.children {
			rec(c)
		}
	}
	for _, root := range r.roots {
		rec(root)
	}
	return n
}

func TestTheWalkReturnsEachHeldFilterOnceWithItsSubscribers(t *testing.T) {
	h := newHarness(t, RoundRobin)
	ss := sessionObjects(t, h, "w", 3)
	a, b, c := ss[0], ss[1], ss[2]
	add := func(s *session.Session, f string) { h.r.Add(s, session.Subscription{Filter: f}) }
	add(a, "x/1")
	add(b, "x/1")
	add(a, "x/2")
	add(a, "$share/g/x/1") // a group on a node that also holds direct subscribers
	add(b, "$share/g/x/1")
	add(c, "$share/h/y/+")
	add(c, "gone/1")
	add(b, "x/2/deeper")
	h.r.Remove(c, "gone/1")
	h.r.Remove(a, "x/2")

	got, _ := walkAll(t, h.r)
	want := map[string][]string{
		"default x/1":          {"w-0", "w-1"},
		"default $share/g/x/1": {"w-0", "w-1"},
		"default $share/h/y/+": {"w-2"},
		"default x/2/deeper":   {"w-1"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("the walk returned %v, want %v", got, want)
	}
	if n, nodes := h.r.Indexed(), trieHolding(h.r); n != nodes {
		t.Fatalf("the index holds %d nodes, the trie %d holding a subscriber or a group", n, nodes)
	}
	// Emptied: the index forgets every node.
	for _, rm := range []struct {
		s *session.Session
		f string
	}{{a, "x/1"}, {b, "x/1"}, {a, "$share/g/x/1"}, {b, "$share/g/x/1"}, {c, "$share/h/y/+"}, {b, "x/2/deeper"}} {
		h.r.Remove(rm.s, rm.f)
	}
	if n := h.r.Indexed(); n != 0 {
		t.Fatalf("emptied, the index holds %d nodes, want 0", n)
	}
	if got, _ := walkAll(t, h.r); len(got) != 0 {
		t.Fatalf("emptied, the walk returned %v", got)
	}
}

func TestAWalkStepIsBounded(t *testing.T) {
	h := newHarness(t, RoundRobin)
	ss := sessionObjects(t, h, "b", 5000)
	for i := 0; i < 10000; i++ {
		h.r.Add(ss[i%len(ss)], session.Subscription{Filter: fmt.Sprintf("many/%d", i)})
	}
	got, steps := walkAll(t, h.r)
	if len(got) != 10000 {
		t.Fatalf("the walk returned %d filters, want 10000", len(got))
	}
	for i, st := range steps {
		if len(st) > walkStepFilters {
			t.Fatalf("step %d copied %d filters, more than %d", i, len(st), walkStepFilters)
		}
	}
	if len(steps) < 3 {
		t.Fatalf("10000 filters walked in %d steps, want at least 3", len(steps))
	}

	// One filter with 5,000 subscribers is a step of its own.
	for _, s := range ss {
		h.r.Add(s, session.Subscription{Filter: "fan/in"})
	}
	_, steps = walkAll(t, h.r)
	found := false
	for i, st := range steps {
		subs, fan := 0, false
		for _, f := range st {
			subs += len(f.Subs)
			fan = fan || f.Filter == "fan/in"
		}
		if !fan {
			if subs > walkStepSubs {
				t.Fatalf("step %d copied %d subscribers without the fan-in filter, more than %d", i, subs, walkStepSubs)
			}
			continue
		}
		found = true
		if len(st) != 1 {
			t.Fatalf("the fan-in filter's step also copied %d other filters", len(st)-1)
		}
	}
	if !found {
		t.Fatal("no step returned the fan-in filter")
	}
}

func TestAWalkReturnsEveryFilterPresentThroughoutDespiteChanges(t *testing.T) {
	h := newHarness(t, RoundRobin)
	ss := sessionObjects(t, h, "c", 10)
	// The filters that go are first in the index and those that stay last, so
	// a removal behind the walk moves one that stays.
	for i := 0; i < 3000; i++ {
		h.r.Add(ss[i%10], session.Subscription{Filter: fmt.Sprintf("churn/%d", i)})
	}
	for i := 0; i < 9000; i++ {
		h.r.Add(ss[i%10], session.Subscription{Filter: fmt.Sprintf("stay/%d", i)})
	}
	seen := map[string]int{}
	k := 0
	for cursor, done, step := 0, false, 0; !done; step++ {
		var fs []session.RoutedFilter
		fs, cursor, done, _ = h.r.WalkStep(cursor)
		for _, f := range fs {
			seen[f.Filter]++
		}
		// Between steps, filters go, and after the first ones come too.
		for j := 0; j < 1000 && k < 3000; j, k = j+1, k+1 {
			h.r.Remove(ss[k%10], fmt.Sprintf("churn/%d", k))
		}
		if step > 0 {
			h.r.Add(ss[step%10], session.Subscription{Filter: fmt.Sprintf("new/%d", step)})
		}
	}
	for i := 0; i < 9000; i++ {
		if n := seen[fmt.Sprintf("stay/%d", i)]; n != 1 {
			t.Fatalf("stay/%d returned %d times, want once", i, n)
		}
	}
	for f, n := range seen {
		if n != 1 {
			t.Fatalf("%s returned %d times", f, n)
		}
	}
	if n, nodes := h.r.Indexed(), trieHolding(h.r); n != nodes {
		t.Fatalf("the index holds %d nodes, the trie %d", n, nodes)
	}
}

func TestThePointReads(t *testing.T) {
	h := newHarness(t, RoundRobin)
	ss := sessionObjects(t, h, "p", 3)
	a, b := ss[0], ss[1]
	h.r.Add(a, session.Subscription{Filter: "d/1"})
	h.r.Add(b, session.Subscription{Filter: "d/1"})
	h.r.Add(a, session.Subscription{Filter: "$share/g/d/1"})
	cases := []struct {
		name string
		got  bool
		want bool
	}{
		{"a holds d/1", h.r.Holds(a, "d/1"), true},
		{"the third client holds d/1", h.r.Holds(ss[2], "d/1"), false},
		{"a holds the group", h.r.Holds(a, "$share/g/d/1"), true},
		{"b holds the group", h.r.Holds(b, "$share/g/d/1"), false},
		{"a holds another group's name", h.r.Holds(a, "$share/x/d/1"), false},
		{"a holds an unknown filter", h.r.Holds(a, "nope"), false},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: %v, want %v", c.name, c.got, c.want)
		}
	}
	if n := h.r.SubscriberCount("default", "d/1"); n != 2 {
		t.Errorf("d/1 counts %d subscribers, want 2", n)
	}
	if n := h.r.SubscriberCount("default", "$share/g/d/1"); n != 1 {
		t.Errorf("the group counts %d members, want 1", n)
	}
	h.r.Remove(a, "d/1")
	if h.r.Holds(a, "d/1") {
		t.Error("a still holds d/1 after its removal")
	}
	if n := h.r.SubscriberCount("default", "d/1"); n != 1 {
		t.Errorf("after a's removal d/1 counts %d, want 1", n)
	}
}
