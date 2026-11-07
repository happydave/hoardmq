package dispatch

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/store"
	"github.com/happydave/hoardmq/internal/topic"
)

// scanMatching is the lookup the index replaced: every topic name sorted,
// each tested with topic.Match. It is the reference for what a lookup
// returns and in what order.
func scanMatching(topics map[string]*message.Message, filter string) []string {
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []string
	for _, name := range names {
		if topic.Match(filter, name) {
			out = append(out, name)
		}
	}
	return out
}

// TestIndexedLookupMatchesTheFullScan: over a random store with empty
// levels, $-topics and names whose byte order differs from their level
// order (a-b sorts before a/b), every filter returns exactly what the full
// scan returned, in the same order, through writes and deletes.
func TestIndexedLookupMatchesTheFullScan(t *testing.T) {
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // seeded
	alphabet := []string{"a", "b", "c", "a-b", "", "$SYS", "x.y"}
	randomTopic := func() string {
		levels := make([]string, 1+rng.Intn(4))
		for i := range levels {
			levels[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return strings.Join(levels, "/")
	}
	filters := []string{"#", "+", "+/+", "a/#", "a/+/c", "+/b/#", "$SYS/#", "a//+", "+/#", "a-b/#", "/#", "a/b/c/#"}
	r := New(RoundRobin)
	model := map[string]*message.Message{}
	now := time.Now()
	for round := 0; round < 40; round++ {
		for k := 0; k < 50; k++ {
			name := randomTopic()
			if name == "" {
				continue
			}
			if rng.Intn(4) == 0 {
				r.Retain(&message.Message{Namespace: "default", Topic: name, Retain: true})
				delete(model, name)
				continue
			}
			m := &message.Message{Namespace: "default", Topic: name, Payload: []byte(fmt.Sprint(round)), Retain: true}
			r.Retain(m)
			model[name] = m
		}
		for _, f := range filters {
			var got []string
			for _, m := range r.RetainedFor("default", f, now) {
				got = append(got, m.Topic)
			}
			want := scanMatching(model, f)
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("round %d, %s: got %q, want %q", round, f, got, want)
			}
		}
	}
	if got := r.RetainedCount(); got != len(model) {
		t.Fatalf("count %d, want %d", got, len(model))
	}
}

// TestEmptyFilterMatchesNothing: a filter that is empty, as a peer's
// malformed query may carry, matches nothing and does not fail.
func TestEmptyFilterMatchesNothing(t *testing.T) {
	r := New(RoundRobin)
	r.Retain(&message.Message{Namespace: "default", Topic: "a", Payload: []byte("x"), Retain: true})
	if got := r.RetainedFor("default", "", time.Now()); len(got) != 0 {
		t.Fatalf("an empty filter matched %v", got)
	}
}

// TestLookupsRunAlongsideEachOther: two wildcard lookups are inside their
// walks at once, which the store's exclusive lock would not allow, and a
// write waits for them only until they leave the walk.
func TestLookupsRunAlongsideEachOther(t *testing.T) {
	r := New(RoundRobin)
	for i := 0; i < 1000; i++ {
		r.Retain(&message.Message{Namespace: "default", Topic: fmt.Sprintf("t/%d", i), Payload: []byte("x"), Retain: true})
	}
	var inside sync.WaitGroup
	inside.Add(2)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	walkHook = func() {
		inside.Done()
		<-release
	}
	t.Cleanup(func() { walkHook = nil })
	t.Cleanup(unblock)
	done := make(chan int, 2)
	for k := 0; k < 2; k++ {
		go func() { done <- len(r.RetainedFor("default", "t/#", time.Now())) }()
	}
	both := make(chan struct{})
	go func() {
		inside.Wait()
		close(both)
	}()
	select {
	case <-both:
	case <-time.After(2 * time.Second):
		t.Fatal("the second lookup did not enter its walk while the first was in its own")
	}
	wrote := make(chan struct{})
	go func() {
		r.Retain(&message.Message{Namespace: "default", Topic: "other", Payload: []byte("y"), Retain: true})
		close(wrote)
	}()
	unblock()
	for k := 0; k < 2; k++ {
		if got := <-done; got != 1000 && got != 1001 {
			t.Fatalf("a held lookup returned %d", got)
		}
	}
	select {
	case <-wrote:
	case <-time.After(2 * time.Second):
		t.Fatal("the write did not complete once the walks ended")
	}
}

// TestExpiredRetainedIsLeftOutThenRemoved: a lookup, wildcard or exact,
// answers without an expired message and the store drops it; a message
// replaced by a live one is kept.
func TestExpiredRetainedIsLeftOutThenRemoved(t *testing.T) {
	r := New(RoundRobin)
	now := time.Unix(1_700_000_000, 0)
	old := now.Add(-time.Second)
	r.Retain(&message.Message{Namespace: "default", Topic: "e/1", Payload: []byte("x"), Retain: true, Expiry: old})
	r.Retain(&message.Message{Namespace: "default", Topic: "e/2", Payload: []byte("x"), Retain: true, Expiry: old})
	r.Retain(&message.Message{Namespace: "default", Topic: "e/3", Payload: []byte("x"), Retain: true})
	if got := r.RetainedFor("default", "e/+", now); len(got) != 1 || got[0].Topic != "e/3" {
		t.Fatalf("wildcard lookup %v, want e/3 only", got)
	}
	if got := r.RetainedFor("default", "e/2", now); len(got) != 0 {
		t.Fatalf("exact lookup of an expired message %v", got)
	}
	if got := r.RetainedCount(); got != 1 {
		t.Fatalf("count after the lookups %d, want 1", got)
	}

	// A message replaced by a live one between the read and the removal.
	expired := &message.Message{Namespace: "default", Topic: "f", Payload: []byte("x"), Retain: true, Expiry: old}
	r.Retain(expired)
	out, stale := r.retained.lookup("default", "f", now)
	if len(out) != 0 || len(stale) != 1 {
		t.Fatalf("lookup of f: %d live, %d expired", len(out), len(stale))
	}
	r.Retain(&message.Message{Namespace: "default", Topic: "f", Payload: []byte("live"), Retain: true})
	r.retained.dropExpired("default", stale, now)
	if got := r.RetainedFor("default", "f", now); len(got) != 1 || string(got[0].Payload) != "live" {
		t.Fatalf("the live replacement was removed: %v", got)
	}
}

// TestRetainedFits: the limit takes a delete, a replacement and anything
// under it, and refuses a new topic at it.
func TestRetainedFits(t *testing.T) {
	for _, c := range []struct {
		held, limit    int
		holds, deletes bool
		want           bool
	}{
		{5, 5, false, true, true},  // a delete
		{5, 5, true, false, true},  // a replacement
		{5, 0, false, false, true}, // no limit
		{4, 5, false, false, true}, // below
		{5, 5, false, false, false},
		{6, 5, false, false, false}, // above, after a lowered limit
	} {
		if got := retainedFits(c.held, c.limit, c.holds, c.deletes); got != c.want {
			t.Errorf("retainedFits(%d, %d, %v, %v) = %v, want %v", c.held, c.limit, c.holds, c.deletes, got, c.want)
		}
	}
}

// TestRetainUnderLimit: a limit of 2 refuses a third topic, takes a
// replacement and a delete, then a new topic once the delete made room;
// namespaces count apart.
func TestRetainUnderLimit(t *testing.T) {
	r := New(RoundRobin)
	put := func(ns, name, payload string) bool {
		return r.RetainUnder(&message.Message{Namespace: ns, Topic: name, Payload: []byte(payload), Retain: true}, 2) == store.RetainStored
	}
	if !put("default", "a", "1") || !put("default", "b", "1") {
		t.Fatal("the first two topics")
	}
	if put("default", "c", "1") {
		t.Fatal("a third topic past a limit of 2 was stored")
	}
	if !put("default", "a", "2") {
		t.Fatal("a replacement at the limit was refused")
	}
	if !put("tenant", "c", "1") {
		t.Fatal("another namespace's first topic was refused")
	}
	if !put("default", "b", "") {
		t.Fatal("a delete at the limit was refused")
	}
	if !put("default", "c", "1") {
		t.Fatal("a new topic after the delete was refused")
	}
	if got := r.RetainedCounts(); got["default"] != 2 || got["tenant"] != 1 {
		t.Fatalf("counts %v", got)
	}
	if got := r.RetainedFor("default", "#", time.Now()); len(got) != 2 || got[0].Topic != "a" || got[1].Topic != "c" {
		t.Fatalf("lookup after the sequence: %v", got)
	}
}
