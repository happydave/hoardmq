package dispatch

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/message"
	"github.com/happydave/hoardmq/internal/session"
)

// TestMatchesRunAlongsideEachOther: two matches are inside the router at
// once, which its exclusive lock would not allow.
func TestMatchesRunAlongsideEachOther(t *testing.T) {
	h := newHarness(t, RoundRobin)
	h.subscribe(h.connect("s", true, 0), 0, sub("t", 0))
	var inside sync.WaitGroup
	inside.Add(2)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	matchHook = func() {
		inside.Done()
		<-release
	}
	t.Cleanup(func() { matchHook = nil })
	t.Cleanup(unblock)
	done := make(chan int, 2)
	for k := 0; k < 2; k++ {
		go func() { done <- len(h.r.Match(&message.Message{Namespace: "default", Topic: "t"}, nil)) }()
	}
	both := make(chan struct{})
	go func() {
		inside.Wait()
		close(both)
	}()
	select {
	case <-both:
	case <-time.After(2 * time.Second):
		t.Fatal("the second match did not enter while the first was inside")
	}
	unblock()
	for k := 0; k < 2; k++ {
		if got := <-done; got != 1 {
			t.Fatalf("a held match returned %d matches, want 1", got)
		}
	}
}

// TestRoundRobinStaysFairUnderConcurrentMatches: 32 goroutines matching a
// topic with a four-member round-robin group give each member exactly a
// quarter of the messages.
func TestRoundRobinStaysFairUnderConcurrentMatches(t *testing.T) {
	h := newHarness(t, RoundRobin)
	for i := 0; i < 4; i++ {
		h.subscribe(h.connect(fmt.Sprintf("m%d", i), true, 0), 0, sub("$share/g/t", 0))
	}
	const goroutines, each = 32, 2000
	var mu sync.Mutex
	chosen := map[*session.Session]int{}
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := map[*session.Session]int{}
			for k := 0; k < each; k++ {
				for _, m := range h.r.Match(&message.Message{Namespace: "default", Topic: "t"}, nil) {
					local[m.Session]++
				}
			}
			mu.Lock()
			for s, n := range local {
				chosen[s] += n
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(chosen) != 4 {
		t.Fatalf("%d members chosen, want 4", len(chosen))
	}
	for s, n := range chosen {
		if n != goroutines*each/4 {
			t.Errorf("%s chosen %d times, want %d", s.ClientID(), n, goroutines*each/4)
		}
	}
}

// TestASubscribeWaitsOnlyForTheMatchesInProgress: a subscribe made while a
// match is held completes once the match ends, and the next match sees it.
func TestASubscribeWaitsOnlyForTheMatchesInProgress(t *testing.T) {
	h := newHarness(t, RoundRobin)
	h.subscribe(h.connect("s", true, 0), 0, sub("t", 0))
	late := h.connect("late", true, 0)
	entered := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var once sync.Once
	matchHook = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	t.Cleanup(func() { matchHook = nil })
	t.Cleanup(unblock)
	go h.r.Match(&message.Message{Namespace: "default", Topic: "t"}, nil)
	<-entered
	added := make(chan struct{})
	go func() {
		h.subscribe(late, 0, sub("t", 0))
		close(added)
	}()
	select {
	case <-added:
		t.Fatal("the subscribe completed while a match held the read lock")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case <-added:
	case <-time.After(2 * time.Second):
		t.Fatal("the subscribe did not complete once the match ended")
	}
	if got := len(h.r.Match(&message.Message{Namespace: "default", Topic: "t"}, nil)); got != 2 {
		t.Fatalf("the next match found %d subscribers, want 2", got)
	}
}
