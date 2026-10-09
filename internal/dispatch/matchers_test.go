package dispatch

import (
	"fmt"
	"testing"
)

// Matchers lists plain and shared subscriptions a topic reaches and leaves
// the shared group's round-robin where it was: a publish after the view goes
// to the member the one before it did not.
func TestMatchersIsAView(t *testing.T) {
	h := newHarness(t, RoundRobin)
	pub := h.connect("pub", true, 0)
	plain := h.connect("plain", true, 0)
	h.subscribe(plain, 0, sub("m/+", 1))
	m1 := h.connect("m1", true, 0)
	m2 := h.connect("m2", true, 0)
	h.subscribe(m1, 0, sub("$share/g/m/#", 0))
	h.subscribe(m2, 0, sub("$share/g/m/#", 1))
	other := h.connect("other", true, 0)
	h.subscribe(other, 0, sub("x/#", 0))

	h.publish(pub, "m/1", 0, "first", false)
	first := "m1"
	if len(m1.sent) == 0 {
		first = "m2"
	}
	if len(m1.sent)+len(m2.sent) != 1 {
		t.Fatalf("the group delivered %d copies of one publish", len(m1.sent)+len(m2.sent))
	}
	got := fmt.Sprint(h.r.Matchers("default", "m/1"))
	want := "[{m1 $share/g/m/# g 0} {m2 $share/g/m/# g 1} {plain m/+  1}]"
	if got != want {
		t.Fatalf("matchers %s, want %s", got, want)
	}
	h.publish(pub, "m/1", 0, "second", false)
	if len(m1.sent) != 1 || len(m2.sent) != 1 {
		t.Fatalf("after the view, the second publish went to %s again (m1 %d, m2 %d): the view advanced the group", first, len(m1.sent), len(m2.sent))
	}
	if n := len(h.r.Matchers("default", "nothing/here")); n != 0 {
		t.Fatalf("an unmatched topic has %d matchers", n)
	}
	if n := len(h.r.Matchers("elsewhere", "m/1")); n != 0 {
		t.Fatalf("another namespace has %d matchers", n)
	}
}
