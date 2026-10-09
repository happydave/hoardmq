package ratelimit

import (
	"math"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

// take takes n one at a time and counts how many were admitted.
func take(b *Bucket, now time.Time, n int) int {
	got := 0
	for i := 0; i < n; i++ {
		var ok bool
		if *b, ok = b.Take(now, 1); ok {
			got++
		}
	}
	return got
}

func TestBucket(t *testing.T) {
	b := New(10, 20, t0)
	if got := take(&b, t0, 21); got != 20 {
		t.Fatalf("a full bucket of burst 20 admitted %d of 21 at one instant", got)
	}
	// 100 ms at 10 a second is one token: one more, then a refusal.
	if got := take(&b, t0.Add(100*time.Millisecond), 2); got != 1 {
		t.Fatalf("after 100 ms at rate 10 admitted %d of 2, want 1", got)
	}
	if got := b.Tokens(t0.Add(10 * time.Second)); got != 20 {
		t.Fatalf("after 10 s at rate 10 the bucket holds %v, want the burst 20", got)
	}
	// A time earlier than the last adds nothing and removes nothing.
	b = New(10, 20, t0)
	b, _ = b.Take(t0, 5)
	if got := b.Tokens(t0.Add(-time.Hour)); got != 15 {
		t.Fatalf("a step back left %v tokens, want 15", got)
	}
	if got := b.Tokens(t0.Add(100 * time.Millisecond)); got != 16 {
		t.Fatalf("100 ms after the last time the bucket holds %v, want 16", got)
	}
	u := New(0, 0, t0)
	if got := take(&u, t0, 10000); got != 10000 {
		t.Fatalf("a zero-rate bucket admitted %d of 10000, want every one", got)
	}
}

func TestAdmitBoth(t *testing.T) {
	empty := func() Bucket { b := New(1, 1, t0); b, _ = b.Take(t0, 1); return b }
	full := func() Bucket { return New(1, 1, t0) }

	l, _, v := Admit(full(), empty(), t0)
	if v != RefusedNode || v.Limit() != "node" {
		t.Fatalf("listener full, node empty: %v", v)
	}
	if l.Tokens(t0) != 1 {
		t.Fatalf("a refusal by the node took the listener's token: it holds %v", l.Tokens(t0))
	}
	_, n, v := Admit(empty(), full(), t0)
	if v != RefusedListener || v.Limit() != "listener" {
		t.Fatalf("listener empty, node full: %v", v)
	}
	if n.Tokens(t0) != 1 {
		t.Fatalf("a refusal by the listener took the node's token: it holds %v", n.Tokens(t0))
	}
	l, n, v = Admit(full(), full(), t0)
	if v != Admitted || v.Limit() != "" || l.Tokens(t0) != 0 || n.Tokens(t0) != 0 {
		t.Fatalf("both full: %v, listener %v, node %v; want admitted and both empty", v, l.Tokens(t0), n.Tokens(t0))
	}
	// An unlimited listener leaves the node to decide.
	u := New(0, 0, t0)
	node := New(1, 2, t0)
	admitted := 0
	for i := 0; i < 5; i++ {
		var v Verdict
		u, node, v = Admit(u, node, t0)
		if v == Admitted {
			admitted++
		} else if v != RefusedNode {
			t.Fatalf("an unlimited listener refused: %v", v)
		}
	}
	if admitted != 2 {
		t.Fatalf("an unlimited listener over a node of burst 2 admitted %d of 5, want 2", admitted)
	}
}

func TestDelay(t *testing.T) {
	// Burst 8 at rate 4, so a delay computed from the burst answers otherwise.
	b := New(4, 8, t0)
	if d := b.Delay(t0, 1); d != 0 {
		t.Fatalf("a full bucket's delay is %v, want 0", d)
	}
	b, _ = b.Take(t0, 8)
	if d := b.Delay(t0, 1); d != 250*time.Millisecond {
		t.Fatalf("one token at rate 4 from empty: %v, want 250ms", d)
	}
	if d := b.Delay(t0, 4); d != time.Second {
		t.Fatalf("four tokens at rate 4 from empty: %v, want 1s", d)
	}
	// Above the burst: the time the rate takes to cover it.
	if d := b.Delay(t0, 10); d != 2500*time.Millisecond {
		t.Fatalf("ten tokens at rate 4 from empty: %v, want 2.5s", d)
	}
	if d := New(0, 0, t0).Delay(t0, math.MaxFloat64); d != 0 {
		t.Fatalf("an unlimited bucket's delay is %v, want 0", d)
	}
}

// A refused take takes nothing: the half token it found is still there to
// complete the next one.
func TestRefusedTakeKeepsWhatItFound(t *testing.T) {
	b := New(10, 1, t0)
	b, _ = b.Take(t0, 1)
	b, ok := b.Take(t0.Add(50*time.Millisecond), 1)
	if ok {
		t.Fatal("half a token admitted a take of one")
	}
	if _, ok := b.Take(t0.Add(100*time.Millisecond), 1); !ok {
		t.Fatal("the refused take at 50 ms lost the half token it found: a take at 100 ms was refused")
	}
}

// A charge goes into debt rather than refusing; the debt is repaid at the
// rate, and a time before the last adds nothing.
func TestCharge(t *testing.T) {
	b := New(10, 10, t0).Charge(t0, 25)
	if d := b.Delay(t0, 0); d != 1500*time.Millisecond {
		t.Fatalf("25 charged against 10 at rate 10: debt repaid in %v, want 1.5s", d)
	}
	if d := b.Delay(t0.Add(1500*time.Millisecond), 0); d != 0 {
		t.Fatalf("after 1.5 s the debt remains: %v", d)
	}
	if d := b.Charge(t0.Add(-time.Second), 0).Delay(t0, 0); d != 1500*time.Millisecond {
		t.Fatalf("a charge at an earlier time changed the debt: %v", d)
	}
	if u := New(0, 0, t0).Charge(t0, 1e9); u.Delay(t0, 0) != 0 {
		t.Fatal("an unlimited bucket went into debt")
	}
}
