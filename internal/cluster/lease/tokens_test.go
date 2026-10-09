package lease

import (
	"sync"
	"testing"
)

func TestTokensKeepTheHighestSeenFromAnySource(t *testing.T) {
	tk := NewTokens()
	if got := tk.Seen("default", "c"); got != 0 {
		t.Fatalf("nothing seen yet: %d", got)
	}
	for _, token := range []uint64{7, 40, 12, 0, 40, 39} {
		tk.Note("default", "c", token)
	}
	if got := tk.Seen("default", "c"); got != 40 {
		t.Fatalf("highest of 7, 40, 12, 0, 40, 39: %d", got)
	}
	if got := tk.Seen("other", "c"); got != 0 {
		t.Fatalf("a client of the same name in another namespace: %d", got)
	}
	// An old holder's release does not forget what a newer grant taught.
	tk.released("default", "c", 12)
	if got := tk.Seen("default", "c"); got != 40 {
		t.Fatalf("after a release at 12: %d, want 40 kept", got)
	}
	tk.released("default", "c", 40)
	if got, n := tk.Seen("default", "c"), tk.Len(); got != 0 || n != 0 {
		t.Fatalf("after the release at 40: seen %d, %d entries", got, n)
	}
	var nilTokens *Tokens
	nilTokens.Note("default", "c", 9)
	if got := nilTokens.Seen("default", "c"); got != 0 {
		t.Fatalf("a nil registry: %d", got)
	}
}

func TestTokensUnderContention(t *testing.T) {
	tk := NewTokens()
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 1; i <= 500; i++ {
				tk.Note("default", "c", uint64(g*1000+i))
				_ = tk.Seen("default", "c")
			}
		}(g)
	}
	wg.Wait()
	if got := tk.Seen("default", "c"); got != 31*1000+500 {
		t.Fatalf("highest after concurrent notes: %d", got)
	}
}
