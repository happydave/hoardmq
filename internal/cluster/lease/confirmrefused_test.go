package lease

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

// refusedErr is a failed durability wait naming the keys a replica refused,
// as the replication barrier's not-held failure does.
type refusedErr []store.Scope

func (e refusedErr) Error() string                { return "a replica refused a record" }
func (e refusedErr) RefusedScopes() []store.Scope { return e }

func TestConfirmOwed(t *testing.T) {
	for _, c := range []struct {
		name   string
		heldAt uint64
		held   bool
		known  bool
		owner  string
		epoch  uint64
		want   bool
	}{
		{"held at the token, the table names this node", 7, true, true, "n1", 7, true},
		{"held above the token (the client came back here)", 9, true, true, "n1", 9, true},
		{"not held here", 0, false, true, "n1", 7, false},
		{"not held here, whatever token is read beside it", 7, false, true, "n1", 7, false},
		{"held at a lower token", 6, true, true, "n1", 6, false},
		{"held, the table names another owner at a later grant", 7, true, true, "n2", 8, false},
		{"held, the table names another owner at the job's grant: lag", 7, true, true, "n2", 7, true},
		{"held, the table does not know the client", 7, true, false, "", 0, true},
		{"held, the table lags at an earlier grant", 7, true, true, "n2", 5, true},
	} {
		if got := confirmOwed(c.heldAt, c.held, 7, c.known, c.owner, "n1", c.epoch); got != c.want {
			t.Errorf("%s: owed %v, want %v", c.name, got, c.want)
		}
	}
}

// TestRefusedKeysLeaveTheSharedWait: when a shared wait fails naming refused
// keys, those jobs are judged again — one still held here waits again later,
// one whose session has left is dropped — and the rest share the next wait.
// No job is asked about alone.
func TestRefusedKeysLeaveTheSharedWait(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	c.settled()
	a := c.nodes["n1"]
	ids := []string{"a", "b", "c", "d"}
	for _, id := range append([]string{"z"}, ids...) {
		st := sessionFor(id)
		st.Epoch = 7
		a.sessions.put("default", id, st)
	}
	scope := func(id string) store.Scope { return store.Scope{Namespace: "default", ClientID: id} }
	// z's wait holds the queue's worker until a, b, c and d are queued, so
	// they share one round.
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var mu sync.Mutex
	refusedA, refusedC := false, false
	a.sessions.mu.Lock()
	a.sessions.durableFn = func(scopes []store.Scope) error {
		if slices.Contains(scopes, scope("z")) {
			<-release
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case !refusedA && len(scopes) > 1:
			refusedA = true
			return refusedErr{scope("a")}
		case !refusedC && slices.Contains(scopes, scope("c")):
			refusedC = true
			// c's session leaves this node before its refusal is read.
			a.sessions.mu.Lock()
			delete(a.sessions.states, "default/c")
			a.sessions.mu.Unlock()
			return refusedErr{scope("c")}
		}
		return nil
	}
	a.sessions.mu.Unlock()
	before := a.svc.LivenessStats()
	a.svc.ConfirmMoved("default", "z", []string{"n2"})
	waitFor(t, "z's wait to begin", 5*time.Second, func() bool {
		a.sessions.mu.Lock()
		defer a.sessions.mu.Unlock()
		return len(a.sessions.durableCalls) >= 1
	})
	for _, id := range ids {
		a.svc.ConfirmMoved("default", id, []string{"n2"})
	}
	unblock()
	// b and d go in the round that judged c, so c is already given up then,
	// not on a later retry.
	waitFor(t, "b's and d's confirmations", 5*time.Second, func() bool {
		return a.svc.LivenessStats().ConfirmsSentMoved-before.ConfirmsSentMoved >= 3
	})
	if n := a.svc.LivenessStats().ConfirmsUnheld - before.ConfirmsUnheld; n != 1 {
		t.Errorf("given up as unheld %d when b and d were sent, want 1 (c, in the same round)", n)
	}
	waitFor(t, "a's confirmation, after its wait again", 10*time.Second, func() bool {
		return a.svc.LivenessStats().ConfirmsSentMoved-before.ConfirmsSentMoved >= 4
	})
	time.Sleep(300 * time.Millisecond)
	st := a.svc.LivenessStats()
	if sent := st.ConfirmsSentMoved - before.ConfirmsSentMoved; sent != 4 {
		t.Errorf("confirmations sent %d, want 4 (z, a, b, d)", sent)
	}
	if n := st.ConfirmsUnheld - before.ConfirmsUnheld; n != 1 {
		t.Errorf("given up as unheld %d, want 1 (c)", n)
	}
	if n := st.ConfirmsUndurable - before.ConfirmsUndurable; n != 0 {
		t.Errorf("given up as undurable %d, want 0", n)
	}
	a.sessions.mu.Lock()
	got := a.sessions.durableCalls
	a.sessions.mu.Unlock()
	got = got[1:] // z's
	var sizes []int
	for _, call := range got {
		sizes = append(sizes, len(call))
	}
	if len(sizes) < 2 || sizes[len(sizes)-1] != 1 || !slices.Equal(got[len(got)-1], []store.Scope{scope("a")}) {
		t.Fatalf("waits %v: want the last to be a's, alone, after its requeue", got)
	}
	if want := []int{4, 3, 2, 1}; !slices.Equal(sizes, want) {
		t.Errorf("waits over %v keys, want %v (a shared wait, less each refused key, then a's requeue)", sizes, want)
	}
}

// TestOtherFailuresMakeAllWait: a wait that fails naming no refused key fails
// every key alike, so every job waits and is asked again together.
func TestOtherFailuresMakeAllWait(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	c.settled()
	a := c.nodes["n1"]
	for _, id := range []string{"z", "a", "b"} {
		st := sessionFor(id)
		st.Epoch = 7
		a.sessions.put("default", id, st)
	}
	z := store.Scope{Namespace: "default", ClientID: "z"}
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var mu sync.Mutex
	failed := false
	a.sessions.mu.Lock()
	a.sessions.durableFn = func(scopes []store.Scope) error {
		if slices.Contains(scopes, z) {
			<-release
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if !failed {
			failed = true
			return errors.New("replication: frames were dropped; awaiting resync")
		}
		return nil
	}
	a.sessions.mu.Unlock()
	before := a.svc.LivenessStats().ConfirmsSentMoved
	a.svc.ConfirmMoved("default", "z", []string{"n2"})
	waitFor(t, "z's wait to begin", 5*time.Second, func() bool {
		a.sessions.mu.Lock()
		defer a.sessions.mu.Unlock()
		return len(a.sessions.durableCalls) >= 1
	})
	a.svc.ConfirmMoved("default", "a", []string{"n2"})
	a.svc.ConfirmMoved("default", "b", []string{"n2"})
	unblock()
	waitFor(t, "every confirmation", 10*time.Second, func() bool { return a.svc.LivenessStats().ConfirmsSentMoved-before >= 3 })
	a.sessions.mu.Lock()
	got := a.sessions.durableCalls[1:]
	a.sessions.mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("waits %v after z's, want two: the failed one and its repeat", got)
	}
	for i, call := range got {
		if len(call) != 2 {
			t.Errorf("wait %d over %d keys, want every wait over both: %v", i+1, len(call), got)
		}
	}
}

// TestUnmatchedRefusedKeyMakesAllWait: a failed wait naming only a key no job
// carries is treated as any other failure: every job waits, and the queue
// does not ask again at once.
func TestUnmatchedRefusedKeyMakesAllWait(t *testing.T) {
	c := newCluster(t, []string{"n1", "n2"}, 2*time.Second, nil)
	c.settled()
	a := c.nodes["n1"]
	st := sessionFor("a")
	st.Epoch = 7
	a.sessions.put("default", "a", st)
	var mu sync.Mutex
	failed := false
	a.sessions.mu.Lock()
	a.sessions.durableFn = func([]store.Scope) error {
		mu.Lock()
		defer mu.Unlock()
		if !failed {
			failed = true
			return refusedErr{{Namespace: "default", ClientID: "elsewhere"}}
		}
		return nil
	}
	a.sessions.mu.Unlock()
	before := a.svc.LivenessStats()
	a.svc.ConfirmMoved("default", "a", []string{"n2"})
	waitFor(t, "the first wait", 5*time.Second, func() bool {
		a.sessions.mu.Lock()
		defer a.sessions.mu.Unlock()
		return len(a.sessions.durableCalls) >= 1
	})
	time.Sleep(time.Second)
	a.sessions.mu.Lock()
	n := len(a.sessions.durableCalls)
	a.sessions.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d waits within a second of a failure naming no job's key, want 1 (the job waits its interval)", n)
	}
	waitFor(t, "the confirmation after the interval", 10*time.Second, func() bool {
		return a.svc.LivenessStats().ConfirmsSentMoved-before.ConfirmsSentMoved >= 1
	})
	if d := a.svc.LivenessStats().ConfirmsUnheld - before.ConfirmsUnheld; d != 0 {
		t.Errorf("given up as unheld %d, want 0", d)
	}
}
