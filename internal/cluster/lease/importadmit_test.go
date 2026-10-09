package lease

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/happydave/hoardmq/internal/store"
)

func TestImportAdmitted(t *testing.T) {
	for _, c := range []struct {
		seen, token uint64
		want        bool
	}{
		{0, 7, true},   // the registry forgot the client at a release
		{6, 7, true},   // seen only an earlier grant
		{7, 7, true},   // the grant the state was fetched for
		{8, 7, false},  // a later grant: the lease moved on
		{90, 7, false}, // a much later one
	} {
		if got := importAdmitted(c.seen, c.token); got != c.want {
			t.Errorf("importAdmitted(seen %d, token %d) = %v, want %v", c.seen, c.token, got, c.want)
		}
	}
}

// TestAdoptionImportRefusedAfterALaterGrant: the adopter's worker has fetched
// the session and is held before its import. A later grant for the client is
// seen meanwhile — the takeover pass notes its request's token before it
// seals. Let go, the worker installs nothing, sends no confirmation, and
// leaves the previous owner noted for the pass that answers the new owner.
// Without the later grant the same worker imports.
func TestAdoptionImportRefusedAfterALaterGrant(t *testing.T) {
	for _, later := range []bool{false, true} {
		name := "no later grant"
		if later {
			name = "later grant seen"
		}
		t.Run(name, func(t *testing.T) {
			var once sync.Once
			entered, gate := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(gate) })
			fixtureFallback = func(context.Context, string, string, string) (*store.SessionState, bool) {
				return sessionFor("x"), true
			}
			t.Cleanup(func() { fixtureFallback = nil })
			fixtureOptions = func(o *Options) {
				o.ImportGate = func(_, id string) {
					if id != "x" {
						return
					}
					once.Do(func() { close(entered) })
					<-gate
				}
			}
			t.Cleanup(func() { fixtureOptions = nil })
			c := newCluster(t, []string{"n1", "n2", "n3"}, 1500*time.Millisecond, nil)
			t.Cleanup(release)
			lead := c.settled()
			var owner *tnode
			for _, n := range c.nodes {
				if n != lead {
					owner = n
					break
				}
			}
			res, err := acquire(t, owner, "x", nil, 300, true)
			if err != nil {
				t.Fatal(err)
			}
			owner.svc.Disconnected("default", "x", res.Epoch, time.Now().Add(5*time.Minute), false)
			waitFor(t, "x offline on the leader's table", 3*time.Second, func() bool {
				l, ok := lead.svc.Table().Lookup("default", "x")
				return ok && !l.Online
			})
			c.stopNode(owner)
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("precondition not reached: the leader's worker never fetched x")
			}
			l, ok := lead.svc.Table().Lookup("default", "x")
			if !ok || l.Owner != lead.name {
				t.Fatalf("precondition not reached: x's lease %+v is not the leader's", l)
			}
			if later {
				lead.svc.tokens.Note("default", "x", l.Epoch+1)
			}
			release()
			waitFor(t, "the worker to finish with x", 5*time.Second, func() bool {
				return !lead.svc.adopting.unfinishedFor(key{"default", "x"})
			})
			st := lead.svc.LivenessStats()
			lead.svc.confirmOut.mu.Lock()
			queued := len(lead.svc.confirmOut.jobs)
			lead.svc.confirmOut.mu.Unlock()
			noted, _ := lead.svc.waitNotes.owner(key{"default", "x"}, lead.svc.steady(), lead.svc.o.SealTimeout)
			if !later {
				if !lead.sessions.has("default", "x") || st.AdoptedByWorker != 1 || st.AdoptImportRefused != 0 {
					t.Fatalf("no later grant: held %v, by worker %d, refused %d; want the import", lead.sessions.has("default", "x"), st.AdoptedByWorker, st.AdoptImportRefused)
				}
				return
			}
			if lead.sessions.has("default", "x") || st.AdoptedByWorker != 0 || st.AdoptImportRefused != 1 {
				t.Fatalf("later grant: held %v, by worker %d, refused %d; want nothing installed and one refusal", lead.sessions.has("default", "x"), st.AdoptedByWorker, st.AdoptImportRefused)
			}
			lead.sessions.mu.Lock()
			refused := append([]string(nil), lead.sessions.refused...)
			lead.sessions.mu.Unlock()
			if len(refused) != 1 || refused[0] != "default/x" {
				t.Fatalf("imports the check refused: %v; want x once", refused)
			}
			if queued != 0 {
				t.Fatalf("a refused import queued %d confirmations", queued)
			}
			if noted != owner.name {
				t.Fatalf("the pass would name %q; want the previous owner %s", noted, owner.name)
			}
		})
	}
}
